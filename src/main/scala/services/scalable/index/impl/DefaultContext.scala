package services.scalable.index.impl

import org.slf4j.LoggerFactory
import services.scalable.index._

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class DefaultContext[K, V](override val indexId: String,
                     override var root: Option[(String, String)],
                     override var num_elements: Long,
                     override var levels: Int,
                     override val NUM_LEAF_ENTRIES: Int,
                     override val NUM_META_ENTRIES: Int)
                    (implicit val ec: ExecutionContext,
                     val storage: Storage,
                     val serializer: Serializer[Block[K, V]],
                     val cache: Cache,
                     val ord: Ordering[K],
                     val idGenerator: IdGenerator) extends Context[K,V] {

  val logger = LoggerFactory.getLogger(this.getClass)

  val LEAF_MAX = NUM_LEAF_ENTRIES
  val LEAF_MIN = LEAF_MAX/2

  val META_MAX = NUM_META_ENTRIES
  val META_MIN = META_MAX/2

  val blocks = TrieMap.empty[(String, String), Block[K,V]]
  val parents = TrieMap.empty[(String, String), (Option[(String, String)], Int)]

  if(root.isDefined) {
    setParent(root.get, 0, None)
  }

  root match {
    case None =>
    case Some(r) => parents += r -> (None, 0)
  }

  /**
   *
   * To work the blocks being manipulated must be in memory before saving...
   */
  override def get(id: (String, String)): Future[Block[K,V]] = blocks.get(id) match {
    case None => cache.get[K, V](id) match {
      case None =>

        storage.get(id).map { buf =>
          val block = serializer.deserialize(buf)
          cache.put(block)
          block
        }

      case Some(block) => Future.successful(block)
    }

    case Some(block) => Future.successful(block)
  }

  override def getLeaf(id: (String, String)): Future[Leaf[K,V]] = {
    get(id).map(_.asInstanceOf[Leaf[K,V]])
  }

  override def getMeta(id: (String, String)): Future[Meta[K,V]] = {
    get(id).map(_.asInstanceOf[Meta[K,V]])
  }

  override def isNew(id: (String, String)): Boolean = {
    blocks.isDefinedAt(id)
  }

  override def createLeaf(): Leaf[K,V] = {
    val leaf = new Leaf[K,V](idGenerator.generateId(this), idGenerator.generatePartition(this), LEAF_MIN, LEAF_MAX)

    blocks += leaf.unique_id -> leaf
    setParent(leaf.unique_id, 0, None)

    leaf
  }

  override def createMeta(): Meta[K,V] = {
    val meta = new Meta[K,V](idGenerator.generateId(this), idGenerator.generatePartition(this), META_MIN, META_MAX)

    blocks += meta.unique_id -> meta
    setParent(meta.unique_id, 0, None)

    meta
  }

  override def setParent(unique_id: (String, String), idx: Int, parent: Option[(String, String)]): Unit = {
    parents += unique_id -> (parent, idx)
    cache.put(unique_id, parent, idx)
  }

  override def getParent(id: (String, String)): Option[(Option[(String, String)], Int)] = {
    if(parents.isDefinedAt(id)) return parents.get(id)

    cache.getParent(id) match {
      case None => None
      case ctxOpt =>

        logger.debug(s"HIT THE CACHE!\n")
        //parents += unique_id -> ctxOpt.get

        val (parent, pos) = ctxOpt.get

        setParent(id, pos, parent)

        ctxOpt
    }
  }

  override def isFromCurrentContext(b: Block[K,V]): Boolean = {
    b.root.equals(root)
  }

  def save(): Boolean = {

    blocks.foreach { case (_, b) =>
      b.root = root
    }

    true
  }

  override def duplicate(): Context[K, V] = {
    new DefaultContext[K, V](indexId, root, num_elements, levels, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, storage,
      serializer, cache, ord, idGenerator)
  }
}
