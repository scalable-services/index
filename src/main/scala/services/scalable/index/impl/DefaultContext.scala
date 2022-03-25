package services.scalable.index.impl

import org.slf4j.LoggerFactory
import services.scalable.index._

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class DefaultContext[K, V](override val indexId: String,
                     override var root: Option[String],
                     override var num_elements: Long,
                     override var levels: Int,
                     override val NUM_LEAF_ENTRIES: Int,
                     override val NUM_META_ENTRIES: Int)
                    (implicit val ec: ExecutionContext,
                     val storage: Storage,
                     val serializer: Serializer[Block[K, V]],
                     val cache: Cache[K, V],
                     val ord: Ordering[K],
                     val idGenerator: IdGenerator = DefaultIdGenerators.idGenerator) extends Context[K,V] {

  val logger = LoggerFactory.getLogger(this.getClass)

  val LEAF_MAX = NUM_LEAF_ENTRIES
  val LEAF_MIN = LEAF_MAX/2

  val META_MAX = NUM_META_ENTRIES
  val META_MIN = META_MAX/2

  val blocks = TrieMap.empty[String, Block[K,V]]
  val parents = TrieMap.empty[String, (Option[String], Int)]

  if(root.isDefined) setParent(root.get, 0, None)

  root match {
    case None =>
    case Some(r) => parents += r -> (None, 0)
  }

  /**
   *
   * To work the blocks being manipulated must be in memory before saving...
   */
  override def get(unique_id: String): Future[Block[K,V]] = blocks.get(unique_id) match {
    case None => cache.get(unique_id) match {
      case None =>

        storage.get[K, V](unique_id).map { block =>
          cache.put(block)
          block
        }

      case Some(block) => Future.successful(block)
    }

    case Some(block) => Future.successful(block)
  }

  override def getLeaf(unique_id: String): Future[Leaf[K,V]] = {
    get(unique_id).map(_.asInstanceOf[Leaf[K,V]])
  }

  override def getMeta(unique_id: String): Future[Meta[K,V]] = {
    get(unique_id).map(_.asInstanceOf[Meta[K,V]])
  }

  override def isNew(unique_id: String): Boolean = {
    blocks.isDefinedAt(unique_id)
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

  override def setParent(unique_id: String, idx: Int, parent: Option[String]): Unit = {
    parents += unique_id -> (parent, idx)
    cache.put(unique_id, parent, idx)
  }

  override def getParent(unique_id: String): Option[(Option[String], Int)] = {
    if(parents.isDefinedAt(unique_id)) return parents.get(unique_id)

    cache.getParent(unique_id) match {
      case None => None
      case ctxOpt =>

        logger.debug(s"HIT THE CACHE!\n")
        //parents += unique_id -> ctxOpt.get

        val (parent, pos) = ctxOpt.get

        setParent(unique_id, pos, parent)

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
