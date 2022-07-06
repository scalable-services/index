package services.scalable.index

import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class Context[K, V](val NUM_LEAF_ENTRIES: Int,
                    val NUM_META_ENTRIES: Int)
                   (implicit val ec: ExecutionContext,
                     val storage: Storage,
                     val serializer: Serializer[Block[K, V]],
                     val cache: Cache,
                     val ord: Ordering[K],
                     val idGenerator: IdGenerator) {

  val logger = LoggerFactory.getLogger(this.getClass)

  val LEAF_MAX = NUM_LEAF_ENTRIES
  val LEAF_MIN = LEAF_MAX/2

  val META_MAX = NUM_META_ENTRIES
  val META_MIN = META_MAX/2

  val blocks = TrieMap.empty[String, TrieMap[(String, String), Block[K, V]]]
  val parents = TrieMap.empty[String, TrieMap[(String, String), (Option[(String, String)], Int)]]

  def createCtxSpace(ctxId: String): Unit = {
    blocks.put(ctxId, TrieMap.empty[(String, String), Block[K, V]])
    parents.put(ctxId, TrieMap.empty[(String, String), (Option[(String, String)], Int)])
  }

  def put(ctxId: String, block: Block[K, V]): Unit = {
    blocks(ctxId).put(block.unique_id, block)
  }

  /**
   *
   * To work the blocks being manipulated must be in memory before saving...
   */
  def get(ctxId: String, id: (String, String)): Future[Block[K,V]] = blocks(ctxId).get(id) match {
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

  def getLeaf(ctxId: String, id: (String, String)): Future[Leaf[K,V]] = {
    get(ctxId, id).map(_.asInstanceOf[Leaf[K,V]])
  }

  def getMeta(ctxId: String, id: (String, String)): Future[Meta[K,V]] = {
    get(ctxId, id).map(_.asInstanceOf[Meta[K,V]])
  }

  def createLeaf(ctxId: String): Leaf[K,V] = {
    val leaf = new Leaf[K,V](idGenerator.generateId(this), idGenerator.generatePartition(this), LEAF_MIN, LEAF_MAX)

    val bs = blocks(ctxId)

    bs.put(leaf.unique_id, leaf)
    setParent(ctxId, leaf.unique_id, 0, None)

    leaf
  }

  def createMeta(ctxId: String): Meta[K,V] = {
    val meta = new Meta[K,V](idGenerator.generateId(this), idGenerator.generatePartition(this), META_MIN, META_MAX)

    val bs = blocks(ctxId)

    bs.put(meta.unique_id, meta)
    setParent(ctxId, meta.unique_id, 0, None)

    meta
  }

  def setParent(ctxId: String, unique_id: (String, String), idx: Int, parent: Option[(String, String)]): Unit = {
    val ps = parents(ctxId)
    ps.put(unique_id, (parent, idx))

    cache.put(unique_id, parent, idx)
  }

  def getParent(ctxId: String, id: (String, String)): Option[(Option[(String, String)], Int)] = {
    val ps = parents(ctxId)

    if(ps.isDefinedAt(id)) return ps.get(id)

    cache.getParent(id) match {
      case None => None
      case ctxOpt =>

        logger.debug(s"HIT THE CACHE!\n")
        //parents += unique_id -> ctxOpt.get

        val (parent, pos) = ctxOpt.get
        setParent(ctxId, id, pos, parent)

        ctxOpt
    }
  }

  /*def isFromCurrentContext(b: Block[K,V]): Boolean = {
    b.root.equals(root)
  }*/

  def getBlocks(ctxId: String): TrieMap[(String, String), Block[K, V]] = {
    blocks(ctxId)
  }

  def removeCtx(ctxId: String): Unit = {
    blocks.remove(ctxId).map(_.clear())
    parents.remove(ctxId).map(_.clear())
  }

  def clear(ctxId: String): Unit = {
    blocks(ctxId).clear()
    parents(ctxId).clear()
  }
}

object Context {
  /*def fromIndexContext[K, V](ictx: IndexContext)(implicit ec: ExecutionContext,
                                                 storage: Storage,
                                                 serializer: Serializer[Block[K, V]],
                                                 cache: Cache,
                                                 ord: Ordering[K],
                                                 idGenerator: IdGenerator): Context[K, V] = {
    new Context[K, V](ictx.id, ictx.root.map{ r => (r.partition, r.id)}, ictx.numElements,
      ictx.levels, ictx.numLeafItems, ictx.numMetaItems)
  }*/
}
