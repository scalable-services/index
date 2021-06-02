package services.scalable.index.impl

import org.slf4j.LoggerFactory
import services.scalable.index.{Block, Context, Leaf, Meta, _}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class DefaultContext(override val indexId: String,
                     override var root: Option[String],
                     override val NUM_LEAF_ENTRIES: Int,
                     override val NUM_META_ENTRIES: Int)
                    (implicit val ec: ExecutionContext,
                     val storage: Storage,
                     val cache: Cache,
                     val ord: Ordering[Bytes],
                     val idGenerator: IdGenerator = DefaultIdGenerators.idGenerator) extends Context {

  val logger = LoggerFactory.getLogger(this.getClass)

  assert(NUM_LEAF_ENTRIES >= 4 && NUM_META_ENTRIES >= 4, "The number of elements must be equal or greater than 4!")

  val LEAF_MAX = NUM_LEAF_ENTRIES
  val LEAF_MIN = LEAF_MAX/2

  val META_MAX = NUM_META_ENTRIES
  val META_MIN = META_MAX/2

  val blocks = TrieMap.empty[String, Block]
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
  override def get(unique_id: String): Future[Block] = blocks.get(unique_id) match {
    case None => cache.get(unique_id) match {
      case None =>

        storage.get(unique_id)(this).map { block =>
          cache.put(block)
          block
        }

      case Some(block) => Future.successful(block)
    }

    case Some(block) => Future.successful(block)
  }

  override def getLeaf(unique_id: String): Future[Leaf] = {
    get(unique_id).map(_.asInstanceOf[Leaf])
  }

  override def getMeta(unique_id: String): Future[Meta] = {
    get(unique_id).map(_.asInstanceOf[Meta])
  }

  override def isNew(unique_id: String): Boolean = {
    blocks.isDefinedAt(unique_id)
  }

  override def createLeaf(): Leaf = {
    val leaf = new Leaf(idGenerator.generateId(this), idGenerator.generatePartition(this), LEAF_MIN, LEAF_MAX)

    blocks += leaf.unique_id -> leaf
    setParent(leaf.unique_id, 0, None)

    leaf
  }

  override def createMeta(): Meta = {
    val meta = new Meta(idGenerator.generateId(this), idGenerator.generatePartition(this), META_MIN, META_MAX)

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

  override def isFromCurrentContext(b: Block): Boolean = {
    b.root.equals(root)
  }

  override def save(): Future[Boolean] = {

    blocks.foreach { case (_, b) =>
      b.root = root
    }

    storage.save(this).map { r =>
      blocks.clear()
      parents.clear()
      r
    }
  }
}
