package services.scalable.index

import org.slf4j.LoggerFactory
import services.scalable.index.grpc.{IndexContext, RootRef}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

sealed class Context[K, V](val indexId: String,
                    var root: Option[(String, String)],
                    var num_elements: Long,
                    var levels: Int,
                    val maxNItems: Int,
                    val NUM_LEAF_ENTRIES: Int,
                    val NUM_META_ENTRIES: Int)
                   (val builder: IndexBuilder[K, V]) {

  import builder._

  val id: String = UUID.randomUUID().toString

  val logger = LoggerFactory.getLogger(this.getClass)

  val LEAF_MAX = NUM_LEAF_ENTRIES
  val LEAF_MIN = LEAF_MAX/2

  val META_MAX = NUM_META_ENTRIES
  val META_MIN = META_MAX/2

  val newBlocks = TrieMap.empty[(String, String), Block[K, V]]
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
  def get(id: (String, String)): Future[Block[K,V]] = newBlocks.get(id) match {
    case None => cache.get[K, V](id) match {
      case None => newBlocks.get(id) match {
        case None => storage.get(id).map { buf =>
          val block = serializer.deserialize(buf)
          cache.put(block)
          block
        }
        case Some(block) => Future.successful(block)
      }

      case Some(block) => Future.successful(block)
    }

    case Some(block) => Future.successful(block)
  }

  def put(block: Block[K, V]): Unit = {
    newBlocks.put(block.unique_id, block)
  }

  def getLeaf(id: (String, String)): Future[Leaf[K,V]] = {
    get(id).map(_.asInstanceOf[Leaf[K,V]])
  }

  def getMeta(id: (String, String)): Future[Meta[K,V]] = {
    get(id).map(_.asInstanceOf[Meta[K,V]])
  }

  def createLeaf(): Leaf[K,V] = {
    val leaf = new Leaf[K,V](idGenerator.generateId(this), idGenerator.generatePartition(this), LEAF_MIN, LEAF_MAX)

    newBlocks += leaf.unique_id -> leaf
    setParent(leaf.unique_id, 0, None)

    leaf
  }

  def createMeta(): Meta[K,V] = {
    val meta = new Meta[K,V](idGenerator.generateId(this), idGenerator.generatePartition(this), META_MIN, META_MAX)

    newBlocks += meta.unique_id -> meta
    setParent(meta.unique_id, 0, None)

    meta
  }

  def setParent(unique_id: (String, String), idx: Int, parent: Option[(String, String)]): Unit = {
    parents += unique_id -> (parent, idx)
  }

  def getParent(id: (String, String)): Option[(Option[(String, String)], Int)] = {
    parents.get(id)
  }

  def isFromCurrentContext(b: Block[K,V]): Boolean = {
    b.root.equals(root)
  }

  def currentSnapshot() = IndexContext(indexId, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, root.map { r => RootRef(r._1, r._2) }, levels,
    num_elements, maxNItems)

  def snapshot(): IndexContext = {

    // Freeze the blocks...
    newBlocks.values.foreach { b =>
      b.isNew = false
      b.root = root
    }

    logger.debug(s"\nSAVING $indexId: ${root.map{r => RootRef(r._1, r._2)}}\n")

    IndexContext(indexId, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, root.map{r => RootRef(r._1, r._2)}, levels,
      num_elements, maxNItems)
  }

  def save(): Future[IndexContext] = {
    val snap = snapshot()

    storage.save(snap, newBlocks.map { case (id, block) => id -> serializer.serialize(block) }.toMap).map { r =>
      snap
    }
  }
}

object Context {
  def fromIndexContext[K, V](ictx: IndexContext)(builder: IndexBuilder[K, V]): Context[K, V] = {
    new Context[K, V](ictx.id, ictx.root.map{ r => (r.partition, r.id)}, ictx.numElements,
      ictx.levels, ictx.maxNItems, ictx.numLeafItems, ictx.numMetaItems)(builder)
  }
}
