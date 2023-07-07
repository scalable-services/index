package services.scalable.index

import org.slf4j.LoggerFactory
import services.scalable.index.grpc.{IndexContext, RootRef}
import services.scalable.index.impl.DefaultCache

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
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

  val newBlocksReferences = mutable.WeakHashMap.empty[(String, String), (String, String)]
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
  def get(id: (String, String)): Future[Block[K,V]] = cache.get[K, V](id) match {
    case None => storage.get(id).map { buf =>
      val block = serializer.deserialize(buf)
      cache.put(block)
      block
    }

    case Some(block) => Future.successful(block)
  }

  def put(block: Block[K, V]): Unit = {
    cache.put(block)
    newBlocksReferences.put(block.unique_id, block.unique_id)
  }

  def getLeaf(id: (String, String)): Future[Leaf[K,V]] = {
    get(id).map(_.asInstanceOf[Leaf[K,V]])
  }

  def getMeta(id: (String, String)): Future[Meta[K,V]] = {
    get(id).map(_.asInstanceOf[Meta[K,V]])
  }

  def createLeaf(): Leaf[K,V] = {
    val leaf = new Leaf[K,V](idGenerator.generateId(this), idGenerator.generatePartition(this), LEAF_MIN, LEAF_MAX)

    newBlocksReferences += leaf.unique_id -> leaf.unique_id
    cache.put(leaf)
    setParent(leaf.unique_id, 0, None)

    leaf
  }

  def createMeta(): Meta[K,V] = {
    val meta = new Meta[K,V](idGenerator.generateId(this), idGenerator.generatePartition(this), META_MIN, META_MAX)

    newBlocksReferences += meta.unique_id -> meta.unique_id
    cache.put(meta)
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
    newBlocksReferences.values.map{cache.get(_)}.foreach { opt =>
      if(opt.isDefined){
        val block = opt.get
        block.isNew = false
        block.root = root
      }
    }

    logger.debug(s"\nSAVING $indexId: ${root.map{r => RootRef(r._1, r._2)}}\n")

    IndexContext(indexId, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, root.map{r => RootRef(r._1, r._2)}, levels,
      num_elements, maxNItems)
  }

  def clear(): Unit = {
    newBlocksReferences.foreach { case (id, _) =>
      cache.invalidate(id)
    }
  }

  def save(): Future[IndexContext] = {
    val snap = snapshot()

    val blocks = newBlocksReferences.map { case (id, _) => id -> cache.get(id)}
      .filter(_._2.isDefined).map{case (id, opt) => id -> opt.get.asInstanceOf[Block[K, V]]}.toMap

    if(newBlocksReferences.size != blocks.size){
      return Future.failed(new RuntimeException("Some blocks were evicted from cache before saving to disk!"))
    }

    storage.save(snap, blocks.map{case (id, block) => id -> builder.serializer.serialize(block)}).map { r =>
      clear()
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
