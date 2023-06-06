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

  object TxState {
    val PENDING = "PENDING"
    val COMPLETED = "COMPLETED"
    val ABORTED = "ABORTED"
  }

  import builder._

  // Context id (for global manipulation of new blocks)
  val id: String = UUID.randomUUID().toString

  val logger = LoggerFactory.getLogger(this.getClass)

  val LEAF_MAX = NUM_LEAF_ENTRIES
  val LEAF_MIN = LEAF_MAX/2

  val META_MAX = NUM_META_ENTRIES
  val META_MIN = META_MAX/2

  val blockReferences = TrieMap.empty[(String, String), (String, String)]
  val parents = TrieMap.empty[(String, String), (Option[(String, String)], Int)]

  protected[index] var txRoot: Option[(String, String)] = None
  protected[index] val txBlockReferences = TrieMap.empty[(String, String), (String, String)]
  protected[index] var txId: Option[String] = None
  protected[index] var txState = TxState.PENDING

  if(root.isDefined) {
    setParent(root.get, 0, None)
  }

  root match {
    case None =>
    case Some(r) => parents += r -> (None, 0)
  }

  def getCurrentTxId(): Option[String] = txId

  def beginTx(): Unit = synchronized {
    assert(txId.isEmpty, s"Previous transaction ${txId.get} not completed!")

    txId = Some(UUID.randomUUID.toString)
    txRoot = root
    txState = TxState.PENDING
  }

  def commitTx(): Unit = synchronized {
    assert(txId.isDefined, "You must start a transaction first calling beginTx()!")
    assert(txState == TxState.PENDING, "Are you trying to commit an aborted tx?")

    txBlockReferences.foreach { t =>
      blockReferences += t
    }

    txBlockReferences.clear()
    txState = TxState.COMPLETED
    txId = None
  }

  def rollbackTx(): Unit = synchronized {
    assert(txId.isDefined, "You must start a transaction first calling beginTx()!")
    assert(txState == TxState.PENDING, "Are you trying to abort an already committed tx?")

    // Removed new blocks created during the failed tx...
    txBlockReferences.foreach { t =>
      cache.newBlocks.remove(t._1)
      parents.remove(t._1)
    }

    txBlockReferences.clear()
    txState = TxState.ABORTED
    txId = None
    root = txRoot
  }

  /**
   *
   * To work the blocks being manipulated must be in memory before saving...
   */
  def get(id: (String, String)): Future[Block[K,V]] = cache.newBlocks.get(id) match {
    case None => cache.get[K, V](id) match {
      case None =>

        storage.get(id).map { buf =>
          val block = serializer.deserialize(buf)
          cache.put(block)
          block
        }

      case Some(block) => Future.successful(block)
    }

    case Some(block) => Future.successful(block.asInstanceOf[Block[K, V]])
  }

  def put(block: Block[K, V]): Unit = {
    //cache.newBlocks.put(block.unique_id, block)
    txBlockReferences += block.unique_id -> block.unique_id
  }

  def getBlocks(): Map[(String, String), Block[K, V]] = {
    blockReferences.map { case (id, _) =>
      id -> cache.newBlocks.get(id).get.asInstanceOf[Block[K, V]]
    }.toMap
  }

  def getLeaf(id: (String, String)): Future[Leaf[K,V]] = {
    get(id).map(_.asInstanceOf[Leaf[K,V]])
  }

  def getMeta(id: (String, String)): Future[Meta[K,V]] = {
    get(id).map(_.asInstanceOf[Meta[K,V]])
  }

  def createLeaf(): Leaf[K,V] = {
    val leaf = new Leaf[K,V](idGenerator.generateId(this), idGenerator.generatePartition(this), LEAF_MIN, LEAF_MAX)

    cache.newBlocks += leaf.unique_id -> leaf

    //blockReferences += leaf.unique_id -> leaf.unique_id
    txBlockReferences += leaf.unique_id -> leaf.unique_id
    setParent(leaf.unique_id, 0, None)

    leaf
  }

  def createMeta(): Meta[K,V] = {
    val meta = new Meta[K,V](idGenerator.generateId(this), idGenerator.generatePartition(this), META_MIN, META_MAX)

    cache.newBlocks += meta.unique_id -> meta

    //blockReferences += meta.unique_id -> meta.unique_id
    txBlockReferences += meta.unique_id -> meta.unique_id
    setParent(meta.unique_id, 0, None)

    meta
  }

  def setParent(unique_id: (String, String), idx: Int, parent: Option[(String, String)]): Unit = {
    parents += unique_id -> (parent, idx)
    cache.put(unique_id, parent, idx)
  }

  def getParent(id: (String, String)): Option[(Option[(String, String)], Int)] = {
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

  def isFromCurrentContext(b: Block[K,V]): Boolean = {
    b.root.equals(root)
  }

  def snapshot(): IndexContext = {
    cache.newBlocks.filter(_._2.isNew).foreach { case (_, b) =>
      b.root = root
      b.isNew = false
    }

    logger.debug(s"\nSAVING $indexId: ${root.map{r => RootRef(r._1, r._2)}}\n")

    IndexContext(indexId, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, root.map{r => RootRef(r._1, r._2)}, levels,
      num_elements, maxNItems)
  }

  def snapshot(clearNewBlocks: Boolean): IndexContext = {

    if(clearNewBlocks){
      cache.newBlocks.filter(_._2.isNew).foreach { case (_, b) =>
        b.root = root
        b.isNew = false
      }
    }

    logger.debug(s"\nSAVING $indexId: ${root.map { r => RootRef(r._1, r._2) }}\n")

    IndexContext(indexId, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, root.map { r => RootRef(r._1, r._2) }, levels,
      num_elements, maxNItems)
  }

  def copy(): Context[K, V] = {
    val ctx = new Context[K, V](indexId, root, num_elements, levels, maxNItems, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(builder)

    blockReferences.foreach { t =>
      ctx.blockReferences += t
    }

    parents.foreach { t =>
      ctx.parents += t
    }

    ctx
  }

  private def clear(): Unit = {
    blockReferences.foreach { case (id, _) =>
      cache.newBlocks.remove(id)
    }
  }

  def save(): Future[IndexContext] = {
    val snap = snapshot()

    storage.save(snap, getBlocks().map { case (id, block) => id -> serializer.serialize(block) }).map { r =>
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
