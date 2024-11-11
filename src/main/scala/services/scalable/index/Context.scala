package services.scalable.index

import org.slf4j.LoggerFactory
import services.scalable.index.grpc.{IndexContext, RootRef}

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

sealed class Context[K, V](val indexId: String,
                    var lastChangeVersion: String,
                    var root: Option[(String, String)],
                    var num_elements: Long,
                    var levels: Int)
                   (val builder: IndexBuilt[K, V]) {

  import builder._

  //val ctxCache = TrieMap.empty[(String, String), Block[K, V]]
  val id: String = UUID.randomUUID().toString

  val logger = LoggerFactory.getLogger(this.getClass)

  val LEAF_MAX = builder.MAX_LEAF_ITEMS
  val LEAF_MIN = LEAF_MAX/2

  val META_MAX = builder.MAX_META_ITEMS
  val META_MIN = META_MAX/2

  var newBlocksReferences = TrieMap.empty[(String, String), Block[K, V]]
  val parents = TrieMap.empty[(String, String), ParentInfo[K]]
  protected[index] val used = new AtomicBoolean(false)

  def getRoot(): Future[Option[Block[K, V]]] = {
    root match {
      case None => Future.successful(None)
      case Some(uid) => get(uid).map(Some(_))
    }
  }

  /**
   *
   * To work the blocks being manipulated must be in memory before saving...
   */
  def get(id: (String, String)): Future[Block[K,V]] = {
    val opt = newBlocksReferences.get(id)

    if(opt.isDefined) return Future.successful(opt.get)

    cache.get[K, V](id) match {
      case None => storage.get(id).map { buf =>
        val block = serializer.deserialize(buf)

        if(root.isDefined && root.get == block.unique_id){
          setParent(block.unique_id, block.lastOption, 0, None)
        }

        cache.put(block)

        //ctxCache.put(block.unique_id, block)

        block
      }

      case Some(block) => Future.successful(block)
    }
  }

  def put(block: Block[K, V]): Unit = {
    newBlocksReferences.put(block.unique_id, block)
  }

  def getLeaf(id: (String, String)): Future[Leaf[K,V]] = {
    get(id).map(_.asInstanceOf[Leaf[K,V]])
  }

  def getMeta(id: (String, String)): Future[Meta[K,V]] = {
    get(id).map(_.asInstanceOf[Meta[K,V]])
  }

  /*def getRootPos(id: (String, String))(positionsCache: TrieMap[(String, String), Int]): Int = {
    val cachedPos = positionsCache.get(id)

    if(cachedPos.isDefined) return cachedPos.get

    var parent = parents(id)

    if(parent._1.isEmpty) return 0

    var links = Seq(id)

    while(parent._1.isDefined){
      links = links :+ parent._1.get
      parent = parents(parent._1.get)
    }

    for(id <- links){
      positionsCache.put(id, parent._2)
    }

    parent._2
  }*/

  def createLeaf(sameId: Option[(String, String)] = None): Leaf[K,V] = {
    val leaf = new Leaf[K,V](if(sameId.isDefined) sameId.get._1 else
      idGenerator.generateBlockId(this), if(sameId.isDefined) sameId.get._2 else
      idGenerator.generateBlockPartition(this),
      LEAF_MIN, LEAF_MAX)

    logger.debug(s"Creating leaf ${leaf.unique_id}...")

    newBlocksReferences += leaf.unique_id -> leaf
    setParent(leaf.unique_id, None, 0, None)

    leaf
  }

  def createMeta(sameId: Option[(String, String)] = None): Meta[K,V] = {
    val meta = new Meta[K,V](if(sameId.isDefined) sameId.get._1 else
      idGenerator.generateBlockId(this), if(sameId.isDefined) sameId.get._2 else
      idGenerator.generateBlockPartition(this), META_MIN, META_MAX)

    logger.debug(s"Creating meta ${meta.unique_id}...")

    newBlocksReferences += meta.unique_id -> meta
    setParent(meta.unique_id, None, 0, None)

    meta
  }

  def setParent(unique_id: (String, String), lastKey: Option[K], idx: Int, parent: Option[(String, String)]): Unit = {
    logger.debug(s"Setting parent for ${unique_id} => ${(idx, parent)}...")
    //parents += unique_id -> ((parent, lastKey), idx)

    parents.put(unique_id, ParentInfo(parent, lastKey, idx))
  }

  def getParent(id: (String, String)): Option[ParentInfo[K]] = {
    parents.get(id)
  }

  def isFromCurrentContext(b: Block[K,V]): Boolean = {
    b.root.equals(root)
  }

  def currentSnapshot() = IndexContext(indexId, MAX_LEAF_ITEMS, MAX_META_ITEMS, root.map { r => RootRef(r._1, r._2) }, levels,
    num_elements, builder.MAX_N_ITEMS, lastChangeVersion)

  /**
   * When creating a snapshot, the new blocks are now immutable!
   * @return
   */
  def snapshot(): IndexContext = {

    // Turn new blocks immutable
    newBlocksReferences.values.foreach { block =>
      block.isNew = false
      block.root = root
    }

    logger.debug(s"\nSAVING $indexId: ${root.map{r => RootRef(r._1, r._2)}}\n")

    IndexContext(indexId, builder.MAX_LEAF_ITEMS, builder.MAX_META_ITEMS, root.map{r => RootRef(r._1, r._2)}, levels,
      num_elements, builder.MAX_N_ITEMS, lastChangeVersion)
  }

  def clear(): Unit = {
    logger.debug(s"Removing new block references ${newBlocksReferences.map(_._1)}...")

    /*newBlocksReferences.foreach { case (id, _) =>
      cache.invalidate(id)
    }*/

    newBlocksReferences.clear()
    parents.clear()
  }

  def save(): Future[IndexContext] = {
    val snap = snapshot()

    newBlocksReferences.values.filter(_.copiedFrom.isDefined).foreach { b =>
      val p = Await.result(get(b.copiedFrom.get), Duration.Inf)

      b match {
        case leaf: Leaf[K, V] =>
          val diff = leaf.tuples.diff(p.asInstanceOf[Leaf[K, V]].tuples)
          logger.info("leaf diff: {}, equals: {}, total: {}", diff.length,  leaf.tuples.length - diff.length, leaf.tuples.length)
        case meta: Meta[K, V] =>

          val diff = meta.pointers.diff(p.asInstanceOf[Meta[K, V]].pointers)
          logger.info("meta diff: {}, equals: {}, total: {}", diff.length,  meta.pointers.length - diff.length, meta.pointers.length)
      }
    }

    storage.save(snap, newBlocksReferences.map { case (id, block) => block.unique_id -> builder.serializer.serialize(block) }.toMap).map { r =>
      clear()
      snap
    }
  }
}

object Context {
  def fromIndexContext[K, V](ictx: IndexContext)(builder: IndexBuilt[K, V]): Context[K, V] = {
    new Context[K, V](ictx.id, ictx.lastChangeVersion, ictx.root.map{ r => (r.partition, r.id)}, ictx.numElements,
      ictx.levels)(builder)
  }
}
