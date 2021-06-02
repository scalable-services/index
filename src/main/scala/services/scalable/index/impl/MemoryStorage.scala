package services.scalable.index.impl

import org.slf4j.LoggerFactory
import services.scalable.index.{Context, _}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class MemoryStorage(val NUM_LEAF_ENTRIES: Int, val NUM_META_ENTRIES: Int)(implicit val ec: ExecutionContext,
                                                                                val ord: Ordering[Bytes],
                                                                                val cache: Cache) extends Storage {

  val logger = LoggerFactory.getLogger(this.getClass)

  val roots = TrieMap.empty[String, (Option[String], Int, Int)]
  val blocks = TrieMap.empty[String, Block]

  override def get(unique_id: String)(implicit ctx: Context): Future[Block] = {
    Future.successful(blocks(unique_id))
  }

  override def save(ctx: Context): Future[Boolean] = {
    val c = ctx.asInstanceOf[DefaultContext]

    roots.put(ctx.indexId, Tuple3(ctx.root, ctx.NUM_LEAF_ENTRIES, ctx.NUM_META_ENTRIES))

    c.blocks.foreach { case (_, b) =>
      blocks.put(b.unique_id, b)
    }

    Future.successful(true)
  }

  override def load(indexId: String): Future[Context] = {
    roots.get(indexId) match {
      case None => Future.failed(Errors.INDEX_NOT_FOUND(indexId))
      case Some((root, nle, nme)) =>
        val ctx = new DefaultContext(indexId, root, nle, nme)(ec, this, cache, ord)
        Future.successful(ctx)
    }
  }

  override def createIndex(indexId: String): Future[Context] = {
    val ctx = new DefaultContext(indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, this, cache, ord)

    if(roots.isDefinedAt(indexId)){
      return Future.failed(Errors.INDEX_ALREADY_EXISTS(indexId))
    }

    roots.put(indexId, (None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES))
    Future.successful(ctx)
  }

  override def loadOrCreate(indexId: String): Future[Context] = {
    load(indexId).recoverWith {
      case e: Errors.INDEX_NOT_FOUND => createIndex(indexId)
      case e => Future.failed(e)
    }
  }
}
