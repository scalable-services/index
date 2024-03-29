package services.scalable.index.impl

import org.slf4j.LoggerFactory
import services.scalable.index._
import services.scalable.index.grpc._

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class MemoryStorage()(implicit val ec: ExecutionContext) extends Storage {

  val logger = LoggerFactory.getLogger(this.getClass)

  val history = TrieMap.empty[String, TemporalContext]
  val indexes = TrieMap.empty[String, IndexContext]
  val blocks = TrieMap.empty[(String, String), Array[Byte]]

  override def get(id: (String, String)): Future[Array[Byte]] = {
    val buf = blocks(id)
    Future.successful(buf)
  }

  override def save(db: TemporalContext, blocks: Map[(String, String), Array[Byte]]): Future[Boolean] = {
    history.put(db.id, db)

    blocks.foreach { case (id, b) =>
      this.blocks.put(id, b)
    }

    Future.successful(true)
  }

  override def save(index: IndexContext, blocks: Map[(String, String), Array[Byte]]): Future[Boolean] = {
    indexes.put(index.id, index)

    blocks.foreach { case (id, b) =>
      this.blocks.put(id, b)
    }

    Future.successful(true)
  }

  override def loadTemporalIndex(id: String): Future[Option[TemporalContext]] = {
    Future.successful(history.get(id))
  }

  override def createIndex(ictx: IndexContext): Future[Boolean] = {
    if(indexes.isDefinedAt(ictx.id)){
      return Future.failed(Errors.INDEX_ALREADY_EXISTS(ictx.id))
    }

    indexes.put(ictx.id, ictx)
    Future.successful(true)
  }

  override def loadIndex(id: String): Future[Option[IndexContext]] = {
    Future.successful(indexes.get(id))
  }

  override def loadOrCreate(ctx: IndexContext): Future[IndexContext] = {
    loadIndex(ctx.id).flatMap {
      case None => createIndex(ctx).map(_ => ctx)
      case Some(c) => Future.successful(c)
    }
  }

  override def createTemporalIndex(tctx: TemporalContext): Future[Boolean] = {
    if (history.isDefinedAt(tctx.id)) {
      return Future.failed(Errors.TEMPORAL_INDEX_ALREADY_EXISTS(tctx.id))
    }

    history.put(tctx.id, tctx)
    Future.successful(true)
  }

  override def loadoOrCreateTemporalIndex(ctx: TemporalContext): Future[TemporalContext] = {
    loadTemporalIndex(ctx.id).flatMap {
      case None => createTemporalIndex(ctx).map(_ => ctx)
      case Some(c) => Future.successful(c)
    }
  }

  override def save(blocks: Map[(String, String), Array[Byte]]): Future[Boolean] = {
    blocks.foreach { case (id, b) =>
      this.blocks.put(id, b)
    }

    Future.successful(true)
  }

  override def close(): Future[Unit] = Future {
    history.clear()
    blocks.clear()
    indexes.clear()
  }

  override def save(ctx: IndexContext): Future[Boolean] = {
    indexes.put(ctx.id, ctx)
    Future.successful(true)
  }

  override def save(ctx: TemporalContext): Future[Boolean] = {
    history.put(ctx.id, ctx)
    Future.successful(true)
  }
}
