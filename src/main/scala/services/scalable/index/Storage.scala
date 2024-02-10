package services.scalable.index

import services.scalable.index.grpc.{IndexContext, TemporalContext}

import scala.concurrent.Future

trait Storage {

  def get(id: (String, String)): Future[Array[Byte]]

  def save(index: IndexContext, blocks: Map[(String, String), Array[Byte]]): Future[Boolean]
  def save(db: TemporalContext, blocks: Map[(String, String), Array[Byte]]): Future[Boolean]

  def save(blocks: Map[(String, String), Array[Byte]]): Future[Boolean]

  def save(ctx: IndexContext): Future[Boolean]

  def save(ctx: TemporalContext): Future[Boolean]

  def createTemporalIndex(tctx: TemporalContext): Future[Boolean]
  def createIndex(ictx: IndexContext): Future[Boolean]

  def loadTemporalIndex(id: String): Future[Option[TemporalContext]]

  def loadoOrCreateTemporalIndex(ctx: TemporalContext): Future[TemporalContext]

  def loadIndex(id: String): Future[Option[IndexContext]]

  def loadOrCreate(ctx: IndexContext): Future[IndexContext]

  def close(): Future[Unit]

}
