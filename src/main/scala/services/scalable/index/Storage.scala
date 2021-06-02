package services.scalable.index

import scala.concurrent.Future

trait Storage {

  def get(id: String)(implicit ctx: Context): Future[Block]
  def save(ctx: Context): Future[Boolean]

  def createIndex(indexId: String): Future[Context]
  def loadOrCreate(indexId: String): Future[Context]
  def load(indexId: String): Future[Context]

}
