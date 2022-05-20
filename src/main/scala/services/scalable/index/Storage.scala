package services.scalable.index

import services.scalable.index.grpc.DBContext
import scala.concurrent.Future

trait Storage {

  def get(id: (String, String)): Future[Array[Byte]]

  def save(db: DBContext, blocks: Map[(String, String), Array[Byte]]): Future[Boolean]

  def createIndex(id: String): Future[DBContext]
  def loadOrCreate(id: String): Future[DBContext]
  def load(id: String): Future[DBContext]

  def close(): Future[Unit]

}
