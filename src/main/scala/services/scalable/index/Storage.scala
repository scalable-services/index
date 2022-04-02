package services.scalable.index

import services.scalable.index.grpc.{DatabaseContext, IndexContext}
import scala.concurrent.Future

trait Storage {

  def get(id: String): Future[Array[Byte]]
  def save(contexts: DatabaseContext, blocks: Map[String, Array[Byte]]): Future[Boolean]

  def createIndex(name: String): Future[DatabaseContext]
  def loadOrCreate(name: String): Future[DatabaseContext]
  def load(name: String): Future[DatabaseContext]

  def close(): Future[Unit]

}
