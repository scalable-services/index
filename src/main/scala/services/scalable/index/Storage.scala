package services.scalable.index

import services.scalable.index.grpc.{DBContext, IndexContext}
import scala.concurrent.Future

trait Storage {

  def get(id: (String, String)): Future[Array[Byte]]

  def save(index: IndexContext, blocks: Map[(String, String), Array[Byte]]): Future[Boolean]
  def save(db: DBContext, blocks: Map[(String, String), Array[Byte]]): Future[Boolean]

  def createDB(id: String): Future[DBContext]
  def createIndex(id: String, num_leaf_entries: Int, num_meta_entries: Int): Future[IndexContext]

  def loadOrCreateDB(id: String): Future[DBContext]
  def loadDB(id: String): Future[DBContext]

  def loadOrCreateIndex(id: String, num_leaf_entries: Int, num_meta_entries: Int): Future[IndexContext]
  def loadIndex(id: String): Future[IndexContext]

  def close(): Future[Unit]

}
