package services.scalable.index

import scala.concurrent.Future

trait Storage[K,V] {

  def get(id: String)(implicit ctx: Context[K,V]): Future[Block[K,V]]
  def save(ctx: Context[K, V]): Future[Boolean]

  def createIndex(indexId: String): Future[Context[K,V]]
  def loadOrCreate(indexId: String): Future[Context[K,V]]
  def load(indexId: String): Future[Context[K,V]]

  def close(): Future[Unit]

}
