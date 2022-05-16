package services.scalable.index.impl

import org.slf4j.LoggerFactory
import services.scalable.index._
import services.scalable.index.grpc._

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class MemoryStorage(val NUM_LEAF_ENTRIES: Int, val NUM_META_ENTRIES: Int)(implicit val ec: ExecutionContext) extends Storage {

  val logger = LoggerFactory.getLogger(this.getClass)

  val databases = TrieMap.empty[String, DBContext]
  val blocks = TrieMap.empty[(String, String), Array[Byte]]

  override def get(id: (String, String)): Future[Array[Byte]] = {
    val buf = blocks(id)
    Future.successful(buf)
  }

  override def save(db: DBContext, blocks: Map[(String, String), Array[Byte]]): Future[Boolean] = {
    databases.put(db.id, db)

    blocks.foreach { case (id, b) =>
      this.blocks.put(id, b)
    }

    Future.successful(true)
  }

  override def load(id: String): Future[DBContext] = {
    databases.get(id) match {
      case None => Future.failed(Errors.INDEX_NOT_FOUND(id))
      case Some(db) => Future.successful(db)
    }
  }

  override def createIndex(id: String): Future[DBContext] = {
    val db = DBContext(id)

    if(databases.isDefinedAt(id)){
      return Future.failed(Errors.INDEX_ALREADY_EXISTS(id))
    }

    databases.put(id, db)
    Future.successful(db)
  }

  override def loadOrCreate(name: String): Future[DBContext] = {
    load(name).recoverWith {
      case e: Errors.INDEX_NOT_FOUND => createIndex(name)
      case e => Future.failed(e)
    }
  }

  override def close(): Future[Unit] = Future.successful()
}
