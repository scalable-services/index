package services.scalable.index.impl

import org.slf4j.LoggerFactory
import services.scalable.index._
import services.scalable.index.grpc._

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class MemoryStorage(val NUM_LEAF_ENTRIES: Int, val NUM_META_ENTRIES: Int)(implicit val ec: ExecutionContext) extends Storage {

  val logger = LoggerFactory.getLogger(this.getClass)

  val databases = TrieMap.empty[String, DBContext]
  val indexes = TrieMap.empty[String, IndexContext]
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

  override def save(index: IndexContext, blocks: Map[(String, String), Array[Byte]]): Future[Boolean] = {
    indexes.put(index.id, index)

    blocks.foreach { case (id, b) =>
      this.blocks.put(id, b)
    }

    Future.successful(true)
  }

  override def loadDB(id: String): Future[Option[DBContext]] = {
    databases.get(id) match {
      case None => Future.successful(None)
      case Some(db) => Future.successful(Some(db))
    }
  }

  override def loadIndex(id: String): Future[Option[IndexContext]] = {
    indexes.get(id) match {
      case None => Future.failed(Errors.INDEX_NOT_FOUND(id))
      case Some(index) => Future.successful(Some(index))
    }
  }

  override def createDB(id: String): Future[DBContext] = {
    val db = DBContext(id)

    if(databases.isDefinedAt(id)){
      return Future.failed(Errors.DB_ALREADY_EXISTS(id))
    }

    databases.put(id, db)
    Future.successful(db)
  }

  override def createIndex(id: String, num_leaf_entries: Int, num_meta_entries: Int): Future[IndexContext] = {
    val index = IndexContext(id, num_leaf_entries, num_meta_entries)

    if(indexes.isDefinedAt(id)){
      return Future.failed(Errors.INDEX_ALREADY_EXISTS(id))
    }

    indexes.put(id, index)
    Future.successful(index)
  }

  override def loadOrCreateDB(name: String): Future[DBContext] = {
    loadDB(name).flatMap {
      case None => Future.failed(Errors.INDEX_NOT_FOUND(name))
      case Some(index) => Future.successful(index)
    }
  }

  override def loadOrCreateIndex(name: String, num_leaf_entries: Int, num_meta_entries: Int): Future[IndexContext] = {
    loadIndex(name).flatMap {
      case None => createIndex(name, num_leaf_entries, num_meta_entries)
      case Some(index) => Future.successful(index)
    }
  }

  override def close(): Future[Unit] = Future.successful()
}
