package services.scalable.index.impl

import org.slf4j.LoggerFactory
import services.scalable.index._
import services.scalable.index.grpc.DatabaseContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class MemoryStorage[K, V](val NUM_LEAF_ENTRIES: Int, val NUM_META_ENTRIES: Int)(implicit val ec: ExecutionContext,
                                                                                val ord: Ordering[K],
                                                                                val cache: Cache[K,V]) extends Storage {
  val logger = LoggerFactory.getLogger(this.getClass)

  val databases = TrieMap.empty[String, DatabaseContext]
  val blocks = TrieMap.empty[String, Array[Byte]]

  override def get[K, V](unique_id: String)(implicit serializer: Serializer[Block[K, V]]): Future[Block[K,V]] = {
    val buf = blocks(unique_id)
    Future.successful(serializer.deserialize(buf))
  }

  override def save(db: DatabaseContext, blocks: Map[String, Array[Byte]]): Future[Boolean] = {
    databases.put(db.name, db)

    blocks.foreach { case (id, b) =>
      this.blocks.put(id, b)
    }

    Future.successful(true)
  }

  override def load(name: String): Future[DatabaseContext] = {
    databases.get(name) match {
      case None => Future.failed(Errors.INDEX_NOT_FOUND(name))
      case Some(db) => Future.successful(db)
    }
  }

  override def createIndex(name: String): Future[DatabaseContext] = {
    val db = DatabaseContext(name)

    if(databases.isDefinedAt(name)){
      return Future.failed(Errors.INDEX_ALREADY_EXISTS(name))
    }

    databases.put(name, db)
    Future.successful(db)
  }

  override def loadOrCreate(name: String): Future[DatabaseContext] = {
    load(name).recoverWith {
      case e: Errors.INDEX_NOT_FOUND => createIndex(name)
      case e => Future.failed(e)
    }
  }

  override def close(): Future[Unit] = Future.successful()
}
