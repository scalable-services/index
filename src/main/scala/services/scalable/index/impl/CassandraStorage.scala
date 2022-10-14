package services.scalable.index.impl

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, DefaultBatchType}
import com.google.protobuf.any.Any
import org.slf4j.LoggerFactory
import services.scalable.index._
import services.scalable.index.grpc.{DBContext, IndexContext}

import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}

class CassandraStorage(val KEYSPACE: String,
                       val NUM_LEAF_ENTRIES: Int,
                       val NUM_META_ENTRIES: Int,
                       val truncate: Boolean = true)(implicit val ec: ExecutionContext) extends Storage {

  val logger = LoggerFactory.getLogger(this.getClass)

  val session = CqlSession
    .builder()
    .withConfigLoader(loader)
    .withKeyspace(KEYSPACE)
    .build()

  val INSERT = session.prepare("insert into blocks(partition, id, bin, size) values (?, ?, ?, ?);")
  val SELECT_DB = session.prepare("select buf from databases where id=?;")
  val SELECT_INDEX = session.prepare("select buf from indexes where id=?;")
  val INSERT_DB = session.prepare("insert into databases(id, buf) VALUES(?,?) IF NOT EXISTS;")
  val INSERT_INDEX = session.prepare("insert into indexes(id, buf) VALUES(?,?) IF NOT EXISTS;")
  val SELECT = session.prepare("select * from blocks where partition=? and id=?;")
  val UPDATE_DB = session.prepare("update databases set buf=? where id = ? IF EXISTS;")
  val UPDATE_INDEX = session.prepare("update indexes set buf=? where id = ? IF EXISTS;")

  if(truncate){
    logger.debug(s"TRUNCATED BLOCKS: ${session.execute("TRUNCATE table databases;").wasApplied()}\n")
    logger.debug(s"TRUNCATED META: ${session.execute("TRUNCATE table blocks;").wasApplied()}\n")
  }

  override def createDB(id: String): Future[DBContext] = {
    val db = DBContext(id)
    val buf = ByteBuffer.wrap(Any.pack(db).toByteArray)

    session.executeAsync(INSERT_DB.bind()
      .setString(0, db.id)
      .setByteBuffer(1, buf)).flatMap {
      case r if r.wasApplied() => Future.successful(db)
      case _ => Future.failed(Errors.DB_ALREADY_EXISTS(db.id))
    }
  }

  override def createIndex(id: String, num_leaf_entries: Int, num_meta_entries: Int): Future[IndexContext] = {
    val index = IndexContext(id, num_leaf_entries, num_meta_entries)
    val buf = ByteBuffer.wrap(Any.pack(index).toByteArray)

    session.executeAsync(INSERT_INDEX.bind()
      .setString(0, index.id)
      .setByteBuffer(1, buf)).flatMap {
      case r if r.wasApplied() => Future.successful(index)
      case _ => Future.failed(Errors.INDEX_ALREADY_EXISTS(index.id))
    }
  }

  override def loadOrCreateDB(id: String): Future[DBContext] = {
    loadDB(id).flatMap {
      case None => createDB(id)
      case Some(db) => Future.successful(db)
    }
  }

  override def loadOrCreateIndex(id: String, num_leaf_entries: Int, num_meta_entries: Int): Future[IndexContext] = {
    loadIndex(id).flatMap {
      case None => createIndex(id, num_leaf_entries, num_meta_entries)
      case Some(index) => Future.successful(index)
    }
  }

  override def loadDB(id: String): Future[Option[DBContext]] = {
    session.executeAsync(SELECT_DB.bind().setString(0, id)).flatMap { rs =>
      val one = rs.one()

      if(one == null){
        Future.successful(None)
      } else {
        val r = one.getByteBuffer("buf")
        val db = Any.parseFrom(r.array()).unpack(DBContext)

        Future.successful(Some(db))
      }
    }
  }

  override def loadIndex(id: String): Future[Option[IndexContext]] = {
    session.executeAsync(SELECT_INDEX.bind().setString(0, id)).flatMap { rs =>
      val one = rs.one()

      if(one == null){
        Future.successful(None)
      } else {
        val r = one.getByteBuffer("buf")
        val index = Any.parseFrom(r.array()).unpack(IndexContext)

        Future.successful(Some(index))
      }
    }
  }

  override def get(id: (String, String)): Future[Array[Byte]] = {
    session.executeAsync(SELECT.bind().setString(0, id._1).setString(1, id._2)).map { rs =>
      val one = rs.one()

      if(one == null){
        println(id)
      }

      val buf = one.getByteBuffer("bin")
      buf.array()
    }
  }

  protected def updateDB(db: DBContext): Future[Boolean] = {
    //val buf = ByteBuffer.wrap(DBContext.toByteArray(db))
    val buf = ByteBuffer.wrap(Any.pack(db).toByteArray)

    session.executeAsync(UPDATE_DB.bind().setByteBuffer(0, buf)
      .setString(1, db.id)).map(_.wasApplied())
  }

  protected def updateIndex(index: IndexContext): Future[Boolean] = {
    //val buf = ByteBuffer.wrap(IndexContext.toByteArray(index))
    val buf = ByteBuffer.wrap(Any.pack(index).toByteArray)

    session.executeAsync(UPDATE_INDEX.bind().setByteBuffer(0, buf)
      .setString(1, index.id)).map(_.wasApplied())
  }

  override def save(db: DBContext, blocks: Map[(String, String), Array[Byte]]): Future[Boolean] = {
    save(blocks).flatMap { ok =>
      if (ok) updateDB(db) else Future.successful(false)
    }
  }

  override def save(db: DBContext): Future[Boolean] = {
    updateDB(db)
  }

  override def save(idx: IndexContext): Future[Boolean] = {
    updateIndex(idx)
  }

  override def save(index: IndexContext, blocks: Map[(String, String), Array[Byte]]): Future[Boolean] = {
    save(blocks).flatMap { ok =>
      if(ok) updateIndex(index) else Future.successful(false)
    }
  }

  override def save(blocks: Map[(String, String), Array[Byte]]): Future[Boolean] = {
    /*val stm = BatchStatement.builder(DefaultBatchType.LOGGED)

    blocks.map { case ((partition, id), bin) =>
      stm.addStatement(INSERT
        .bind()
        .setString(0, partition)
        .setString(1, id)
        .setByteBuffer(2, ByteBuffer.wrap(bin))
        .setInt(3, bin.length)
      )
    }

    session.executeAsync(stm.build()).map(_.wasApplied())*/

    Future.sequence(blocks.map { case ((partition, id), bin) =>
      session.executeAsync(INSERT
        .bind()
        .setString(0, partition)
        .setString(1, id)
        .setByteBuffer(2, ByteBuffer.wrap(bin))
        .setInt(3, bin.length)).map(res => (partition, id) -> res.wasApplied())
    }).map{ r =>
      println(s"block ids not saved: ${r}")
      r
    }.map(r => !r.exists(_._2 == false))
  }

  override def close(): Future[Unit] = {
    session.closeAsync().map{_ => {}}
  }
}
