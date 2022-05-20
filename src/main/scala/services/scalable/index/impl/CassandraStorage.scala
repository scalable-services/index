package services.scalable.index.impl

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, DefaultBatchType}
import com.google.protobuf.any.Any
import org.slf4j.LoggerFactory
import services.scalable.index._
import services.scalable.index.grpc.DBContext

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
  val SELECT_ROOT = session.prepare("select buf from databases where id=?;")
  val INSERT_DB = session.prepare("insert into databases(id, buf) VALUES(?,?) IF NOT EXISTS;")
  val SELECT = session.prepare("select * from blocks where partition=? and id=?;")
  val UPDATE_DB = session.prepare("update databases set buf=? where id = ? IF EXISTS;")

  if(truncate){
    logger.debug(s"TRUNCATED BLOCKS: ${session.execute("TRUNCATE table databases;").wasApplied()}\n")
    logger.debug(s"TRUNCATED META: ${session.execute("TRUNCATE table blocks;").wasApplied()}\n")
  }

  override def createIndex(id: String): Future[DBContext] = {
    val db = DBContext(id)
    val buf = ByteBuffer.wrap(Any.pack(db).toByteArray)

    session.executeAsync(INSERT_DB.bind()
      .setString(0, db.id)
      .setByteBuffer(1, buf)).flatMap {
      case r if r.wasApplied() => Future.successful(db)
      case _ => Future.failed(Errors.INDEX_ALREADY_EXISTS(db.id))
    }
  }

  override def loadOrCreate(id: String): Future[DBContext] = {
    load(id).recoverWith {
      case e: Errors.INDEX_NOT_FOUND => createIndex(id)
      case e => Future.failed(e)
    }
  }

  override def load(id: String): Future[DBContext] = {
    session.executeAsync(SELECT_ROOT.bind().setString(0, id)).flatMap { rs =>
      val one = rs.one()

      if(one == null){
        Future.failed(Errors.INDEX_NOT_FOUND(id))
      } else {
        val r = one.getByteBuffer("buf")
        val db = Any.parseFrom(r.array()).unpack(DBContext)

        Future.successful(db)
      }
    }
  }

  override def get(id: (String, String)): Future[Array[Byte]] = {
    session.executeAsync(SELECT.bind().setString(0, id._1).setString(1, id._2)).map { rs =>
      val one = rs.one()
      val buf = one.getByteBuffer("bin")
      buf.array()
    }
  }

  def updateDB(db: DBContext): Future[Boolean] = {
    val buf = ByteBuffer.wrap(Any.pack(db).toByteArray)

    session.executeAsync(UPDATE_DB.bind().setByteBuffer(0, buf)
      .setString(1, db.id)).map(_.wasApplied())
  }

  override def save(db: DBContext, blocks: Map[(String, String), Array[Byte]]): Future[Boolean] = {
    val stm = BatchStatement.builder(DefaultBatchType.LOGGED)

    blocks.map { case ((partition, id), bin) =>
      stm.addStatement(INSERT
        .bind()
        .setString(0, partition)
        .setString(1, id)
        .setByteBuffer(2, ByteBuffer.wrap(bin))
        .setInt(3, bin.length)
      )
    }

    session.executeAsync(stm.build()).flatMap{ ok =>
      if(ok.wasApplied()) updateDB(db) else Future.successful(false)
    }
  }

  override def close(): Future[Unit] = {
    session.closeAsync().map{_ => {}}
  }

}
