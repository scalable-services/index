package services.scalable.index.impl

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, DefaultBatchType}
import org.slf4j.LoggerFactory
import services.scalable.index._
import services.scalable.index.grpc.{DatabaseContext, IndexContext}

import java.nio.ByteBuffer
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import com.google.protobuf.any.Any

import scala.jdk.FutureConverters._

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

  val INSERT = session.prepare("insert into blocks(id, bin, size) values (?, ?, ?);")
  val SELECT_ROOT = session.prepare("select buf from databases where id=?;")
  val INSERT_DB = session.prepare("insert into databases(id, buf) VALUES(?,?) IF NOT EXISTS;")
  val SELECT = session.prepare("select * from blocks where id=?;")
  val UPDATE_DB = session.prepare("update databases set buf=? where id = ? IF EXISTS;")

  if(truncate){
    logger.debug(s"TRUNCATED BLOCKS: ${session.execute("TRUNCATE table databases;").wasApplied()}\n")
    logger.debug(s"TRUNCATED META: ${session.execute("TRUNCATE table blocks;").wasApplied()}\n")
  }

  override def createIndex(name: String): Future[DatabaseContext] = {
    val db = DatabaseContext(name)
    val buf = ByteBuffer.wrap(Any.pack(db).toByteArray)

    session.executeAsync(INSERT_DB.bind()
      .setString(0, db.name)
      .setByteBuffer(1, buf)).flatMap {
      case r if r.wasApplied() => Future.successful(db)
      case _ => Future.failed(Errors.INDEX_ALREADY_EXISTS(db.name))
    }
  }

  override def loadOrCreate(name: String): Future[DatabaseContext] = {
    load(name).recoverWith {
      case e: Errors.INDEX_NOT_FOUND => createIndex(name)
      case e => Future.failed(e)
    }
  }

  override def load(name: String): Future[DatabaseContext] = {
    session.executeAsync(SELECT_ROOT.bind().setString(0, name)).flatMap { rs =>
      val one = rs.one()

      if(one == null){
        Future.failed(Errors.INDEX_NOT_FOUND(name))
      } else {
        val r = one.getByteBuffer("buf")
        val db = Any.parseFrom(r.array()).unpack(DatabaseContext)

        Future.successful(db)
      }
    }
  }

  override def get[K, V](unique_id: String)(implicit serializer: Serializer[Block[K, V]]): Future[Block[K,V]] = {
    session.executeAsync(SELECT.bind().setString(0, unique_id)).map { rs =>
      val one = rs.one()
      val buf = one.getByteBuffer("bin")
      serializer.deserialize(buf.array())
    }
  }

  def updateDB(db: DatabaseContext): Future[Boolean] = {
    val buf = ByteBuffer.wrap(Any.pack(db).toByteArray)

    session.executeAsync(UPDATE_DB.bind().setByteBuffer(0, buf)
      .setString(1, db.name)).map(_.wasApplied())
  }

  override def save(db: DatabaseContext, blocks: Map[String, Array[Byte]]): Future[Boolean] = {
    val stm = BatchStatement.builder(DefaultBatchType.LOGGED)

    blocks.map { case (uid, bin) =>
      stm.addStatement(INSERT
        .bind()
        .setString(0, uid)
        .setByteBuffer(1, ByteBuffer.wrap(bin))
        .setInt(2, bin.length)
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
