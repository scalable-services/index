package services.scalable.index.impl

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, DefaultBatchType}
import org.slf4j.LoggerFactory
import services.scalable.index._

import java.nio.ByteBuffer
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters._

class CassandraStorage[K, V](val KEYSPACE: String,
                                       val NUM_LEAF_ENTRIES: Int,
                                       val NUM_META_ENTRIES: Int,
                                       val truncate: Boolean = true)(implicit val ec: ExecutionContext,
                                                           val ord: Ordering[K],
                                                           val cache: Cache[K,V],
                                                           val serializer: Serializer[Block[K, V]]) extends Storage[K,V] {

  val logger = LoggerFactory.getLogger(this.getClass)
  val parents = TrieMap.empty[String, (Option[String], Int)]

  val session = CqlSession
    .builder()
    .withConfigLoader(loader)
    .withKeyspace(KEYSPACE)
    .build()

  val INSERT = session.prepare("insert into blocks(id, bin, leaf, size) values (?, ?, ?, ?);")
  val SELECT_ROOT = session.prepare("select * from meta where id=?;")
  val INSERT_META = session.prepare("insert into meta(id, root, num_leaf_entries, num_meta_entries) VALUES(?,?,?,?) IF NOT EXISTS;")
  val SELECT = session.prepare("select * from blocks where id=?;")
  val UPDATE_META = session.prepare("update meta set root = ? where id = ? IF EXISTS;")

  if(truncate){
    logger.debug(s"TRUNCATED BLOCKS: ${session.execute("TRUNCATE blocks;").wasApplied()}\n")
    logger.debug(s"TRUNCATED META: ${session.execute("TRUNCATE meta;").wasApplied()}\n")
  }

  override def createIndex(indexId: String): Future[Context[K,V]] = {
    val ctx = new DefaultContext[K,V](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, this, cache, ord)

    session.executeAsync(INSERT_META.bind()
      .setString(0, ctx.indexId)
      .setString(1, indexId)
      .setInt(2, ctx.NUM_LEAF_ENTRIES)
      .setInt(3, ctx.NUM_META_ENTRIES)).flatMap {
      case r if r.wasApplied() => Future.successful(ctx)
      case _ => Future.failed(Errors.INDEX_ALREADY_EXISTS(indexId))
    }
  }

  override def loadOrCreate(indexId: String): Future[Context[K,V]] = {
    load(indexId).recoverWith {
      case e: Errors.INDEX_NOT_FOUND => createIndex(indexId)
      case e => Future.failed(e)
    }
  }

  override def load(indexId: String): Future[Context[K,V]] = {
    session.executeAsync(SELECT_ROOT.bind().setString(0, indexId)).flatMap { rs =>
      val one = rs.one()

      if(one == null){
        Future.failed(Errors.INDEX_NOT_FOUND(indexId))
      } else {
        val r = one.getString("root")
        val NUM_LEAF_ENTRIES = one.getInt("num_leaf_entries")
        val NUM_META_ENTRIES = one.getInt("num_meta_entries")

        val root: Option[String] = if(r == null) None else Some(r)
        val ctx = new DefaultContext(indexId, root, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, this, cache, ord)

        Future.successful(ctx)
      }
    }
  }

  override def get(unique_id: String)(implicit ctx: Context[K,V]): Future[Block[K,V]] = {
    session.executeAsync(SELECT.bind().setString(0, unique_id)).map { rs =>
      val one = rs.one()
      val buf = one.getByteBuffer("bin")
      serializer.deserialize(buf.array())
    }
  }

  def updateMeta(ctx: Context[K,V]): Future[Boolean] = {
    session.executeAsync(UPDATE_META.bind().setString(0, if(ctx.root.isDefined) ctx.root.get else null)
      .setString(1, ctx.indexId)).map(_.wasApplied())
  }

  override def save(ctx: Context[K,V]): Future[Boolean] = {

    val c = ctx.asInstanceOf[DefaultContext[K,V]]

    val stm = BatchStatement.builder(DefaultBatchType.LOGGED)

    logger.debug(s"ctx root: ${ctx.root}")

    //logger.debug(s"${Console.MAGENTA_B}saving blocks ${blocks.map(_.parent)}${Console.RESET}")

    val blocks = c.blocks.map(_._2)

    blocks.map { b =>
      val bin = serializer.serialize(b)

      b match {
        case b: Leaf[K,V] => logger.debug(s"root ${ctx.root} => "+ b.unique_id)
        case _ =>
      }

      stm.addStatement(INSERT.bind()
        .setString(0, b.unique_id)
        .setByteBuffer(1, ByteBuffer.wrap(bin))
        .setBoolean(2, b.isInstanceOf[Leaf[K,V]])
        .setInt(3, bin.length)
      )
    }

    session.executeAsync(stm.build()).flatMap(ok => if(ok.wasApplied()) updateMeta(ctx) else Future.successful(false))
  }

  override def close(): Future[Unit] = {
    session.closeAsync().map{_ => {}}
  }

}
