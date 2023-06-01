package services.scalable.index.test

import io.netty.util.internal.ThreadLocalRandom
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.{IndexContext, TemporalContext}
import services.scalable.index.impl._
import services.scalable.index.{Context, DefaultComparators, DefaultSerializers, IdGenerator, IndexBuilder, TemporalIndex}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import services.scalable.index.DefaultPrinters._

class LoadTemporalIndexFromStorageSpec extends Repeatable {

  override val times: Int = 1

  "operations" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)
        
    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Long
    type V = Long

    import services.scalable.index.DefaultComparators._
    import services.scalable.index.DefaultSerializers._

    implicit val longBytesSerializer = new GrpcByteSerializer[K, V]()

    val dbId = TestConfig.DATABASE

    implicit val idGenerator = new IdGenerator {
      override def generateId[K, V](ctx: Context[K, V]): String = "1"
      override def generatePartition[K, V](ctx: Context[K, V]): String = UUID.randomUUID.toString
    }

    //implicit val storage = new MemoryStorage()
    implicit val storage = new CassandraStorage(TestConfig.session, false)

    val dbCtx = Await.result(TestHelper.loadOrCreateTemporalIndex(
      TemporalContext(dbId)
    ), Duration.Inf).get

    val grpcLongLongSerializer = new GrpcByteSerializer[Long, Long]()

    val indexBuilder = IndexBuilder.create[K, V](DefaultComparators.ordLong)
      .storage(storage)
      .serializer(grpcLongLongSerializer)

    val historyBuilder = IndexBuilder.create[Long, IndexContext](DefaultComparators.ordLong)
      .storage(storage)
      .serializer(DefaultSerializers.grpcLongIndexContextSerializer)

    val db = new TemporalIndex[K, V](dbCtx)(indexBuilder, historyBuilder)

    val t0 = 0L
    val t1 = System.nanoTime()

    val dbt0 = Await.result(db.findIndex(t0), Duration.Inf)

    val list = Await.result(TestHelper.all(dbt0.get.inOrder()), Duration.Inf)

    val t0Index = Await.result(db.findIndex(t0), Duration.Inf).get
    val t1Index = Await.result(db.findIndex(t1), Duration.Inf).get

    val t0list = Await.result(TestHelper.all(t0Index.inOrder()), Duration.Inf)
    val t1list = Await.result(TestHelper.all(t1Index.inOrder()), Duration.Inf)

    val latest = Await.result(TestHelper.all(db.findIndex().inOrder()), Duration.Inf)

    logger.debug(s"${Console.GREEN_B}t0: ${t0list.map { case (k, v, _) => k -> v }}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}t1: ${t1list.map { case (k, v, _) => k -> v }}${Console.RESET}\n")
    logger.debug(s"${Console.YELLOW_B}latest: ${latest.map { case (k, v, _) => k -> v }}${Console.RESET}\n")

    logger.debug(s"${Console.CYAN_B}hDB2 main: ${list.map { case (k, v, _) => k -> v }}${Console.RESET}\n")
  }

}
