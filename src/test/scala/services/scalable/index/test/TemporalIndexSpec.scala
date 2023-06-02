package services.scalable.index.test

import io.netty.util.internal.ThreadLocalRandom
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.{IndexContext, TemporalContext}
import services.scalable.index.impl._
import services.scalable.index.{Commands, Context, DefaultComparators, DefaultSerializers, IdGenerator, IndexBuilder, TemporalIndex}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import services.scalable.index.DefaultPrinters._

import scala.jdk.FutureConverters.CompletionStageOps

class TemporalIndexSpec extends Repeatable {

  override val times: Int = 1

  "operations" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)
        
    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Long
    type V = Long

    import services.scalable.index.DefaultComparators._

    val NUM_LEAF_ENTRIES = 32
    val NUM_META_ENTRIES = 32

    val historyIndexId = TestConfig.DATABASE
    val indexId = "main"

    import services.scalable.index.DefaultSerializers._

    implicit val longBytesSerializer = new GrpcByteSerializer[K, V]()

    implicit val idGenerator = new IdGenerator {
      override def generateId[K, V](ctx: Context[K, V]): String = "1"
      override def generatePartition[K, V](ctx: Context[K, V]): String = UUID.randomUUID.toString
    }

    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new MemoryStorage()
    val session = TestHelper.createCassandraSession()
    implicit val storage = new CassandraStorage(session, false)

    val tctx = Await.result(TestHelper.loadOrCreateTemporalIndex(TemporalContext(
      historyIndexId,
      IndexContext(historyIndexId, NUM_LEAF_ENTRIES, NUM_META_ENTRIES),
      IndexContext(s"$historyIndexId-history", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    )), Duration.Inf).get

    val grpcLongLongSerializer = new GrpcByteSerializer[Long, Long]()

    val indexBuilder = IndexBuilder.create[K, V](DefaultComparators.ordLong)
      .storage(storage)
      .serializer(grpcLongLongSerializer)

    val historyBuilder = IndexBuilder.create[Long, IndexContext](DefaultComparators.ordLong)
      .storage(storage)
      .serializer(DefaultSerializers.grpcLongIndexContextSerializer)

    val hDB = new TemporalIndex[K, V](tctx)(indexBuilder, historyBuilder)
    var data = Seq.empty[(K, V, Boolean)]

    def insert(): Seq[Commands.Command[K, V]] = {
      val n = 100//rand.nextInt(1, 100)
      var list = Seq.empty[Tuple3[K, V, Boolean]]

      for(i<-0 until n){
        val k = rand.nextLong()
        val v = rand.nextLong()//RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        if(!data.exists{case (k1, _, _) => ordLong.equiv(k, k1)} && !list.exists{case (k1, _, _) => ordLong.equiv(k, k1)}){
          list = list :+ (k, v, false)
        }
      }

      val cmds = Seq(
        Commands.Insert(indexId, list)
      )

      cmds
    }

    var result = Await.result(hDB.execute(insert()), Duration.Inf)

    // Explicitly save snapshot
    Await.result(hDB.snapshot(), Duration.Inf)

    result = Await.result(hDB.execute(insert()), Duration.Inf)

    // Explicitly save snapshot
    Await.result(hDB.snapshot(), Duration.Inf)

    logger.info(s"\n${Console.MAGENTA_B}result: ${result}${Console.RESET}\n")

    val hdbCtxSaved = Await.result(hDB.save(), Duration.Inf)

    val hDB2 = new TemporalIndex[K, V](hdbCtxSaved)(indexBuilder, historyBuilder)

    val t0 = 0L
    val t1 = System.nanoTime()

    val hdb2t0 = Await.result(hDB2.findIndex(t0), Duration.Inf)

    val list = Await.result(TestHelper.all(hdb2t0.get.inOrder()), Duration.Inf)

    val t0Index = Await.result(hDB.findIndex(t0), Duration.Inf).get
    val t1Index = Await.result(hDB.findIndex(t1), Duration.Inf).get

    val t0list = Await.result(TestHelper.all(t0Index.inOrder()), Duration.Inf)
    val t1list = Await.result(TestHelper.all(t1Index.inOrder()), Duration.Inf)

    val latest = Await.result(TestHelper.all(hDB.findIndex().inOrder()), Duration.Inf)

    logger.debug(s"${Console.GREEN_B}t0: ${t0list.map{case (k, v, _) => k -> v}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}t1: ${t1list.map{case (k, v, _) => k -> v}}${Console.RESET}\n")
    logger.debug(s"${Console.YELLOW_B}latest: ${latest.map{case (k, v, _) => k -> v}}${Console.RESET}\n")

    logger.debug(s"${Console.CYAN_B}hDB2 main: ${list.map{case (k, v, _) => k -> v}}${Console.RESET}\n")

    Await.result(session.closeAsync().asScala, Duration.Inf)
  }

}
