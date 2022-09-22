package services.scalable.index

import io.netty.util.internal.ThreadLocalRandom
import org.slf4j.LoggerFactory
import services.scalable.index.impl._

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class LongDBSpec extends Repeatable {

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

    val dbId = TestConfig.DATABASE
    val indexId = "main"

    import services.scalable.index.DefaultSerializers._

    implicit val longBytesSerializer = new GrpcByteSerializer[K, V]()

    implicit val idGenerator = new IdGenerator {
      override def generateId[K, V](ctx: Context[K, V]): String = "1"
      override def generatePartition[K, V](ctx: Context[K, V]): String = UUID.randomUUID.toString
    }

    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    //implicit val storage = new CassandraStorage(TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, false)

    val dbCtx = Await.result(storage.loadOrCreateDB(dbId), Duration.Inf)

    val db = new DB[K, V](dbCtx)
    var data = Seq.empty[(K, V)]

    db.createIndex("main", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    db.createHistory("history", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    def insert(): Seq[Commands.Command[K, V]] = {
      val n = 100//rand.nextInt(1, 100)
      var list = Seq.empty[Tuple2[K, V]]

      for(i<-0 until n){
        val k = rand.nextLong()
        val v = rand.nextLong()//RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        if(!data.exists{case (k1, _) => ordLong.equiv(k, k1)} && !list.exists{case (k1, _) => ordLong.equiv(k, k1)}){
          list = list :+ (k -> v)
        }
      }

      val cmds = Seq(
        Commands.Insert(indexId, list)
      )

      cmds
    }

    var result = Await.result(db.execute(insert()), Duration.Inf)
    result = Await.result(db.execute(insert()), Duration.Inf)

    logger.info(s"\n${Console.MAGENTA_B}result: ${result}${Console.RESET}\n")

    val dbCtxSaved = Await.result(db.save(), Duration.Inf)

    val db2 = new DB[K, V](dbCtxSaved)

    val t0 = 0L
    val t1 = System.nanoTime()

    val db2t0 = Await.result(db2.findIndex(t0, "main"), Duration.Inf)

    val list = Await.result(TestHelper.all(db2t0.get.inOrder()), Duration.Inf)

    val t0Index = Await.result(db.findIndex(t0, "main"), Duration.Inf).get
    val t1Index = Await.result(db.findIndex(t1, "main"), Duration.Inf).get

    val t0list = Await.result(TestHelper.all(t0Index.inOrder()), Duration.Inf)
    val t1list = Await.result(TestHelper.all(t1Index.inOrder()), Duration.Inf)

    val latest = Await.result(TestHelper.all(db.indexes("main").inOrder()), Duration.Inf)

    logger.debug(s"${Console.GREEN_B}t0: ${t0list.map{case (k, v, _) => k -> v}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}t1: ${t1list.map{case (k, v, _) => k -> v}}${Console.RESET}\n")
    logger.debug(s"${Console.YELLOW_B}latest: ${latest.map{case (k, v, _) => k -> v}}${Console.RESET}\n")

    logger.debug(s"${Console.CYAN_B}db2 main: ${list.map{case (k, v, _) => k -> v}}${Console.RESET}\n")
  }

}
