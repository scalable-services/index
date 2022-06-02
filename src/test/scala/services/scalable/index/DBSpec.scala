package services.scalable.index

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc._
import services.scalable.index.impl._
import com.google.protobuf.any.Any

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.Await
import scala.concurrent.duration.{DAYS, Duration}

class DBSpec extends Repeatable {

  override val times: Int = 1

  "operations" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)
        
    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Bytes
    type V = Bytes

    import services.scalable.index.DefaultComparators._

    val NUM_LEAF_ENTRIES = 32
    val NUM_META_ENTRIES = 32

    val indexId = "main"

    import services.scalable.index.DefaultSerializers._

    implicit val idGenerator = new IdGenerator {
      override def generateId[K, V](ctx: Context[K, V]): String = "1"
      override def generatePartition[K, V](ctx: Context[K, V]): String = UUID.randomUUID.toString
    }

    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    //implicit val storage = new CassandraStorage(TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, false)

    val db = new DB[K, V]()
    var data = Seq.empty[(K, V)]

    db.createIndex("main", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    db.createHistory("history", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    def insert(): Seq[Commands.Command[K, V]] = {
      val n = 100//rand.nextInt(1, 100)
      var list = Seq.empty[Tuple[K, V]]

      for(i<-0 until n){
        val k = RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
        val v = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        if(!data.exists{case (k1, _) => ord.equiv(k, k1)}){
          list = list :+ (k -> v)
        }
      }

      val cmds = Seq(
        Commands.Insert(indexId, list)
      )

      cmds
    }

    val t0 = System.nanoTime()
    var result = Await.result(db.execute(insert()), Duration.Inf)
    result = Await.result(db.execute(insert()), Duration.Inf)

    logger.info(s"\n${Console.MAGENTA_B}result: ${result}${Console.RESET}\n")

    val ok = Await.result(db.save(), Duration.Inf)

    val db2 = new DB[K, V](db.ctx)

    val t1 = System.nanoTime()

    val db2t0 = Await.result(db2.findIndex(t0, "main"), Duration.Inf)

    val list = Await.result(TestHelper.all(db2t0.get.inOrder()), Duration.Inf)

    val t0Index = Await.result(db.findIndex(t0, "main"), Duration.Inf).get
    val t1Index = Await.result(db.findIndex(t1, "main"), Duration.Inf).get

    val t0list = Await.result(TestHelper.all(t0Index.inOrder()), Duration.Inf)
    val t1list = Await.result(TestHelper.all(t1Index.inOrder()), Duration.Inf)

    val latest = Await.result(TestHelper.all(db.indexes("main").inOrder()), Duration.Inf)

    logger.debug(s"${Console.GREEN_B}t0: ${t0list.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}t1: ${t1list.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.YELLOW_B}latest: ${latest.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

    logger.debug(s"${Console.CYAN_B}db2 main: ${list.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
  }

}
