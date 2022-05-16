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
import scala.concurrent.duration.Duration

class DBSpec extends Repeatable {

  override val times: Int = 1

  "operations" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)
        
    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Bytes
    type V = Bytes

    import services.scalable.index.DefaultComparators._

    val NUM_LEAF_ENTRIES = 4
    val NUM_META_ENTRIES = 4

    val indexId = "main"

    import services.scalable.index.DefaultSerializers._

    implicit val idGenerator = new IdGenerator {
      override def generateId[K, V](ctx: Context[K, V]): String = "1"
      override def generatePartition[K, V](ctx: Context[K, V]): String = UUID.randomUUID.toString
    }

    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    //implicit val storage = new CassandraStorage(TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, false)

    val view = IndexView(System.nanoTime(), Map(
      "main" -> IndexContext(indexId, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, None, 0, 0)
    ))

    val dbCtx = Await.result(storage.loadOrCreate(indexId), Duration.Inf)
      .withLatest(view)
      .withHistory(IndexContext(s"$indexId-history", NUM_LEAF_ENTRIES, NUM_META_ENTRIES, None, 0, 0))

    val db = new DB[K, V](dbCtx)
    var data = Seq.empty[(K, V)]

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

    var cmds = insert()

    val t0 = System.nanoTime()
    var result = Await.result(db.execute(cmds), Duration.Inf)

   // dbCtx = result.ctx.get

    logger.info(Await.result(storage.save(dbCtx, result.blocks),
      Duration.Inf).toString)

    //dbCtx = result.ctx.get
    //db = new DB[K, V](dbCtx)

    cmds = insert()

    result = Await.result(db.execute(cmds), Duration.Inf)

    //dbCtx = result.ctx.get

    logger.info(s"\n${Console.MAGENTA_B}result: ${result.ok}${Console.RESET}\n")

    if(result.ok){

      //dbCtx = result.ctx.get
      //db = new DB[K, V](dbCtx)

      logger.info(Await.result(storage.save(dbCtx, result.blocks), Duration.Inf).toString)

      val t1 = System.nanoTime()

      val t0Index = Await.result(db.findIndex(t0, "main"), Duration.Inf).get//last.tuples.head._2
      val t1Index = Await.result(db.findIndex(t1, "main"), Duration.Inf).get //last.tuples.last._2

      val t0list = Await.result(TestHelper.all(t0Index.inOrder()), Duration.Inf)
      val t1list = Await.result(TestHelper.all(t1Index.inOrder()), Duration.Inf)

      logger.debug(s"${Console.GREEN_B}t0: ${t0list.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
      logger.debug(s"${Console.MAGENTA_B}t1: ${t1list.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

      //assert(isColEqual(dlist, ilist))
    }
  }

}
