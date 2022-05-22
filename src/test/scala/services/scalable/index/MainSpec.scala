package services.scalable.index

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc._
import services.scalable.index.impl._

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class MainSpec extends Repeatable {

  override val times: Int = 1

  "operations" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Bytes
    type V = Bytes

    import services.scalable.index.DefaultComparators._

    val NUM_LEAF_ENTRIES = 4//rand.nextInt(5, 64)
    val NUM_META_ENTRIES = 4//rand.nextInt(5, 64)

    val indexId = UUID.randomUUID().toString

    import services.scalable.index.DefaultSerializers._

    implicit val idGenerator = new IdGenerator {
      override def generateId[K, V](ctx: Context[K, V]): String = UUID.randomUUID.toString
      override def generatePartition[K, V](ctx: Context[K, V]): String = "p0"
    }

    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    //implicit val storage = new CassandraStorage(TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, false)

    var db = Await.result(storage.loadOrCreate(indexId), Duration.Inf)
    val indexContext = if(db.latest.indexes.isEmpty) IndexContext(indexId, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, None, 0, 0)
      else db.latest.indexes.head._2

    var data = Seq.empty[(K, V)]
    val index = new QueryableIndex[K, V](indexContext)

    def insert(): Unit = {
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
      val result = Await.result(index.execute(cmds), Duration.Inf)

      if(result){
        data = data ++ list
      }
    }

    insert()

    db = db.withLatest(
      IndexView(
        System.nanoTime(),
        Map(
          "main" -> index.save()
        )
      )
    )

    logger.info(Await.result(storage.save(db, index.ctx.blocks.map{case (id, block) => id -> grpcBytesSerializer.serialize(block)}.toMap),
      Duration.Inf).toString)

    val dlist = data.sortBy(_._1)
    val ilist = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)

    logger.debug(s"${Console.GREEN_B}tdata: ${dlist.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata: ${ilist.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

    assert(TestHelper.isColEqual(dlist, ilist))
  }

}
