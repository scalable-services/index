package services.scalable.index.test

import io.netty.util.internal.ThreadLocalRandom
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl._
import services.scalable.index.{Commands, Context, IdGenerator, QueryableIndex}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class LongIndexSpec extends Repeatable {

  override val times: Int = 1

  "operations" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Long
    type V = Long

    import services.scalable.index.DefaultComparators._

    val NUM_LEAF_ENTRIES = 4//rand.nextInt(5, 64)
    val NUM_META_ENTRIES = 4//rand.nextInt(5, 64)

    val indexId = "mysusindex"//UUID.randomUUID().toString

    import services.scalable.index.DefaultSerializers._

    implicit val idGenerator = new IdGenerator {
      override def generateId[K, V](ctx: Context[K, V]): String = UUID.randomUUID.toString
      override def generatePartition[K, V](ctx: Context[K, V]): String = "p0"
    }

    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new MemoryStorage()
    //implicit val storage = new CassandraStorage(TestConfig.KEYSPACE, false)

    val indexContext = Await.result(TestHelper.loadOrCreateIndex(
      IndexContext(indexId, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    ), Duration.Inf).get

    implicit val longLongBlockSerializer = new GrpcByteSerializer[Long, Long]()

    var data = Seq.empty[(K, V, Boolean)]
    val index = new QueryableIndex[K, V](indexContext)

    def insert(): Unit = {
      val n = 100//rand.nextInt(1, 100)
      var list = Seq.empty[Tuple3[K, V, Boolean]]

      for(i<-0 until n){
        val k = rand.nextLong(0, 10000)
        val v = rand.nextLong(0, 10000)

        if(!data.exists{case (k1, _, _) => ordLong.equiv(k, k1)} &&
          !list.exists{case (k1, _, _) => ordLong.equiv(k, k1)}){
          list = list :+ (k, v, false)
        }
      }

      val cmds = Seq(
        Commands.Insert(indexId, list)
      )
      val result = Await.result(index.execute(cmds), Duration.Inf)

      //index.snapshot()

      assert(result)

      if(result){
        data = data ++ list
      }
    }

    insert()
    insert()

    logger.info(Await.result(index.save(), Duration.Inf).toString)

    val dlist = data.sortBy(_._1).map{case (k, v, _) => k -> v}
    val ilist = Await.result(TestHelper.all(index.inOrder()), Duration.Inf).map{case (k, v, _) => k -> v}

    logger.debug(s"${Console.GREEN_B}tdata: ${dlist}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata: ${ilist}${Console.RESET}\n")

    assert(TestHelper.isColEqual(dlist, ilist))
  }

}
