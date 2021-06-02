package services.scalable.index

import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.index.impl.{DefaultCache, DefaultContext, GrpcByteSerializer, MemoryStorage}

import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

class SerializeOpsSpec extends AnyFlatSpec {

  val rand = ThreadLocalRandom.current()

  val logger = LoggerFactory.getLogger(this.getClass)

  "it " should "execute operations serially and successfully " in {

    import DefaultComparators._
    import DefaultIdGenerators._

    val NUM_LEAF_ENTRIES = 64
    val NUM_META_ENTRIES = 64

    val indexId = "demo_db"

    import DefaultSerializers._
    implicit val serializer = new GrpcByteSerializer[Bytes, Bytes]()

    implicit val cache = new DefaultCache[Bytes, Bytes](100L * 1024L * 1024L, 10000)
    //implicit val storage = new CassandraStorage(TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, truncate = true)

    implicit val storage = new MemoryStorage[Bytes, Bytes](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    implicit val ctx = new DefaultContext[Bytes, Bytes](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    logger.debug(s"${Await.result(storage.loadOrCreate(indexId), Duration.Inf)}")

    val index = new Index()

    val EMPTY = Array.empty[Byte]

    var tasks = Seq.empty[() => Future[Int]]

    val m0 = Runtime.getRuntime.totalMemory()

    for(i<-0 until 100){
      val list = (0 until 1000).map{_ => RandomStringUtils.randomAlphanumeric(10).getBytes() -> EMPTY}

      // Defer the operation using a callback
      tasks = tasks :+ (() => index.insert(list).map { n =>
          logger.debug(s"inserted at step ${i} inserted ${list.length}")
          n
      }.recover {
        case t: Throwable => logger.debug(s"${t}")
          0
      })
    }

    Await.result(serialiseFutures(tasks)(_.apply()), Duration.Inf)

    val mem = m0 - Runtime.getRuntime.freeMemory()

    logger.debug(s"${Await.result(ctx.save(), Duration.Inf)}")

    /*logger.debug(Await.result(index.inOrder(), Duration.Inf).map{case (k, _) =>
      new String(k)
    })*/

    logger.debug(s"${index.ctx.num_elements}")

    logger.debug(s"${Console.RED_B}USED MEMORY: ${mem/(1024*1024)} KBytes${Console.RESET}")

    Await.ready(storage.close(), Duration.Inf)
  }

}
