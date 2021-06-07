package services.scalable.index

import com.google.common.base.Charsets
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.index.impl._

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class NextSpec extends AnyFlatSpec with Repeatable {

  val logger = LoggerFactory.getLogger(this.getClass)

  override val times: Int = 100

  val rand = ThreadLocalRandom.current()

  "index data " must "be equal to test data" in {

    implicit def xf(k: Bytes): String = new String(k)

    import DefaultComparators._
    import DefaultSerializers._

    val NUM_LEAF_ENTRIES = rand.nextInt(8, 64)
    val NUM_META_ENTRIES = rand.nextInt(8, 64)

    //implicit val storage = new MemoryStorage[K, V]()

    val indexId = "demo_index"

    import DefaultSerializers._
    implicit val serializer = new GrpcByteSerializer[Bytes, Bytes]()

    implicit val cache = new DefaultCache[Bytes, Bytes]()
    implicit val storage = new CassandraStorage[Bytes, Bytes](TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, truncate = true)

    implicit val ctx = Await.result(storage.createIndex(indexId), Duration.Inf)

    val index = new Index[Bytes,Bytes]()

    var data = Seq.empty[Tuple[Bytes, Bytes]]
    val iter = rand.nextInt(1, 20)

    var tasks = Seq.empty[() => Future[Boolean]]

    def insert(index: Index[Bytes,Bytes]): Future[Boolean] = {

      val n = rand.nextInt(1, 100)

      var list = Seq.empty[Tuple[Bytes, Bytes]]

      for(i<-0 until n){
        val k = RandomStringUtils.randomAlphanumeric(5).getBytes("UTF-8")
        val v = k.clone()//RandomStringUtils.randomAlphanumeric(5)

        if(!list.exists{case (k1, _) => ord.equiv(k, k1)} && !data.exists{ case (k1, _) =>
          ord.equiv(k1, k)}){
          list = list :+ k -> v
        }
      }

      index.insert(list).map { n =>
        data = data ++ list.slice(0, n)
        logger.debug(s"insertion result n: $n")
        true
      }
    }

    def remove(index: Index[Bytes,Bytes]): Future[Boolean] = {
      if(data.isEmpty) return Future.successful(true)

      val bound = if(data.length == 1) 1 else rand.nextInt(1, data.length)
      val list = scala.util.Random.shuffle(data.slice(0, bound)).map(_._1)

      index.remove(list).map { n =>
        logger.debug(s"removal result m: $n")
        data = data.filterNot{case (k, _) => list.exists{k1 => ord.equiv(k, k1)}}

        true
      }
    }

    def update(index: Index[Bytes,Bytes]): Future[Boolean] = {
      if(data.isEmpty) return Future.successful(true)

      val bound = if(data.length == 1) 1 else rand.nextInt(1, data.length)
      val list = scala.util.Random.shuffle(data.slice(0, bound))
        .map{case (k, _) => k -> RandomStringUtils.randomAlphabetic(5).getBytes("UTF-8")}

      index.update(list).map { n =>
        logger.debug(s"update result m: $n")

        val notin = data.filterNot{case (k1, _) => list.exists{case (k, _) => ord.equiv(k, k1)}}
        data = (notin ++ list).sortBy(_._1)

        true
      }
    }

    for(i<-0 until iter){
      tasks = tasks :+ (() => rand.nextInt(1, 4) match {
        case 1 => insert(index).flatMap(_ => ctx.save())
        case 2 => update(index).flatMap(_ => ctx.save())
        case 3 => remove(index).flatMap(_ => ctx.save())
      })
    }

    Await.ready(serialiseFutures(tasks)(_.apply()), Duration.Inf)

    var it = index.inOrder()

    var idata = Await.result(TestHelper.all(it), Duration.Inf)
    var list = data.sortBy(_._1)

    logger.debug(s"tdata: ${idata.map{case (k, v) => new String(k) -> new String(v)}}\n")
    logger.debug(s"idata: ${list.map{case (k, v) => new String(k) -> new String(v)}}")

    assert(isColEqual(list, idata))

    list = Seq.empty[Tuple[Bytes, Bytes]]

    it = index.reverse()

    idata = Await.result(TestHelper.all(it), Duration.Inf)
    list = data.reverse

    logger.debug(s"tdata: ${idata.map{case (k, v) => new String(k) -> new String(v)}}\n")
    logger.debug(s"idata: ${list.map{case (k, v) => new String(k) -> new String(v)}}")

    assert(isColEqual(list, idata))

   // Await.ready(storage.close(), Duration.Inf)
  }

}
