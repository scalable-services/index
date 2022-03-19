package services.scalable.index

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.impl.{CassandraStorage, DefaultCache, DefaultContext, GrpcByteSerializer}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class CQLStorageSpec extends Repeatable {

  override val times: Int = 1

  "index " should " be stored and later read successfully" in {

    type T = services.scalable.index.QueryableIndex[K, V]
    val logger = LoggerFactory.getLogger(this.getClass)
    
    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Bytes
    type V = Bytes

    import services.scalable.index.DefaultComparators._

    val NUM_LEAF_ENTRIES = 8
    val NUM_META_ENTRIES = 8

    val indexId = "test"

    import DefaultSerializers._

    implicit val cache = new DefaultCache[K, V](MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new CassandraStorage[Bytes, Bytes](TestConfig.KEYSPACE, NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES, truncate = true)

    implicit val ctx = Await.result(storage.loadOrCreate(indexId), Duration.Inf)

    //implicit val ctx = new DefaultContext[K, V](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    val index = new QueryableIndex[K, V](ctx)

    var data = Seq.empty[(K, V)]

    val prefixes = (0 until 10).map{_ => RandomStringUtils.randomAlphanumeric(4)}.distinct
      .map(_.getBytes(Charsets.UTF_8))

    def insert(): Unit = {
      val n = rand.nextInt(1, 100)
      var list = Seq.empty[Tuple[K, V]]

      for(i<-0 until n){
        val prefix = prefixes(rand.nextInt(0, prefixes.length))
        val k = prefix ++ RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
        val v = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        if(!data.exists{case (k1, _) => ord.equiv(k, k1)}){
          list = list :+ (k -> v)
        }
      }

      val result = Await.result(index.insert(list), Duration.Inf)

      if(result > 0){
        data = data ++ list.slice(0, result)
      }
    }

    def remove(): Unit = {
      if(data.isEmpty) return

      val bound = if(data.length == 1) 1 else rand.nextInt(1, data.length)
      val list = scala.util.Random.shuffle(data.slice(0, bound)).map(_._1)

      val m = Await.result(index.remove(list), Duration.Inf)

      logger.debug(s"removal result m: $m")
      data = data.filterNot{case (k, _) => list.exists{k1 => ord.equiv(k, k1)}}
    }

    def update(): Unit = {
      if(data.isEmpty) return

      val bound = if(data.length == 1) 1 else rand.nextInt(1, data.length)
      val list = scala.util.Random.shuffle(data.slice(0, bound))
        .map{case (k, _) => k -> RandomStringUtils.randomAlphabetic(5).getBytes("UTF-8")}

      val m = Await.result(index.update(list), Duration.Inf)

      logger.debug(s"update result m: $m")

      val notin = data.filterNot{case (k1, _) => list.exists{case (k, _) => ord.equiv(k, k1)}}
      data = (notin ++ list).sortBy(_._1)
    }

    val iter = rand.nextInt(10, 100)

    for(i<-0 until iter){

      rand.nextInt(1, 4) match {
        case 1 => insert()
        case 2 => remove()
        case 3 => update()
        case _ =>
      }
    }

    Await.ready(ctx.save(), Duration.Inf)

    val tdata = data.sortBy(_._1)
    val idata = Await.result(TestHelper.all(index.inOrder()), Duration.Inf).toSeq

    logger.debug(s"${Console.GREEN_B}tdata: ${tdata.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata: ${idata.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

    isColEqual(idata, tdata)
  }

}
