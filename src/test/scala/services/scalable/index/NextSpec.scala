package services.scalable.index

import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.index.impl._

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class NextSpec extends AnyFlatSpec with Repeatable {

  val logger = LoggerFactory.getLogger(this.getClass)

  override val times: Int = 1

  val rand = ThreadLocalRandom.current()

  "index data " must "be equal to test data" in {

    type K = Array[Byte]
    type V = Array[Byte]
    type S = String

    implicit def xf(k: K): String = new String(k)

    import DefaultComparators._
    import DefaultSerializers._

    val NUM_LEAF_ENTRIES = 8//rand.nextInt(4, 100)
    val NUM_META_ENTRIES = 8//rand.nextInt(4, NUM_LEAF_ENTRIES)

    //implicit val storage = new MemoryStorage[K, V]()

    val indexId = "test_index"

    implicit val cache = new DefaultCache()
    implicit val storage = new CassandraStorage(TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, truncate = true)

    implicit val ctx = Await.result(storage.createIndex(indexId), Duration.Inf)

    var data = Seq.empty[Tuple]
    val iter = 20//rand.nextInt(1, 100)

    //implicit val ctx = new DefaultContext(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    //val index = new Index[S, K, V]()

    def insert(index: Index): Unit = {

      val n = rand.nextInt(1, 100)

      var list = Seq.empty[Tuple]

      for(i<-0 until n){
        val k = RandomStringUtils.randomAlphanumeric(5).getBytes("UTF-8")
        val v = k.clone()//RandomStringUtils.randomAlphanumeric(5)

        if(!list.exists{case (k1, _) => ord.equiv(k, k1)} && !data.exists{ case (k1, _) =>
          ord.equiv(k1, k)}){
          list = list :+ k -> v
        }
      }

      val m = Await.result(index.insert(list), Duration.Inf)

      logger.debug(s"insertion result n: $m")

      data = data ++ list.slice(0, m)
    }

    def remove(index: Index): Unit = {
      if(data.isEmpty) return

      val bound = if(data.length == 1) 1 else rand.nextInt(1, data.length)
      val list = scala.util.Random.shuffle(data.slice(0, bound)).map(_._1).toSeq

      val m = Await.result(index.remove(list), Duration.Inf)

      logger.debug(s"removal result m: $m")
      data = data.filterNot{case (k, _) => list.exists{k1 => ord.equiv(k, k1)}}
    }

    def update(index: Index): Unit = {
      if(data.isEmpty) return

      val bound = if(data.length == 1) 1 else rand.nextInt(1, data.length)
      val list = scala.util.Random.shuffle(data.slice(0, bound))
        .map{case (k, _) => k -> RandomStringUtils.randomAlphabetic(5).getBytes("UTF-8")}.toSeq

      val m = Await.result(index.update(list), Duration.Inf)

      logger.debug(s"update result m: $m")

      val notin = data.filterNot{case (k1, _) => list.exists{case (k, _) => ord.equiv(k, k1)}}
      data = (notin ++ list).sortBy(_._1)
    }

   // implicit val storage = new CassandraStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES, "indexes")

    for(i<-0 until iter){

      //implicit val ctx = new DefaultContext(indexId, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
      val index = new Index()

      rand.nextInt(1, 4) match {
        case 1 => insert(index)
        case 2 => update(index)
        case 3 => remove(index)
      }

      Await.ready(ctx.save(), Duration.Inf)
    }

    val index = new Index()

    val tdata = data.sortBy(_._1)

    var list = Seq.empty[Tuple2[Bytes, Bytes]]

    var cur: Option[Leaf] = Await.result(index.first(), Duration.Inf)

    while(cur.isDefined){
      val block = cur.get
      list = list ++ block.inOrder()
      cur = Await.result(index.next(cur.map(_.unique_id)), Duration.Inf)
    }

    logger.debug(s"next: ${list.map{case (k, v) => new String(k) -> new String(v)}}\n")

    list = Await.result(index.inOrder(), Duration.Inf)

    logger.debug(s"tdata: ${tdata.map{case (k, v) => new String(k) -> new String(v)}}\n")
    logger.debug(s"idata: ${list.map{case (k, v) => new String(k) -> new String(v)}}")

    assert(isColEqual(tdata, list))

    val reverse = tdata.reverse

    list = Seq.empty[Tuple2[Bytes, Bytes]]
    cur = Await.result(index.last(), Duration.Inf)

    while(cur.isDefined){
      val block = cur.get
      list = list ++ block.inOrder().reverse

      cur = Await.result(index.prev(cur.map(_.unique_id)), Duration.Inf)
    }

    assert(isColEqual(reverse, list))

  }

}
