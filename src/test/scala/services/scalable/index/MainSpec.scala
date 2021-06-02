package services.scalable.index

import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.index.impl._

import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class MainSpec extends AnyFlatSpec with Repeatable {

  val logger = LoggerFactory.getLogger(this.getClass)

  override val times: Int = 100

  val rand = ThreadLocalRandom.current()

  "index data " must "be equal to test data" in {

    implicit def xf(k: Bytes): String = new String(k)

    import DefaultComparators._
    import DefaultIdGenerators._

    var versions = ArrayBuffer.empty[Tuple2[Seq[Tuple], Seq[Tuple]]]

    val NUM_LEAF_ENTRIES = 8//rand.nextInt(4, 100)
    val NUM_META_ENTRIES = 8//rand.nextInt(4, NUM_LEAF_ENTRIES)

    val indexId = "test_index"

    implicit val cache = new DefaultCache()
    //implicit val storage = new CassandraStorage(TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, truncate = true)
    implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    implicit val ctx = new DefaultContext(indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    
    var data = Seq.empty[Tuple]
    val iter = 10//rand.nextInt(1, 100)

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

      val before = data.sortBy(_._1)
      versions += before -> Await.result(index.inOrder(), Duration.Inf)
    }

    val index = new Index()

    val tdata = data.sortBy(_._1).toSeq
    val idata = Await.result(index.inOrder(), Duration.Inf)

    logger.debug(s"tdata: ${tdata.map{case (k, v) => new String(k) -> new String(v)}}\n")
    logger.debug(s"idata: ${idata.map{case (k, v) => new String(k) -> new String(v)}}")

    //assert(idata.equals(tdata))

    assert(isColEqual(idata, tdata), s"idata len: ${idata.length} tdata len: ${tdata.length}")

    logger.debug(s"\n")

    /*val levels = Await.result(index.getLevels(), Duration.Inf)

    logger.debug("BEGIN BTREE:\n")
    levels.keys.toSeq.sorted.foreach { case level =>
      logger.debug(s"level[$level]: ${levels(level)}\n")
    }
    logger.debug("END BTREE\n")*/

    /*index.prettyPrint()
    assert(false, "it was me your dumbass :P")*/

    var list = Seq.empty[Tuple2[Bytes, Bytes]]

    var cur: Option[Leaf] = Await.result(index.first(), Duration.Inf)

    while(cur.isDefined){
      val block = cur.get
      list = list ++ block.inOrder()

      cur = Await.result(index.next(cur.map(_.unique_id)), Duration.Inf)
    }

    logger.debug(s"next: ${list.map{case (k, v) => new String(k) -> new String(v)}}")

    //assert(list.equals(idata))

    assert(isColEqual(idata, list))

    cur = Await.result(index.last(), Duration.Inf)
    list = Seq.empty[Tuple]

    while(cur.isDefined){
      val block = cur.get
      list = list ++ block.inOrder().reverse

      cur = Await.result(index.prev(cur.map(_.unique_id)), Duration.Inf)
    }

    // logger.debug(s"next: ${list.map{case (k, v) => new String(k) -> new String(v)}}")

    val reversed = idata.reverse

    // logger.debug(s"reverse: ${list.map{case (k, v) => new String(k) -> new String(v)}}")

    //assert(list.equals(reversed))

    assert(isColEqual(reversed, list))

    if(!tdata.isEmpty){

      val (randomKey, _) = tdata(if(tdata.length == 1) 0 else rand.nextInt(0, tdata.length))
      val randomKeyStr = new String(randomKey)

      cur = Await.result(index.findPath(randomKey), Duration.Inf)

      list = Seq.empty[Tuple]

      while(cur.isDefined){
        val block = cur.get
        list = list ++ block.lt(randomKey)
        cur = Await.result(index.prev(cur.map(_.unique_id)), Duration.Inf)
      }

      val ltdata = tdata.filter{case (k, _) => ord.lt(k, randomKey)}.reverse

      assert(isColEqual(list, ltdata))

      val found = Await.result(index.find(randomKey), Duration.Inf)

      assert(found.isDefined)
      logger.debug(s"\nfound key ${randomKeyStr} = ${found.map{case (k, v) => new String(k) -> new String(v)}}")

      val tmin: Option[Tuple] = if(tdata.isEmpty) None else Some(tdata.min)
      val imin: Option[Tuple] = Await.result(index.min(), Duration.Inf)

      logger.debug(s"\ntmin: ${tmin.map{case (k, v) => new String(k) -> new String(v)}} imin: ${imin.map{case (k, v) => new String(k) -> new String(v)}}")

      assert(tmin.equals(imin))

      assert((tmin.isEmpty && imin.isEmpty) || tmin.map{case (k, _) => ord.equiv(k, imin.get._1)}.get)

      val tmax: Option[Tuple] = if(tdata.isEmpty) None else Some(tdata.max)
      val imax: Option[Tuple] = Await.result(index.max(), Duration.Inf)

      logger.debug(s"\ntmax: ${tmax.map{case (k, v) => new String(k) -> new String(v)}} imax: ${imax.map{case (k, v) => new String(k) -> new String(v)}}")

      assert(tmax.equals(imax))

      assert((tmax.isEmpty && imax.isEmpty) || tmax.map{case (k, _) => ord.equiv(k, imax.get._1)}.get)
    }

    /*logger.debug("\nversions\n")

    for(j<-0 until iter){
      val (d, i) = versions(j)

      logger.debug("\n")

      logger.debug(s"d: ${d.map{case (k, v) => new String(k) -> new String(v)}}")
      logger.debug(s"i: ${i.map{case (k, v) => new String(k) -> new String(v)}}")

      logger.debug("\n")

      assert(i.equals(d))
    }*/

  }

}
