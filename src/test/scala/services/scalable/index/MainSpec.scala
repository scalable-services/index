package services.scalable.index

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.impl.{CassandraStorage, DefaultCache, DefaultContext, GrpcByteSerializer, MemoryStorage}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class MainSpec extends Repeatable {

  override val times: Int = 1000

  "operations" should " run successfully" in {

    type T = services.scalable.index.QueryableIndex[K, V]
    val logger = LoggerFactory.getLogger(this.getClass)
    
    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Bytes
    type V = Bytes

    import services.scalable.index.DefaultComparators._

    val NUM_LEAF_ENTRIES = rand.nextInt(5, 64)
    val NUM_META_ENTRIES = rand.nextInt(5, 64)

    val indexId = UUID.randomUUID().toString

    implicit val cache = new DefaultCache[K, V](MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new MemoryStorage[K, V](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    implicit val ctx = new DefaultContext[K, V](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    val index = new QueryableIndex[K, V]()

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
    val idata = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)

    logger.debug(s"${Console.GREEN_B}tdata: ${tdata.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata: ${idata.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

    isColEqual(idata, tdata)
    assert(Await.result(index.getNumLevels(), Duration.Inf) == ctx.levels)
    assert(ctx.num_elements == idata.length)

    println()

    def lt(k: K, fromPrefix: Option[K], fromWord: K, inclusiveFrom: Boolean, prefixOrder: Ordering[K]): Boolean = {
      (fromPrefix.isEmpty || prefixOrder.equiv(k, fromPrefix.get)) && (inclusiveFrom && ord.lteq(k, fromWord) || !inclusiveFrom && ord.lt(k, fromWord))
    }

    def gt(k: K, fromPrefix: Option[K], fromWord: K, inclusiveFrom: Boolean, prefixOrder: Ordering[K]): Boolean = {
      (fromPrefix.isEmpty || prefixOrder.equiv(k, fromPrefix.get)) && (inclusiveFrom && ord.gteq(k, fromWord) || !inclusiveFrom && ord.gt(k, fromWord))
    }

    def range(k: K, from: K, to: K, inclusiveFrom: Boolean, inclusiveTo: Boolean): Boolean = {
      (inclusiveFrom && ord.gteq(k, from) || ord.gt(k, from)) &&
        (inclusiveTo && ord.lteq(k, to) || ord.lt(k, to))
    }

    def find(k: K, word: K, order: Ordering[K]): Boolean = {
      order.equiv(k, word)
    }

    if(data.length > 2){

      val reverse = rand.nextBoolean()
      val inclusiveFrom = rand.nextBoolean()
      val inclusiveTo = rand.nextBoolean()

      var dlist = Seq.empty[Tuple2[K, V]]
      var it: RichAsyncIterator[K, V] = null

      val idx0 = rand.nextInt(0, data.length)
      val idx1 = rand.nextInt(idx0, data.length)

      val fromTerm = data(idx0)._1
      val toTerm = data(idx1)._1

      val fromPrefix = if(rand.nextBoolean()) Some(fromTerm.slice(0, 4)) else None
      var op = ""

      val prefixOrd = new Ordering[K] {
        override def compare(x: K, fromPrefix: K): Int = {
          if(x.length < fromPrefix.length) return ord.compare(x, fromPrefix)

          val pre = x.slice(0, fromPrefix.length)

          ord.compare(pre, fromPrefix)
        }
      }

     // reverse = true

      rand.nextInt(1, 5) match {
        case 1 =>

          op = "<"

          dlist = tdata.filter{ case (k, _) => lt(k, fromPrefix, fromTerm, inclusiveFrom, prefixOrd)}
          dlist = if(reverse) dlist.reverse else dlist
          it = index.lt(fromPrefix, fromTerm, inclusiveFrom, reverse)(Some(prefixOrd), ord)

        case 2 =>

          op = ">"

          dlist = tdata.filter{case (k, _) => gt(k, fromPrefix, fromTerm, inclusiveFrom, prefixOrd)}
          dlist = if(reverse) dlist.reverse else dlist
          it = index.gt(fromPrefix, fromTerm, inclusiveFrom, reverse)(Some(prefixOrd), ord)

        case 3 =>

          op = "range"

          dlist = tdata.filter{case (k, _) => range(k, fromTerm, toTerm, inclusiveFrom, inclusiveTo)}
          dlist = if(reverse) dlist.reverse else dlist
          it = index.range(fromTerm, toTerm, inclusiveFrom, inclusiveTo, reverse)(prefixOrd)

        case 4 =>

          op = "find"

          val fromPrefix = prefixes(rand.nextInt(0, prefixes.length))

          dlist = tdata.filter{case (k, _) => find(k, fromPrefix, prefixOrd)}
          dlist = if(reverse) dlist.reverse else dlist
          it = index.find(fromPrefix, reverse)(prefixOrd)

        case _ =>
      }

      val ilist = Await.result(TestHelper.all(it), Duration.Inf)

      if(op.compareTo("range") == 0) {
        logger.debug(s"${Console.CYAN_B} op: ${op} fromPrefix: ${fromPrefix.map(s => new String(s, Charsets.UTF_8))} from: ${new String(fromTerm)} to: ${new String(toTerm)} reverse: ${reverse} includeFrom: ${inclusiveFrom} includeTo: ${inclusiveTo}${Console.RESET}\n")
      } else {
        logger.debug(s"${Console.CYAN_B} op: ${op} fromPrefix: ${fromPrefix.map(s => new String(s, Charsets.UTF_8))} from: ${new String(fromTerm)} includeFrom: ${inclusiveFrom}${Console.RESET}\n")
      }

      logger.debug(s"${Console.GREEN_B}tdata: ${dlist.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
      logger.debug(s"${Console.MAGENTA_B}idata: ${ilist.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

      isColEqual(dlist, ilist)

    }

  }

}
