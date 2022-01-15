package services.scalable.index

import com.google.protobuf.ByteString
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.Datom
import services.scalable.index.impl._

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class ByteSpec extends AnyFlatSpec with Repeatable {

  val logger = LoggerFactory.getLogger(this.getClass)

  override val times: Int = 1000

  val rand = ThreadLocalRandom.current()

  "index data " must "be equal to test data" in {

    implicit def xf(k: Bytes): String = new String(k)

    import DefaultComparators._
    import DefaultIdGenerators._

    val NUM_LEAF_ENTRIES = rand.nextInt(5, 64)
    val NUM_META_ENTRIES = rand.nextInt(5, 64)

    val indexId = "test_index"

    type K = Bytes
    type V = Bytes

    implicit val cache = new DefaultCache[K, V](MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new CassandraStorage[Bytes, Bytes](TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, truncate = true)
    implicit val storage = new MemoryStorage[K, V](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    implicit val ctx = new DefaultContext[K, V](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    var data = Seq.empty[Tuple[K, V]]
    val iter = rand.nextInt(1, 100)

    val prefixes = (0 until 3).map{_ => RandomStringUtils.randomAlphanumeric(3)}

    def insert(index: Index[K, V]): Unit = {

      val n = rand.nextInt(5, 100)

      var list = Seq.empty[Tuple[K, V]]

      for(i<-0 until n){
        val k = (prefixes(rand.nextInt(0, prefixes.length)) ++ RandomStringUtils.randomAlphanumeric(3, 5)).getBytes()
        val v = RandomStringUtils.randomAlphanumeric(1, 5).getBytes()

        if(!list.exists{case (k1, _) => ord.equiv(k, k1)} && !data.exists{ case (k1, _) =>
          ord.equiv(k1, k)}){
          list = list :+ k -> v
        }
      }

      val m = Await.result(index.insert(list), Duration.Inf)

      logger.debug(s"insertion result n: $m")

      data = data ++ list.slice(0, m)

      assert(data.length == index.ctx.num_elements)
      assert(index.ctx.levels == index.getNumLevels())
    }

    def remove(index: Index[K, V]): Unit = {
      if(data.isEmpty) return

      val bound = if(data.length == 1) 1 else rand.nextInt(1, data.length)
      val list = scala.util.Random.shuffle(data.slice(0, bound)).map(_._1)

      val m = Await.result(index.remove(list), Duration.Inf)

      logger.debug(s"removal result m: $m")
      data = data.filterNot{case (k, _) => list.exists{k1 => ord.equiv(k, k1)}}

      assert(data.length == index.ctx.num_elements)
      assert(index.ctx.levels == index.getNumLevels())
    }

    def update(index: Index[K, V]): Unit = {
      if(data.isEmpty) return

      val bound = if(data.length == 1) 1 else rand.nextInt(1, data.length)
      val list = scala.util.Random.shuffle(data.slice(0, bound))
        .map{case (k, _) => k -> RandomStringUtils.randomAlphabetic(5).getBytes("UTF-8")}

      val m = Await.result(index.update(list), Duration.Inf)

      logger.debug(s"update result m: $m")

      val notin = data.filterNot{case (k1, _) => list.exists{case (k, _) => ord.equiv(k, k1)}}
      data = (notin ++ list).sortBy(_._1)

      assert(data.length == index.ctx.num_elements)
      assert(index.ctx.levels == index.getNumLevels())
    }

    def printDatom(d: Datom, p: String): String = {
      p match {
        case "users/:name" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]"
        case "users/:age" => s"[${d.a},${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()},${d.e},${d.t}]"
        case "users/:color" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]"
        case "users/:height" => s"[${d.a},${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()},${d.e},${d.t}]"
        //case "users/:height" => s"[${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()}]"
        case _ => ""
      }
    }

    // implicit val storage = new CassandraStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES, "indexes")

    val index = new QueryableIndex[K, V]()

    for(i<-0 until iter){
      rand.nextInt(1, 2) match {
        case 1 => insert(index)
        case 2 => update(index)
        case 3 => remove(index)
        case _ =>
      }

      Await.ready(ctx.save(), Duration.Inf)
    }

    val tdata = data.sortBy(_._1)
    val idata = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)

    logger.debug(s"tdata: ${tdata.map{case (k, v) => new String(k) -> new String(v)}}\n")
    logger.debug(s"idata: ${idata.map{case (k, v) => new String(k) -> new String(v)}}")

    assert(isColEqual(idata, tdata))

    println()

    if(!tdata.isEmpty){

      var reverse = rand.nextBoolean()
      var withPrefix = rand.nextBoolean()

      var inclusiveFrom = rand.nextBoolean()
      var inclusiveTo = rand.nextBoolean()

      def lt(term: K, k: K, inclusive: Boolean, prefix: Option[K], prefixOrd: Option[Ordering[K]], order: Ordering[K]): Boolean = {
        (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && (inclusive && order.lteq(k, term) || !inclusive && order.lt(k, term))
      }

      def gt(term: K, k: K, inclusive: Boolean, prefix: Option[K], prefixOrd: Option[Ordering[K]], order: Ordering[K]): Boolean = {
        (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && (inclusive && order.gteq(k, term) || !inclusive && order.gt(k, term))
      }

      def range(k: K, fromWord: K, toWord: K, inclusiveFrom: Boolean, inclusiveTo: Boolean, fromPrefix: Option[K], toPrefix: Option[K],
                prefixOrd: Option[Ordering[K]], order: Ordering[K]): Boolean = {
        /*(fromPrefix.isEmpty || ((inclusiveFrom && prefixOrd.get.gteq(k, fromPrefix.get)) || (!inclusiveFrom && prefixOrd.get.gt(k, fromPrefix.get)))) &&
          (toPrefix.isEmpty || ((inclusiveTo && prefixOrd.get.lteq(k, toPrefix.get)) || (!inclusiveTo && prefixOrd.get.lt(k, toPrefix.get)))) &&*/
          (inclusiveFrom && order.gteq(k, fromWord) || !inclusiveFrom && order.gt(k, fromWord)) &&
          (inclusiveTo && order.lteq(k, toWord) || !inclusiveTo && order.lt(k, toWord))
      }

      def find(word: K, k: K, order: Ordering[K]): Boolean = {
        order.equiv(k, word)
      }

      var dlist = Seq.empty[(K, V)]
      var ilist = Seq.empty[(K, V)]

      var op = ""

      val idx = rand.nextInt(0, tdata.length)
      val (fromWord, _) = tdata(idx)
      val fromPrefix = fromWord.slice(0, 3)
      val fromTerm = fromWord.slice(fromPrefix.length, fromWord.length)

      val (toWord, _) = tdata(rand.nextInt(idx, tdata.length))
      val toPrefix = toWord.slice(0, 3)
      val toTerm = toWord.slice(toPrefix.length, toWord.length)

      val prefixOrd = new Ordering[K] {
        override def compare(input: K, prefix: K): Int = {
          if (input.length < prefix.length) {
            return ord.compare(input, prefix)
          }

          ord.compare(input.slice(0, prefix.length), prefix)
        }
      }

      reverse = false
      withPrefix = false
      inclusiveFrom = rand.nextBoolean()
      inclusiveTo = rand.nextBoolean()

      rand.nextInt(1, 5) match {
        case 1 =>

          reverse = rand.nextBoolean()
          withPrefix =  rand.nextBoolean()
          inclusiveFrom = rand.nextBoolean()

          val fp = if(withPrefix) Some(fromPrefix) else None
          val fpo = if(withPrefix) Some(prefixOrd) else None

          val idx = tdata.indexWhere{case (k, _) => gt(fromWord, k, inclusiveFrom, fp, fpo, ord)}
          dlist = tdata.slice(idx, tdata.length).takeWhile{case (k, _) => gt(fromWord, k, inclusiveFrom, fp, fpo, ord)}
          if(reverse) dlist = dlist.reverse

          op = s"${if(inclusiveFrom) ">=" else ">"} ${new String(fromWord)}"

          ilist = Await.result(TestHelper.all(index.gt(fromWord, inclusiveFrom, reverse, fp, fpo, ord)), Duration.Inf)

        case 2 =>

          reverse = rand.nextBoolean()
          withPrefix = rand.nextBoolean()
          inclusiveFrom = rand.nextBoolean()

          val fp = if(withPrefix) Some(fromPrefix) else None
          val fpo = if(withPrefix) Some(prefixOrd) else None

          val idx = tdata.indexWhere{case (k, _) => lt(fromWord, k, inclusiveFrom, fp, fpo, ord)}
          dlist = tdata.slice(idx, tdata.length).takeWhile{case (k, _) => lt(fromWord, k, inclusiveFrom, fp, fpo, ord)}
          if(reverse) dlist = dlist.reverse

          op = s"${if(inclusiveFrom) "<=" else "<"} ${new String(fromWord)}"

          ilist = Await.result(TestHelper.all(index.lt(fromWord, inclusiveFrom, reverse, fp, fpo, ord)), Duration.Inf)

        case 3 =>

          reverse = rand.nextBoolean()
          withPrefix = rand.nextBoolean()
          inclusiveFrom = rand.nextBoolean()
          inclusiveTo = rand.nextBoolean()

          val fp = if(withPrefix) Some(fromPrefix) else None
          val tp = if(withPrefix) Some(toPrefix) else None
          val po = if(withPrefix) Some(prefixOrd) else None

          dlist = tdata.filter{case (k, _) => range(k, fromWord, toWord, inclusiveFrom, inclusiveTo, fp, tp, po, ord)}
          if(reverse) dlist = dlist.reverse

          op = s"range: ${new String(fromWord)} ${if(inclusiveFrom) "<=" else "<"} x ${if(inclusiveTo) "<=" else "<"} ${new String(toWord)}"

          ilist = Await.result(TestHelper.all(index.range(fromWord, toWord, inclusiveFrom, inclusiveTo, reverse)(ord)), Duration.Inf)

        case 4 =>

          reverse = rand.nextBoolean()
          withPrefix = rand.nextBoolean()
          inclusiveFrom = rand.nextBoolean()

          dlist = tdata.filter{case (k, _) => find(k, fromWord, ord)}
          if(reverse) dlist = dlist.reverse

          op = s"find: ${new String(fromWord)}"

          ilist = Await.result(TestHelper.all(index.find(fromWord, reverse, ord)), Duration.Inf)

        case _ =>
      }

      logger.debug(s"${Console.BLUE_B}withPrefix: ${withPrefix} inclusiveLower: ${inclusiveFrom} inclusiveUpper: ${inclusiveTo} reverse: ${reverse}${Console.RESET}\n")
      logger.debug(s"${Console.MAGENTA_B}${op} dlist: ${dlist.map{case (k, v) => new String(k) /*-> new String(v)*/}}${Console.RESET}\n")
      logger.debug(s"${Console.BLUE_B}${op} ilist: ${ilist.map{case (k, v) => new String(k) /*-> new String(v)*/}}\n${Console.RESET}")
      logger.debug(s"${Console.GREEN_B}length: ${ilist.length == dlist.length}${Console.RESET}")

      assert(isColEqual(ilist, dlist))

    }
  }

}