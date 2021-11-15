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

    val NUM_LEAF_ENTRIES = 5//rand.nextInt(4, 20)
    val NUM_META_ENTRIES = 5//rand.nextInt(4, if(NUM_LEAF_ENTRIES == 4) 5 else NUM_LEAF_ENTRIES)

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

      val n = rand.nextInt(1, 50)

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
    }

    def remove(index: Index[K, V]): Unit = {
      if(data.isEmpty) return

      val bound = if(data.length == 1) 1 else rand.nextInt(1, data.length)
      val list = scala.util.Random.shuffle(data.slice(0, bound)).map(_._1)

      val m = Await.result(index.remove(list), Duration.Inf)

      logger.debug(s"removal result m: $m")
      data = data.filterNot{case (k, _) => list.exists{k1 => ord.equiv(k, k1)}}
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

    var reverse = false//rand.nextBoolean()
    var withPrefix = rand.nextBoolean()

    var inclusiveLower = rand.nextBoolean()
    var inclusiveUpper = rand.nextBoolean()

    def lt(term: K, k: K, inclusive: Boolean, prefix: Option[K], prefixOrd: Option[Ordering[K]], order: Ordering[K]): Boolean = {
      (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && (inclusive && order.lteq(k, term) || !inclusive && order.lt(k, term))
    }

    def gt(term: K, k: K, inclusive: Boolean, prefix: Option[K], prefixOrd: Option[Ordering[K]], order: Ordering[K]): Boolean = {
      (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && (inclusive && order.gteq(k, term) || !inclusive && order.gt(k, term))
    }

    def range(fromTerm: K, toTerm: K, k: K, inclusiveFrom: Boolean, inclusiveTo: Boolean, fromPrefix: Option[K], toPrefix: Option[K], prefixOrd: Option[Ordering[K]], order: Ordering[K]): Boolean = {
      ((fromPrefix.isEmpty || prefixOrd.get.equiv(k, fromPrefix.get)) && (inclusiveFrom && order.gteq(k, fromTerm) || !inclusiveFrom && order.gt(k, fromTerm))) &&
        ((toPrefix.isEmpty || prefixOrd.get.equiv(k, toPrefix.get)) && (inclusiveTo && order.lteq(k, toTerm) || !inclusiveTo && order.lt(k, toTerm)))
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

    rand.nextInt(1, 4) match {
      case 1 =>

        withPrefix = false

        if(withPrefix){
          dlist = tdata.filter{case (k, _) => gt(fromWord, k, inclusiveLower, Some(fromPrefix), Some(prefixOrd), ord)}
          if(reverse) dlist = dlist.reverse

          op = s"${if(inclusiveLower) ">=" else ">"} prefix: ${new String(fromPrefix)} term: ${new String(fromTerm)} word: ${fromWord}"

          ilist = Await.result(TestHelper.all(index.gt(fromWord, inclusiveLower, reverse, Some(fromPrefix), Some(prefixOrd))), Duration.Inf)
        } else {

          dlist = tdata.filter{case (k, _) => gt(fromWord, k, inclusiveLower, None, None, ord)}
          if(reverse) dlist = dlist.reverse

          op = s"${if(inclusiveLower) ">=" else ">"} word: ${new String(fromWord)}"

          ilist = Await.result(TestHelper.all(index.gt(fromWord, inclusiveLower, reverse, None, None)), Duration.Inf)
        }

      case 2 =>

        if(withPrefix){
          dlist = tdata.filter{case (k, _) => lt(fromWord, k, inclusiveLower, Some(fromPrefix), Some(prefixOrd), ord)}
          if(reverse) dlist = dlist.reverse

          op = s"${if(inclusiveLower) "<=" else "<"} prefix: ${new String(fromPrefix)} term: ${new String(fromTerm)}"

          ilist = Await.result(TestHelper.all(index.lt(fromWord, inclusiveLower, reverse, Some(fromPrefix), Some(prefixOrd))), Duration.Inf)
        } else {

          dlist = tdata.filter{case (k, _) => lt(fromWord, k, inclusiveLower, None, None, ord)}
          if(reverse) dlist = dlist.reverse

          op = s"${if(inclusiveLower) "<=" else "<"} word: ${new String(fromWord)}"

          ilist = Await.result(TestHelper.all(index.lt(fromWord, inclusiveLower, reverse, None, None)), Duration.Inf)
        }

      case 3 =>

        if(withPrefix){

          dlist = tdata.filter{case (k, _) => range(fromWord, toWord, k, inclusiveLower, inclusiveUpper, Some(fromPrefix), Some(toPrefix), Some(prefixOrd), ord)}
          if(reverse) dlist = dlist.reverse

          op = s"range: fromPrefix: ${new String(fromPrefix)}-${new String(fromTerm)} ${if(inclusiveLower) "<=" else "<"} x ${if(inclusiveUpper) "<=" else "<"} ${new String(fromPrefix)}-${new String(toTerm)}"

          ilist = Await.result(TestHelper.all(index.range(fromWord, toWord, inclusiveLower, inclusiveUpper, reverse, Some(fromPrefix), Some(toPrefix), Some(prefixOrd))), Duration.Inf)

        } else {
          dlist = tdata.filter{case (k, _) => range(fromWord, toWord, k, inclusiveLower, inclusiveUpper, None, None, None, ord)}
          if(reverse) dlist = dlist.reverse

          op = s"range: ${new String(fromWord)} ${if(inclusiveLower) "<=" else "<"} x ${if(inclusiveUpper) "<=" else "<"} ${new String(toWord)}"

          ilist = Await.result(TestHelper.all(index.range(fromWord, toWord, inclusiveLower, inclusiveUpper, reverse, None, None, None)), Duration.Inf)
        }

      case _ =>

    }

    logger.debug(s"${Console.BLUE_B}withPrefix: ${withPrefix} inclusiveLower: ${inclusiveLower} inclusiveUpper: ${inclusiveUpper} reverse: ${reverse}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}${op} dlist: ${dlist.map{case (k, v) => new String(k) /*-> new String(v)*/}}${Console.RESET}\n")
    logger.debug(s"${Console.BLUE_B}${op} ilist: ${ilist.map{case (k, v) => new String(k) /*-> new String(v)*/}}\n${Console.RESET}")
    logger.debug(s"${Console.GREEN_B}length: ${ilist.length == dlist.length}${Console.RESET}")

    assert(isColEqual(ilist, dlist))

    //assert(!dlist.isEmpty)

  }

}