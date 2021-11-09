package services.scalable.index

import com.google.common.base.Charsets
import com.google.protobuf.ByteString
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.Datom
import services.scalable.index.impl._

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import com.google.protobuf.any.Any

class MainSpec extends AnyFlatSpec with Repeatable {

  val logger = LoggerFactory.getLogger(this.getClass)

  override val times: Int = 1000

  val rand = ThreadLocalRandom.current()

  "index data " must "be equal to test data" in {

    implicit def xf(k: Bytes): String = new String(k)

    import DefaultComparators._
    import DefaultIdGenerators._

    type K = Bytes
    type V = Bytes

    val NUM_LEAF_ENTRIES = 5//rand.nextInt(4, 20)
    val NUM_META_ENTRIES = 5//rand.nextInt(4, if(NUM_LEAF_ENTRIES == 4) 5 else NUM_LEAF_ENTRIES)

    val indexId = "test_index"

    implicit val cache = new DefaultCache[K, V](MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new CassandraStorage[Bytes, Bytes](TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, truncate = true)
    implicit val storage = new MemoryStorage[K, V](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    implicit val ctx = new DefaultContext[K, V](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    var data = Seq.empty[Tuple[K, V]]
    val iter = rand.nextInt(1, 100)

    val colors = Seq("red", "green", "blue", "yellow", "orange", "black", "white", "magenta", "gold", "brown", "pink")

    var prefixes = Seq.empty[Bytes]

    for(i<-0 until 100){
      prefixes :+= RandomStringUtils.randomAlphabetic(3, 5).getBytes("UTF-8")
    }

    def insert(index: Index[K, V]): Unit = {

      val n = rand.nextInt(1, 50)

      var list = Seq.empty[Tuple[Bytes, Bytes]]

      for(i<-0 until n){

        val k = prefixes(rand.nextInt(0, prefixes.length)) ++ RandomStringUtils.randomAlphanumeric(5, 10).getBytes()
        val v = RandomStringUtils.randomAlphanumeric(5, 10).getBytes()

        if(!list.exists{case (k1, _) => ord.equiv(k, k1)} && !data.exists{ case (k1, _) => ord.equiv(k1, k)}){
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

    def update(index: Index[Bytes, Bytes]): Unit = {
      if(data.isEmpty) return

      val bound = if(data.length == 1) 1 else rand.nextInt(1, data.length)
      val list = scala.util.Random.shuffle(data.slice(0, bound))
        .map{case (k, _) => k -> RandomStringUtils.randomAlphabetic(5).getBytes("UTF-8")}

      val m = Await.result(index.update(list), Duration.Inf)

      logger.debug(s"update result m: $m")

      val notin = data.filterNot{case (k1, _) => list.exists{case (k, _) => ord.equiv(k, k1)}}
      data = (notin ++ list).sortBy(_._1)
    }

   // implicit val storage = new CassandraStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES, "indexes")

    val index = new QueryableIndex()

    for(i<-0 until iter){
      rand.nextInt(1, 2) match {
        case 1 => insert(index)
        case 2 => update(index)
        case 3 => remove(index)
        case _ =>
      }

      Await.ready(ctx.save(), Duration.Inf)
    }

    var tdata = data.sortBy(_._1)
    var idata = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)

    logger.debug(s"${Console.GREEN_B}tdata: ${tdata.map{case (k, v) => new String(k) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.GREEN_B}idata: ${idata.map{case (k, v) => new String(k) -> new String(v)}}${Console.RESET}\n")

    assert(isColEqual(idata, tdata))

    def filterGt(prefix: Option[K], term: K, data: Seq[(K, V)], inclusive: Boolean)(implicit order: Ordering[K]): Seq[(K, V)] = {
      val word = if(prefix.isDefined) prefix.get ++ term else term

      data.filter{case (k, _) =>
        if(prefix.isEmpty || k.length < prefix.get.length){
          (inclusive && order.gteq(k, word)) || order.gt(k, word)
        } else {
          val p = k.slice(0, prefix.get.length)
          val t = k.slice(prefix.get.length, k.length)

          order.equiv(p, prefix.get) && (inclusive && order.gteq(t, term) || order.gt(t, term))
        }
      }
    }

    def filterLt(prefix: Option[K], term: K, data: Seq[(K, V)], inclusive: Boolean)(implicit order: Ordering[K]): Seq[(K, V)] = {
      val word = if(prefix.isDefined) prefix.get ++ term else term

      data.filter{case (k, _) =>
        if(prefix.isEmpty || k.length < prefix.get.length){
          (inclusive && order.lteq(k, word)) || order.lt(k, word)
        } else {
          val p = k.slice(0, prefix.get.length)
          val t = k.slice(prefix.get.length, k.length)

          order.equiv(p, prefix.get) && (inclusive && order.lteq(t, term) || order.lt(t, term))
        }
      }
    }

    def filterRange(fromPrefix: Option[K], toPrefix: Option[K], from: K, to: K, data: Seq[(K, V)], includeFrom: Boolean, includeTo: Boolean)(implicit order: Ordering[K]): Seq[(K, V)] = {
      val fromWord = if(fromPrefix.isDefined) fromPrefix.get ++ from else from
      val toWord = if(toPrefix.isDefined) toPrefix.get ++ to else to

      data.filter{case (k, _) =>
        ((includeFrom && order.gteq(k, fromWord) || order.gt(k, fromWord)) && (fromPrefix.isEmpty || order.gteq(k.slice(0, fromPrefix.get.length), fromPrefix.get))) &&
          ((includeTo && order.lteq(k, toWord) || order.lt(k, toWord)) && (toPrefix.isEmpty || order.lteq(k.slice(0, toPrefix.get.length), toPrefix.get)))}
    }

    val idx = if(tdata.length > 1) rand.nextInt(0, tdata.length - 1) else 0
    val (fromWord, _) = tdata(idx)
    val (toWord, _) = tdata(rand.nextInt(idx, tdata.length))

    val fromPrefix = fromWord.slice(0, 3)
    val toPrefix = toWord.slice(0, 3)

    val fromTerm = fromWord.slice(fromPrefix.length, fromWord.length)
    val toTerm = toWord.slice(toPrefix.length, toWord.length)

    val includeFrom = rand.nextBoolean()
    val includeTo = rand.nextBoolean()

    val useFromPrefix = rand.nextBoolean()
    val useToPrefix = rand.nextBoolean()

    val reverse = rand.nextBoolean()

    val from_prefix = if(useFromPrefix) Some(fromPrefix) else None
    val from_term = if(useFromPrefix) fromTerm else fromWord

    val to_prefix = if(useToPrefix) Some(toPrefix) else None
    val to_term = if(useToPrefix) toTerm else toWord

    rand.nextInt(1, 4) match {
      case 1 =>

        println("<=")

        tdata = filterLt(from_prefix, from_term, tdata, includeFrom)
        if(reverse) tdata = tdata.reverse

        idata = Await.result(TestHelper.all(index.lt(from_prefix, from_term, includeFrom, reverse)), Duration.Inf)

      case 2 =>

        println(">=")

        /*reverse = rand.nextBoolean()
        useFromPrefix = true
        includeFrom = true*/

        tdata = filterGt(from_prefix, from_term, tdata, includeFrom)
        if(reverse) tdata = tdata.reverse

        idata = Await.result(TestHelper.all(index.gt(from_prefix, from_term, includeFrom, reverse)), Duration.Inf)

      case 3 =>

        println("range")

        tdata = filterRange(from_prefix, to_prefix, from_term, to_term, tdata, includeFrom, includeTo)
        if(reverse) tdata = tdata.reverse

        idata = Await.result(TestHelper.all(index.range(from_prefix, to_prefix, from_term, to_term, includeFrom, includeTo, reverse)), Duration.Inf)
    }

    logger.debug(s"${Console.MAGENTA_B}useFromPrefix: ${useFromPrefix} useToPrefix: ${useToPrefix} reverse: ${reverse}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}tdata: ${tdata.map{case (k, v) => new String(k) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.BLUE_B}idata: ${idata.map{case (k, v) => new String(k) -> new String(v)}}${Console.RESET}\n")

    logger.debug(s"${Console.RED_B}from prefix + term: ${new String(fromPrefix)} + ${new String(fromTerm)} = ${new String(fromWord)} to prefix + term: ${new String(toPrefix)} + ${new String(toTerm)} = ${new String(toWord)} includeFrom: ${includeFrom} includeTo: ${includeTo}${Console.RESET}\n")

    //logger.debug(s"${Console.RED_B}k: ${new String(k)} prefix: ${new String(prefix)} term: ${new String(term)} inclusive: ${includeFrom}")

    assert(isColEqual(idata, tdata))
  }

}
