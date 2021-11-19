package services.scalable.index

import com.google.common.base.Charsets
import com.google.protobuf.ByteString
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.Datom
import services.scalable.index.impl._

import java.util.{Comparator, UUID}
import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import com.google.protobuf.any.Any

import java.util

class MainSpec extends AnyFlatSpec with Repeatable {

  val logger = LoggerFactory.getLogger(this.getClass)

  override val times: Int = 1000

  val rand = ThreadLocalRandom.current()

  "index data " must "be equal to test data" in {

    implicit def xf(k: Bytes): String = new String(k)

    import DefaultComparators._
    import DefaultIdGenerators._

    implicit val avetOrd = new Ordering[Datom] {
      override def compare(x: Datom, y: Datom): Int = {
        var r = x.getA.compareTo(y.getA)

        if(r != 0) return r

        r = ord.compare(x.getV.toByteArray, y.getV.toByteArray)

        if(r != 0) return r

        r = x.getE.compareTo(y.getE)

        if(r != 0) return r

        r = x.getT.compareTo(y.getT)

        if(r != 0) return r

        x.getOp.compareTo(y.getOp)
      }
    }

    val NUM_LEAF_ENTRIES = 5
    val NUM_META_ENTRIES = 5

    val indexId = "test_index"

    implicit val cache = new DefaultCache[Datom, Bytes](MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new CassandraStorage[Bytes, Bytes](TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, truncate = true)
    implicit val storage = new MemoryStorage[Datom, Bytes](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    implicit val ctx = new DefaultContext[Datom, Bytes](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    var data = Seq.empty[Tuple[Datom, Bytes]]
    val iter = rand.nextInt(1, 30)

    val colors = Seq("red", "green", "blue", "yellow", "orange", "black", "white", "magenta", "gold", "brown", "pink", "cyan", "purple")

    var nOfH = 0

    def insert(index: Index[Datom, Bytes]): Unit = {

      val n = rand.nextInt(10, 50)

      var list = Seq.empty[Tuple[Datom, Bytes]]

      for(i<-0 until n){
        val now = System.currentTimeMillis()
        val id = UUID.randomUUID.toString.getBytes()
        val name = RandomStringUtils.randomAlphanumeric(5, 10).getBytes()
        val age = java.nio.ByteBuffer.allocate(4).putInt(rand.nextInt(18, 100)).flip().array()
        val color = colors(rand.nextInt(0, colors.length)).getBytes()

        val h = if(nOfH < 30) 150 else rand.nextInt(150, 210)

        val height = java.nio.ByteBuffer.allocate(4).putInt(h).flip().array()

        if(h == 150){
          nOfH += 1
        }

        //AVET
        val datoms = Seq(
          Datom(Some("users/:name"), Some(ByteString.copyFrom(name)), Some(id), Some(now), Some(true)),
          Datom(Some("users/:age"), Some(ByteString.copyFrom(age)), Some(id), Some(now), Some(true)),
          Datom(Some("users/:color"), Some(ByteString.copyFrom(color)), Some(id), Some(now), Some(true)),
          Datom(Some("users/:height"), Some(ByteString.copyFrom(height)), Some(id), Some(now), Some(true))
        )

        /*val pairs = datoms.map { d =>
          Any.pack(d).toByteArray
        }*/

        for(d <- datoms){
          list = list :+ d -> Array.empty[Byte]
        }

        /*if(!list.exists{case (k1, _) => ord.equiv(k, k1)} && !data.exists{ case (k1, _) =>
          ord.equiv(k1, k)}){
          list = list :+ k -> v
        }*/
      }

      val m = Await.result(index.insert(list), Duration.Inf)

      logger.debug(s"insertion result n: $m")

      data = data ++ list.slice(0, m)
    }

    def remove(index: Index[Datom, Bytes]): Unit = {
      if(data.isEmpty) return

      val bound = if(data.length == 1) 1 else rand.nextInt(1, data.length)
      val list = scala.util.Random.shuffle(data.slice(0, bound)).map(_._1)

      val m = Await.result(index.remove(list), Duration.Inf)

      logger.debug(s"removal result m: $m")
      data = data.filterNot{case (k, _) => list.exists{k1 => avetOrd.equiv(k, k1)}}
    }

    def update(index: Index[Datom, Bytes]): Unit = {
      if(data.isEmpty) return

      val bound = if(data.length == 1) 1 else rand.nextInt(1, data.length)
      val list = scala.util.Random.shuffle(data.slice(0, bound))
        .map{case (k, _) => k -> RandomStringUtils.randomAlphabetic(5).getBytes("UTF-8")}

      val m = Await.result(index.update(list), Duration.Inf)

      logger.debug(s"update result m: $m")

      val notin = data.filterNot{case (k1, _) => list.exists{case (k, _) => avetOrd.equiv(k, k1)}}
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

    val index = new QueryableIndex[Datom, Bytes]()

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

    logger.debug(s"tdata: ${tdata.map{case (k, v) => printDatom(k, k.getA) -> new String(v)}}\n")
    logger.debug(s"idata: ${idata.map{case (k, v) => printDatom(k, k.getA) -> new String(v)}}")

    assert(isColEqual(idata, tdata))

    println()

    var reverse = false//rand.nextBoolean()

    var inclusiveFrom = rand.nextBoolean()
    var withPrefix = rand.nextBoolean()
    var inclusiveTo = rand.nextBoolean()

    val properties = Seq("users/:age", "users/:color", "users/:height")

    val from_prefix = properties(rand.nextInt(0, properties.length))
    val to_prefix = properties(rand.nextInt(0, properties.length))

    val fromPrefix = Datom(a = Some(from_prefix))
    val toPrefix = Datom(a = Some(to_prefix))

    var fromWord: Datom = null
    var toWord: Datom = null

    def generateRandom(prefix: String): (Datom, Datom, Datom) = {
      prefix match {
        case "users/:age" =>
          val lv = rand.nextInt(18, 100)
          (Datom(a = Some(prefix), v = Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(lv).flip().array()))),
            Datom(a = Some(prefix), v = Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(lv).flip().array())),
            e = Some("ffffffffffffffffffffffffffffffff"), t = Some(Long.MaxValue), op = Some(true)),
            Datom(a = Some(prefix), v = Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(lv).flip().array())),
              e = Some("00000000000000000000000000000000"), t = Some(Long.MinValue), op = Some(true)))

        case "users/:color" =>

          val lv = colors(rand.nextInt(0, colors.length))
          (Datom(Some(prefix), Some(ByteString.copyFrom(lv.getBytes()))),
            Datom(Some(prefix), Some(ByteString.copyFrom(lv.getBytes())), e = Some("ffffffffffffffffffffffffffffffff"), t = Some(Long.MaxValue), op = Some(true)),
            Datom(Some(prefix), Some(ByteString.copyFrom(lv.getBytes())), e = Some("00000000000000000000000000000000"), t = Some(Long.MinValue), op = Some(true)),
          )

        case "users/:height" =>

          val lv = rand.nextInt(150, 210)
          (Datom(Some(prefix), Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(lv).flip().array()))),
            Datom(Some(prefix), Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(lv).flip().array())), e = Some("ffffffffffffffffffffffffffffffff"), t = Some(Long.MaxValue), op = Some(true)),
            Datom(Some(prefix), Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(lv).flip().array())), e = Some("00000000000000000000000000000000"), t = Some(Long.MinValue), op = Some(true))
          )
      }
    }

    val x = generateRandom(from_prefix)
    val y = generateRandom(to_prefix)

    fromWord = x._1
    toWord = y._1

    val prefixOrd = new Ordering[Datom] {
      override def compare(k: Datom, prefix: Datom): Int = {
        ord.compare(k.getA.getBytes(), prefix.getA.getBytes())
      }
    }

    var termOrd = new Ordering[Datom] {
      override def compare(x: Datom, y: Datom): Int = {
        var r = x.getA.compareTo(y.getA)

        if(r != 0) return r

        ord.compare(x.getV.toByteArray, y.getV.toByteArray)
      }
    }

    var dlist = Seq.empty[(Datom, Bytes)]
    var ilist = Seq.empty[(Datom, Bytes)]

    var op = ""

    def lt(word: Datom, k: Datom, inclusive: Boolean, prefix: Option[Datom], prefixOrd: Option[Ordering[Datom]], order: Ordering[Datom]): Boolean = {
      (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && (inclusive && order.lteq(k, word) || !inclusive && order.lt(k, word))
    }

    def gt(word: Datom, k: Datom, inclusive: Boolean, prefix: Option[Datom], prefixOrd: Option[Ordering[Datom]], order: Ordering[Datom]): Boolean = {
      (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && (inclusive && order.gteq(k, word) || !inclusive && order.gt(k, word))
    }

    reverse = rand.nextBoolean()
    withPrefix = rand.nextBoolean()
    inclusiveFrom = rand.nextBoolean()
    inclusiveTo = rand.nextBoolean()

    fromWord = x._1
    toWord = y._1

    /*termOrd = avetOrd

    if(!inclusiveFrom){
      fromWord = x._2
    }*/

    /*if(!inclusiveTo){
      toWord = y._3
    }*/

    rand.nextInt(1, 2) match {
      case 1 =>

        reverse = rand.nextBoolean()
        withPrefix =  false//rand.nextBoolean()
        inclusiveFrom = rand.nextBoolean()

        val fp = if(withPrefix) Some(fromPrefix) else None
        val fpo = if(withPrefix) Some(prefixOrd) else None

        val idx = tdata.indexWhere{case (k, _) => gt(fromWord, k, inclusiveFrom, fp, fpo, termOrd)}
        dlist = tdata.slice(idx, tdata.length).takeWhile{case (k, _) => gt(fromWord, k, inclusiveFrom, fp, fpo, termOrd)}
        if(reverse) dlist = dlist.reverse

        op = s"${if(inclusiveFrom) ">=" else ">"} ${printDatom(fromWord, fromWord.getA)}"

        ilist = Await.result(TestHelper.all(index.gt(fromWord, inclusiveFrom, reverse, fp, fpo, termOrd)), Duration.Inf)

      case 2 =>

        /*reverse = false//rand.nextBoolean()
        withPrefix = rand.nextBoolean()
        inclusiveFrom = rand.nextBoolean()

        val fp = if(withPrefix) Some(fromPrefix) else None
        val fpo = if(withPrefix) Some(prefixOrd) else None

        val idx = tdata.indexWhere{case (k, _) => lt(fromWord, k, inclusiveFrom, fp, fpo, termOrd)}
        dlist = tdata.slice(idx, tdata.length).takeWhile{case (k, _) => lt(fromWord, k, inclusiveFrom, fp, fpo, termOrd)}
        if(reverse) dlist = dlist.reverse

        op = s"${if(inclusiveFrom) "<=" else "<"} ${printDatom(fromWord, fromWord.getA)}"

        ilist = Await.result(TestHelper.all(index.lt(fromWord, inclusiveFrom, reverse, fp, fpo, termOrd)), Duration.Inf)*/

      case 3 =>

        /*def cond: Datom => Boolean = k => ((inclusiveFrom && termOrd.gteq(k, fromWord)) || (!inclusiveFrom && termOrd.gt(k, fromWord))) &&
          ((inclusiveTo && termOrd.lteq(k, toWord)) || (!inclusiveTo && termOrd.lt(k, toWord)))

        val idx = tdata.indexWhere{case (k, _) => /*termOrd.gt(k, fromWord)*/cond(k)}
        dlist = tdata.slice(idx, tdata.length).takeWhile{case (k, _) => cond(k)}
        if(reverse) dlist = dlist.reverse

        op = s"range: ${printDatom(fromWord, fromWord.getA)} ${if(inclusiveFrom) "<=" else "<"} x ${if(inclusiveTo) "<=" else "<"} ${printDatom(toWord, toWord.getA)}"

        ilist = Await.result(TestHelper.all(index.range(fromWord, toWord, inclusiveFrom, inclusiveTo, reverse, None, None, None, termOrd)), Duration.Inf)
*/
      case _ =>
    }

    logger.debug(s"${Console.BLUE_B}withPrefix: ${withPrefix} inclusiveLower: ${inclusiveFrom} inclusiveUpper: ${inclusiveTo} reverse: ${reverse}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}dlist ${op}: ${dlist.map{case (k, v) => printDatom(k, k.getA) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.BLUE_B}ilist ${op}: ${ilist.map{case (k, v) => printDatom(k, k.getA) -> new String(v)}}\n${Console.RESET}")
    logger.debug(s"${Console.GREEN_B}length: ${ilist.length == dlist.length}${Console.RESET}")

    assert(isColEqual(dlist, ilist)(termOrd, ord))

  }

}