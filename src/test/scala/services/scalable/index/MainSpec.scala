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
    val iter = rand.nextInt(1, 100)

    val colors = Seq("red", "green", "blue", "yellow", "orange", "black", "white", "magenta", "gold", "brown", "pink", "cyan", "purple")

    var nOfH = 0

    def insert(index: Index[Datom, Bytes]): Unit = {

      val n = rand.nextInt(1, 50)

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

    var inclusiveLower = rand.nextBoolean()
    var withPrefix = rand.nextBoolean()
    var inclusiveUpper = rand.nextBoolean()

    val properties = Seq("users/:age", "users/:color", "users/:height")

    val from_prefix = properties(rand.nextInt(0, properties.length))
    val to_prefix = from_prefix//properties(rand.nextInt(0, properties.length))

    val fromPrefix = Datom(a = Some(from_prefix))
    val toPrefix = Datom(a = Some(from_prefix))

    var fromWord: Datom = null
    var toWord: Datom = null

    from_prefix match {
      case "users/:age" =>

        val lv = rand.nextInt(18, 100)
        val uv = rand.nextInt(lv, 100)

        fromWord = Datom(a = Some(from_prefix), v = Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(lv).flip().array())))
        toWord = Datom(a = Some(to_prefix), v = Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(uv).flip().array())))

      case "users/:color" =>

        val lv = colors(rand.nextInt(0, colors.length))
        val filtered = colors.filter(_.compareTo(lv) >= 0)
        val uv = if(filtered.length > 1) filtered(rand.nextInt(0, filtered.length)) else lv

        fromWord = Datom(Some(from_prefix), Some(ByteString.copyFrom(lv.getBytes())))
        toWord = Datom(Some(to_prefix), Some(ByteString.copyFrom(uv.getBytes())))

      case "users/:height" =>

        val lv = rand.nextInt(150, 210)
        val uv = rand.nextInt(lv, 210)

        fromWord = Datom(Some(from_prefix), Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(lv).flip().array())))
        toWord = Datom(Some(to_prefix), Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(uv).flip().array())))
    }

    val prefixOrd = new Ordering[Datom] {
      override def compare(pre: Datom, prefix: Datom): Int = {
        pre.getA.compareTo(prefix.getA)
      }
    }

    val termOrd = new Ordering[Datom] {
      override def compare(x: Datom, y: Datom): Int = {
        val r = prefixOrd.compare(x, y)

        if(r != 0) return r

        ord.compare(x.getV.toByteArray, y.getV.toByteArray)
      }
    }

    var dlist = Seq.empty[(Datom, Bytes)]
    var ilist = Seq.empty[(Datom, Bytes)]

    var op = ""
    var opCode = 0

    def lt(term: Datom, k: Datom, inclusive: Boolean, prefix: Option[Datom], prefixOrd: Option[Ordering[Datom]], order: Ordering[Datom]): Boolean = {
      (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && (inclusive && order.lteq(k, term) || !inclusive && order.lt(k, term))
    }

    def gt(term: Datom, k: Datom, inclusive: Boolean, prefix: Option[Datom], prefixOrd: Option[Ordering[Datom]], order: Ordering[Datom]): Boolean = {
      (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && (inclusive && order.gteq(k, term) || !inclusive && order.gt(k, term))
    }

    def range(fromTerm: Datom, toTerm: Datom, k: Datom, inclusiveFrom: Boolean, inclusiveTo: Boolean, fromPrefix: Option[Datom], toPrefix: Option[Datom], prefixOrd: Option[Ordering[Datom]], order: Ordering[Datom]): Boolean = {
      ((fromPrefix.isEmpty || prefixOrd.get.equiv(k, fromPrefix.get)) && (inclusiveFrom && order.gteq(k, fromTerm) || !inclusiveFrom && order.gt(k, fromTerm))) &&
        ((toPrefix.isEmpty || prefixOrd.get.equiv(k, toPrefix.get)) && (inclusiveTo && order.lteq(k, toTerm) || !inclusiveTo && order.lt(k, toTerm)))
    }

    //withPrefix = true
    //reverse = false

    rand.nextInt(1, 4) match {
      case 1 =>

        if(withPrefix){

          dlist = tdata.filter{case (k, _) => gt(fromWord, k, inclusiveLower, Some(fromPrefix), Some(prefixOrd), avetOrd)}
          if(reverse) dlist = dlist.reverse

          op = s"${if(inclusiveLower) ">=" else ">"} word: ${printDatom(fromWord, fromWord.getA)}"

          ilist = Await.result(TestHelper.all(index.gt(fromPrefix, fromWord, inclusiveLower, reverse)(prefixOrd, avetOrd)), Duration.Inf)

        } else {

          dlist = tdata.filter{case (k, _) => gt(fromWord, k, inclusiveLower, None, None, avetOrd)}
          if(reverse) dlist = dlist.reverse

          op = s"${if(inclusiveLower) ">=" else ">"} word: ${printDatom(fromWord, fromWord.getA)}"

          ilist = Await.result(TestHelper.all(index.gt(fromWord, inclusiveLower, reverse)(avetOrd)), Duration.Inf)

        }

      case 2 =>

        if(withPrefix){

          dlist = tdata.filter{case (k, _) => lt(fromWord, k, inclusiveLower, Some(fromPrefix), Some(prefixOrd), avetOrd)}
          if(reverse) dlist = dlist.reverse

          op = s"${if(inclusiveLower) "<=" else "<"} word: ${printDatom(fromWord, fromWord.getA)}"

          ilist = Await.result(TestHelper.all(index.lt(fromPrefix, fromWord, inclusiveLower, reverse)(prefixOrd, avetOrd)), Duration.Inf)

        } else {

          dlist = tdata.filter{case (k, _) => lt(fromWord, k, inclusiveLower, None, None, avetOrd)}
          if(reverse) dlist = dlist.reverse

          op = s"${if(inclusiveLower) "<=" else "<"} word: ${printDatom(fromWord, fromWord.getA)}"

          ilist = Await.result(TestHelper.all(index.lt(fromWord, inclusiveLower, reverse)(avetOrd)), Duration.Inf)

        }

      case 3 =>

        if(withPrefix){

          dlist = tdata.filter{case (k, _) => range(fromWord, toWord, k, inclusiveLower, inclusiveUpper, Some(fromPrefix), Some(toPrefix), Some(prefixOrd), avetOrd)}
          if(reverse) dlist = dlist.reverse

          op = s"range: fromPrefix: ${from_prefix}-${printDatom(fromWord, fromWord.getA)} ${if(inclusiveLower) "<=" else "<"} x ${if(inclusiveUpper) "<=" else "<"} ${to_prefix}-${printDatom(toWord, toWord.getA)}"

          ilist = Await.result(TestHelper.all(index.range(fromWord, toWord, inclusiveLower, inclusiveUpper, reverse, Some(fromPrefix), Some(toPrefix), Some(prefixOrd), avetOrd)), Duration.Inf)

        } else {
          dlist = tdata.filter{case (k, _) => range(fromWord, toWord, k, inclusiveLower, inclusiveUpper, None, None, None, avetOrd)}
          if(reverse) dlist = dlist.reverse

          op = s"range: ${printDatom(fromWord, fromWord.getA)} ${if(inclusiveLower) "<=" else "<"} x ${if(inclusiveUpper) "<=" else "<"} ${printDatom(toWord, toWord.getA)}"

          ilist = Await.result(TestHelper.all(index.range(fromWord, toWord, inclusiveLower, inclusiveUpper, reverse, None, None, None, avetOrd)), Duration.Inf)
        }

      case _ =>
    }

    logger.debug(s"${Console.BLUE_B}withPrefix: ${withPrefix} inclusiveLower: ${inclusiveLower} inclusiveUpper: ${inclusiveUpper} reverse: ${reverse}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}dlist ${op}: ${dlist.map{case (k, v) => printDatom(k, k.getA) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.BLUE_B}ilist ${op}: ${ilist.map{case (k, v) => printDatom(k, k.getA) -> new String(v)}}\n${Console.RESET}")
    logger.debug(s"${Console.GREEN_B}length: ${ilist.length == dlist.length}${Console.RESET}")

    assert(isColEqual(ilist, dlist)(avetOrd, ord))

  }

}