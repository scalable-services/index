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

class MainSpec2 extends AnyFlatSpec with Repeatable {

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

    val NUM_LEAF_ENTRIES = 5//rand.nextInt(4, 20)
    val NUM_META_ENTRIES = 5//rand.nextInt(4, if(NUM_LEAF_ENTRIES == 4) 5 else NUM_LEAF_ENTRIES)

    val indexId = "test_index"

    implicit val cache = new DefaultCache[Datom, Bytes](MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new CassandraStorage[Bytes, Bytes](TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, truncate = true)
    implicit val storage = new MemoryStorage[Datom, Bytes](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    implicit val ctx = new DefaultContext[Datom, Bytes](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    var data = Seq.empty[Tuple[Datom, Bytes]]
    val iter = rand.nextInt(1, 100)

    val colors = Seq("red", "green", "blue", "yellow", "orange", "black", "white", "magenta", "gold", "brown", "pink")

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

    val index = new QueryableIndex2[Datom, Bytes]()

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

    val inclusiveLower = rand.nextBoolean()
    val withLowerPrefix = rand.nextBoolean()
    val withUpperPrefix = rand.nextBoolean()

    val inclusiveUpper = rand.nextBoolean()

    /*val lh = rand.nextInt(150, 210)
    val lowerTerm = Datom(Some("users/:height"), Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(lh).flip().array())))
    val lowerPrefix = Datom(a = Some("users/:height"))

    val hh = if(lh == 210) 210 else rand.nextInt(lh, 210)
    val upperTerm = Datom(Some("users/:height"), Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(hh).flip().array())))
    val upperPrefix = Datom(a = Some("users/:height"))*/

    val properties = Seq("users/:age", "users/:color", "users/:height")

    val lower_prefix = properties(rand.nextInt(0, properties.length))
    val upper_prefix = properties(rand.nextInt(0, properties.length))

    val lowerPrefix = Datom(a = Some(lower_prefix))
    val lowerTerm: Datom = lower_prefix match {
      case "users/:age" =>

        if(withLowerPrefix){
          Datom(Some(lower_prefix), Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(rand.nextInt(18, 100)).flip().array())))
        } else {
          Datom(v = Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(rand.nextInt(18, 100)).flip().array())))
        }

      case "users/:color" =>

        if(withLowerPrefix){
          Datom(Some(lower_prefix), Some(ByteString.copyFrom(colors(rand.nextInt(0, colors.length)).getBytes())))
        } else {
          Datom(v = Some(ByteString.copyFrom(colors(rand.nextInt(0, colors.length)).getBytes())))
        }

      case "users/:height" =>
        val lh = rand.nextInt(150, 210)

        if(withLowerPrefix){
          Datom(Some(lower_prefix), Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(lh).flip().array())))
        } else {
          Datom(v = Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(lh).flip().array())))
        }
    }

    val upperPrefix = Datom(a = Some(upper_prefix))
    val upperTerm: Datom = upper_prefix match {
      case "users/:age" =>

        if(withLowerPrefix){
          Datom(Some(upper_prefix), Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(rand.nextInt(18, 100)).flip().array())))
        } else {
          Datom(v = Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(rand.nextInt(18, 100)).flip().array())))
        }


      case "users/:color" =>

        if(withLowerPrefix){
          Datom(Some(upper_prefix), Some(ByteString.copyFrom(colors(rand.nextInt(0, colors.length)).getBytes())))
        } else {
          Datom(v = Some(ByteString.copyFrom(colors(rand.nextInt(0, colors.length)).getBytes())))
        }

      case "users/:height" =>
        val lh = rand.nextInt(150, 210)

        if(withLowerPrefix){
          Datom(Some(upper_prefix), Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(lh).flip().array())))
        } else {
          Datom(v = Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(lh).flip().array())))
        }

    }

    val prefixOrd = new Ordering[Datom] {
      override def compare(pre: Datom, k: Datom): Int = {
        pre.getA.compareTo(k.getA)
      }
    }

    val termOrdPrefix = new Ordering[Datom]{
      override def compare(x: Datom, y: Datom): Int = {

        var r = ord.compare(x.getV.toByteArray, y.getV.toByteArray)

        if(r != 0) return r

        r = x.getE.compareTo(y.getE)

        if(r != 0) return r

        r = x.getT.compareTo(y.getT)

        if(r != 0) return r

        x.getOp.compareTo(y.getOp)

      }
    }

    val termOrd = avetOrd

    def ltPrefix(prefix: Datom, term: Datom, k: Datom, inclusive: Boolean): Boolean = {
      prefixOrd.equiv(k, prefix) && ((inclusive && termOrdPrefix.lteq(k, term)) || termOrdPrefix.lt(k, term))
    }

    def lt(term: Datom, k: Datom, inclusive: Boolean): Boolean = {
      (inclusive && termOrd.lteq(k, term)) || termOrd.lt(k, term)
    }

    def gtPrefix(prefix: Datom, term: Datom, k: Datom, inclusive: Boolean): Boolean = {
      prefixOrd.equiv(k, prefix) && ((inclusive && termOrdPrefix.gteq(k, term)) || termOrdPrefix.gt(k, term))
    }

    def gt(term: Datom, k: Datom, inclusive: Boolean): Boolean = {
      (inclusive && termOrd.gteq(k, term)) || termOrd.gt(k, term)
    }

    def range(lowerPrefix: Option[Datom], upperPrefix: Option[Datom], lowerTerm: Datom, upperTerm: Datom,
                      lowerPrefixOrd: Option[Ordering[Datom]],
                      upperPrefixOrd: Option[Ordering[Datom]],
                      lowerTermOrd: Ordering[Datom],
                      upperTermOrd: Ordering[Datom],
                      k: Datom,
                      inclusiveLower: Boolean,
                      inclusiveUpper: Boolean): Boolean = {

      ((lowerPrefix.isEmpty || lowerPrefixOrd.get.equiv(k, lowerPrefix.get)) && (inclusiveLower && lowerTermOrd.gteq(k, lowerTerm) || lowerTermOrd.gt(k, lowerTerm))) &&
        ((upperPrefix.isEmpty || upperPrefixOrd.get.equiv(k, upperPrefix.get)) && (inclusiveUpper && upperTermOrd.lteq(k, upperTerm) || upperTermOrd.lt(k, upperTerm)))
    }

    var dlist = Seq.empty[(Datom, Bytes)]
    var ilist = Seq.empty[(Datom, Bytes)]

    var op = ""

    rand.nextInt(1, 4) match {
      case 1 =>

        reverse = rand.nextBoolean()

        op = if(inclusiveLower) s"<= ${printDatom(lowerTerm, lower_prefix)}" else s"< ${printDatom(lowerTerm, lower_prefix)}"

        dlist = if(withLowerPrefix) tdata.filter{case (k, _) => ltPrefix(lowerPrefix, lowerTerm, k, inclusiveLower)} else
          tdata.filter{case (k, _) => lt(lowerTerm, k, inclusiveLower)}

        dlist = if(reverse) dlist.reverse else dlist

        ilist = if(withLowerPrefix) Await.result(TestHelper.all(index.lt(Some(lowerPrefix), lowerTerm, inclusiveLower, reverse)(Some(prefixOrd), termOrdPrefix)), Duration.Inf)
          else Await.result(TestHelper.all(index.lt(None, lowerTerm, inclusiveLower, reverse)(None, termOrd)), Duration.Inf)

      case 2 =>

        reverse = rand.nextBoolean()

        op = if(inclusiveLower) s">= ${printDatom(lowerTerm, lower_prefix)}" else s"> ${printDatom(lowerTerm, lower_prefix)}"

        dlist = if(withLowerPrefix) tdata.filter{case (k, _) => gtPrefix(lowerPrefix, lowerTerm, k, inclusiveLower)} else
          tdata.filter{case (k, _) => gt(lowerTerm, k, inclusiveLower)}

        dlist = if(reverse) dlist.reverse else dlist

        ilist = if(withLowerPrefix) Await.result(TestHelper.all(index.gt(Some(lowerPrefix), lowerTerm, inclusiveLower, reverse)(Some(prefixOrd), termOrdPrefix)), Duration.Inf)
          else Await.result(TestHelper.all(index.gt(None, lowerTerm, inclusiveLower, reverse)(None, termOrd)), Duration.Inf)

      case 3 =>

        reverse = rand.nextBoolean()

        op = s"${printDatom(lowerTerm, lower_prefix)} ${if(inclusiveLower) "<=" else "<"} x ${if(inclusiveUpper) "<=" else "<"} ${printDatom(upperTerm, upper_prefix)}"

        dlist = tdata.filter { case (k, _) => range(
          if(withLowerPrefix) Some(lowerPrefix) else None,
          if(withUpperPrefix) Some(upperPrefix) else None,
          lowerTerm,
          upperTerm,
          if(withLowerPrefix) Some(prefixOrd) else None,
          if(withUpperPrefix) Some(prefixOrd) else None,

          if(withLowerPrefix) termOrdPrefix else termOrd,
          if(withUpperPrefix) termOrdPrefix else termOrd,
          k,
          inclusiveLower,
          inclusiveUpper
        )}

        dlist = if(reverse) dlist.reverse else dlist

        ilist = Await.result(TestHelper.all(index.range(
          if(withLowerPrefix) Some(lowerPrefix) else None,
          if(withUpperPrefix) Some(upperPrefix) else None,

          lowerTerm,
          upperTerm,

          inclusiveLower,
          inclusiveUpper,
          reverse
        )(if(withLowerPrefix) Some(prefixOrd) else None, if(withUpperPrefix) Some(prefixOrd) else None,
          if(withLowerPrefix) termOrdPrefix else termOrd, if(withUpperPrefix) termOrdPrefix else termOrd)), Duration.Inf)

      case _ =>
    }

    logger.debug(s"${Console.BLUE_B}withLowerPrefix: ${withLowerPrefix} withUpperPrefix: ${withUpperPrefix} reverse: ${reverse}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}dlist ${op}: ${dlist.map{case (k, v) => printDatom(k, k.getA) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.BLUE_B}ilist ${op}: ${ilist.map{case (k, v) => printDatom(k, k.getA) -> new String(v)}}\n${Console.RESET}")

    assert(isColEqual(ilist, dlist))

  }

}