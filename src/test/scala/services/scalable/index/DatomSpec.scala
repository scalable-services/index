package services.scalable.index

import com.google.common.base.Charsets
import com.google.protobuf.ByteString
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.Datom
import services.scalable.index.impl.{DefaultCache, DefaultContext, MemoryStorage}
import services.scalable.index.{Bytes, QueryableIndex, Tuple}

import java.nio.ByteBuffer
import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class DatomSpec extends Repeatable {

  override val times: Int = 1000

  "datom operations" should " run successfully" in {

    type K = Datom
    type V = Bytes

    type T = services.scalable.index.QueryableIndex[K, V]
    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    import services.scalable.index.DefaultComparators._

    /**
     * Why start with tx and op? Because tx is recovered from the latest root of the tree and operation is always present.
     */
    implicit val avetOrd = new Ordering[Datom] {
      override def compare(x: Datom, y: Datom): Int = {

        var r: Int = 0

        /* r = x.getT.compareTo(y.getT)

         if(r != 0) return r

         r = x.getOp.compareTo(y.getOp)*/

        if(r != 0) return r

        r = x.getA.compareTo(y.getA)

        if(x.v.isEmpty || y.v.isEmpty || r != 0) return r

        r = ord.compare(x.getV.toByteArray, y.getV.toByteArray)

        if(x.e.isEmpty || y.e.isEmpty || r != 0) return r

        r = x.getE.compareTo(y.getE)

        r
      }
    }

    val NUM_LEAF_ENTRIES = 8
    val NUM_META_ENTRIES = 8

    val indexId = UUID.randomUUID().toString

    implicit val cache = new DefaultCache[K, V](MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new CassandraStorage[Bytes, Bytes](TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, truncate = true)
    implicit val storage = new MemoryStorage[K, V](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    implicit val ctx = new DefaultContext[K, V](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    val index = new QueryableIndex[K, V]()

    def printDatom(d: Datom, p: String): String = {
      p match {
        case "users/:name" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t},${d.op}]"
        case "users/:age" => s"[${d.a},${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()},${d.e},${d.t},${d.op}]"
        case "users/:color" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}, ${d.op}]"
        case "users/:height" => s"[${d.a},${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()},${d.e},${d.t},${d.op}]"
        case _ => ""
      }
    }

    val colors = Seq("ambar", "red", "green", "blue", "yellow", "orange", "black", "white", "magenta", "gold", "brown", "pink", "cyan", "purple")

    val n = 100
    var data = Seq.empty[(K, V)]

    var tx = UUID.randomUUID().toString
    var lastColor: Bytes = Array.empty[Byte]
    var lastHeight: Bytes = Array.empty[Byte]
    var colorList = Seq.empty[(String, Boolean)]
    var lastColorDatom: Datom = null

    def insert(): Unit = {
      var list = Seq.empty[Tuple[K, V]]

      for(i<-0 until n){
        val id = UUID.randomUUID.toString
        val name = RandomStringUtils.randomAlphanumeric(5, 10).getBytes()
        val age = java.nio.ByteBuffer.allocate(4).putInt(rand.nextInt(18, 100)).flip().array()
        val color = colors(rand.nextInt(0, colors.length)).getBytes()
        val height = java.nio.ByteBuffer.allocate(4).putInt(rand.nextInt(150, 210)).flip().array()

        lastColor = color
        lastHeight = height

        val colorOk = true//rand.nextBoolean()

        //AVET
        val datoms = Seq(
          Datom(Some("users/:name"), Some(ByteString.copyFrom(name)), Some(id),  Some(tx), Some(true)),
          Datom(Some("users/:age"), Some(ByteString.copyFrom(age)), Some(id), Some(tx), Some(true)),
          Datom(Some("users/:color"), Some(ByteString.copyFrom(color)), Some(id), Some(tx), Some(colorOk)),
          Datom(Some("users/:height"), Some(ByteString.copyFrom(height)), Some(id), Some(tx), Some(true))
        )

        lastColorDatom = datoms(2)

        colorList = colorList :+ new String(color, Charsets.UTF_8) -> colorOk

        for(d <- datoms){
          list = list :+ d -> Array.empty[Byte]
        }
      }

      val result = Await.result(index.insert(list), Duration.Inf)

      if(result > 0){
        data = data ++ list.slice(0, result)
      }
    }

    insert()
    Await.ready(ctx.save(), Duration.Inf)

    val tdata = data.sortBy(_._1)(avetOrd)
    val idata = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)

    logger.debug(s"tdata: ${tdata.map{case (k, v) => printDatom(k, k.getA) -> new String(v)}}\n")
    logger.debug(s"idata: ${idata.map{case (k, v) => printDatom(k, k.getA) -> new String(v)}}")

    assert(idata == tdata)

    println()

    def find(k: Datom, term: K, order: Ordering[Datom]): Boolean = {
      order.equiv(k, term)
    }

    data = data.sortBy(_._1)(avetOrd)

    /*implicit val datomToStr = (d: K) => printDatom(d, d.getA)
    implicit val bytesToStr = (v: Bytes) => new String(v, Charsets.UTF_8)

    index.prettyPrint()*/

    println("max values: ")
    val d0 = data.filter{case (d, _) => d.getA.compareTo("users/:color") == 0}.head._1
    val d1 = data.filter{case (d, _) => d.getA.compareTo("users/:height") == 0}.last._1

    println()
    println(printDatom(d0, d0.getA))
    println(printDatom(d1, d1.getA))
    println()

    logger.debug(s"lastColor: ${new String(lastColor)} last height: ${ByteBuffer.wrap(lastHeight).getInt} last datom: ${printDatom(lastColorDatom, lastColorDatom.getA)}")

    /*tx = UUID.randomUUID().toString
    println(s"removal: ${Await.result(index.insert(Seq(lastColorDatom.withOp(false).withT(tx) -> Array.empty[Byte])),
      Duration.Inf)}")*/

    val rd0 = scala.util.Random.shuffle(data).head._1

    val fromTerm = Datom(a = Some(rd0.getA), v = Some(rd0.getV), t = Some(tx), op = Some(true))
    val fromPrefix = Datom(a = Some(rd0.getA)/*, v = Some(ByteString.copyFrom(lastColor))*/, t = Some(tx), op = Some(true))

    val rd1 = scala.util.Random.shuffle(data).head._1

    //val toTerm = Datom(a = Some("users/:height"), v = Some(ByteString.copyFrom(lastHeight)), t = Some(tx), op = Some(true))
    //val toPrefix = Datom(a = Some("users/:height")/*, v = Some(ByteString.copyFrom(lastHeight))*/, t = Some(tx), op = Some(true))

    val toTerm = Datom(a = Some(rd1.getA), v = Some(rd1.getV), t = Some(tx), op = Some(true))
    val toPrefix = Datom(a = Some(rd1.getA)/*, v = Some(ByteString.copyFrom(lastColor))*/, t = Some(tx), op = Some(true))

    //val dlist = data.filter{case (k, _) => find(k, fromTerm, avetOrd)}

    def lt(k: K, fromPrefix: Option[K], fromWord: K, inclusiveFrom: Boolean, order: Ordering[K]): Boolean = {
      (fromPrefix.isEmpty || order.equiv(k, fromPrefix.get)) && (inclusiveFrom && order.lteq(k, fromWord) || !inclusiveFrom && order.lt(k, fromWord))
    }

    def gt(k: K, fromPrefix: Option[K], fromWord: K, inclusiveFrom: Boolean, order: Ordering[K]): Boolean = {
      (fromPrefix.isEmpty || order.equiv(k, fromPrefix.get)) && (inclusiveFrom && order.gteq(k, fromWord) || !inclusiveFrom && order.gt(k, fromWord))
    }

    def range(k: K, from: K, to: K, inclusiveFrom: Boolean, inclusiveTo: Boolean, order: Ordering[K]): Boolean = {
      (inclusiveFrom && order.gteq(k, from) || !inclusiveFrom && order.gt(k, from)) &&
        (inclusiveTo && order.lteq(k, to) || !inclusiveTo && order.lt(k, to))
    }

    val inclusiveFrom = rand.nextBoolean()
    val inclusiveTo = rand.nextBoolean()

    val reverse = rand.nextBoolean()

    var dlist = Seq.empty[Tuple2[K, V]]
    var it: RichAsyncIterator[K, V] = null

    rand.nextInt(1, 4) match {
      case 1 =>

        dlist = data.filter{case (k, _) => gt(k, Some(fromPrefix), fromTerm, inclusiveFrom, avetOrd)}
        dlist = if(reverse) dlist.reverse else dlist
        it = index.gt(Some(fromPrefix), fromTerm, inclusiveFrom, reverse)(avetOrd)

      case 2 =>

        dlist = data.filter{case (k, _) => lt(k, Some(fromPrefix), fromTerm, inclusiveFrom, avetOrd)}
        dlist = if(reverse) dlist.reverse else dlist
        it = index.lt(Some(fromPrefix), fromTerm, inclusiveFrom, reverse)(avetOrd)

      case 3 =>

        dlist = data.filter{case (k, _) => range(k, fromTerm, toTerm, inclusiveFrom, inclusiveTo, avetOrd)}
        dlist = if(reverse) dlist.reverse else dlist
        it = index.range(fromTerm, toTerm, inclusiveFrom, inclusiveTo, reverse)(avetOrd)

      case _ =>
    }

    val ilist = Await.result(TestHelper.all(it), Duration.Inf)

    logger.debug(s"\n${Console.GREEN_B}dlist: ${dlist.map{case (k, v) => printDatom(k, k.getA)}}${Console.RESET}\n")
    logger.debug(s"\n${Console.MAGENTA_B}ilist: ${ilist.map{case (k, v) => printDatom(k, k.getA)}}${Console.RESET}\n")

    assert(dlist == ilist)

  }

}
