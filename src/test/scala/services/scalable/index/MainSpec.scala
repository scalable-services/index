package services.scalable.index

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

  override val times: Int = 100

  val rand = ThreadLocalRandom.current()

  "index data " must "be equal to test data" in {

    implicit def xf(k: Bytes): String = new String(k)

    import DefaultComparators._
    import DefaultIdGenerators._

    implicit val avetOrd = new Ordering[Datom] {
      override def compare(x: Datom, y: Datom): Int = {
        var r = x.getA.compareTo(y.getA)

        if(r != 0) return r

        r = x.getV.toByteArray.compareTo(y.getV.toByteArray)

        if(r != 0) return r

        r = x.getE.compareTo(y.getE)

        if(r != 0) return r

        r = x.getT.compareTo(y.getT)

        if(r != 0) return r

        x.getOp.compareTo(y.getOp)
      }
    }

    val NUM_LEAF_ENTRIES = 8//rand.nextInt(4, 20)
    val NUM_META_ENTRIES = 8//rand.nextInt(4, if(NUM_LEAF_ENTRIES == 4) 5 else NUM_LEAF_ENTRIES)

    val indexId = "test_index"

    implicit val cache = new DefaultCache[Datom, Bytes](MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new CassandraStorage[Bytes, Bytes](TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, truncate = true)
    implicit val storage = new MemoryStorage[Datom, Bytes](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    implicit val ctx = new DefaultContext[Datom, Bytes](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    
    var data = Seq.empty[Tuple[Datom, Bytes]]
    val iter = 5//rand.nextInt(1, 100)

    val colors = Seq("red", "green", "blue", "yellow", "orange", "black", "white", "magenta", "gold", "brown", "pink")

    def insert(index: Index[Datom, Bytes]): Unit = {

      val n = rand.nextInt(1, 50)

      var list = Seq.empty[Tuple[Datom, Bytes]]

      for(i<-0 until n){
        val now = System.currentTimeMillis()
        val id = UUID.randomUUID.toString.getBytes()
        val name = RandomStringUtils.randomAlphanumeric(5, 10).getBytes()
        val age = java.nio.ByteBuffer.allocate(4).putInt(rand.nextInt(18, 100)).flip().array()
        val color = colors(rand.nextInt(0, colors.length)).getBytes()
        val height = java.nio.ByteBuffer.allocate(4).putInt(rand.nextInt(150, 210)).flip().array()

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

    /*def remove(index: Index[Bytes, Bytes]): Unit = {
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
    }*/

    def printDatom(d: Datom): String = {
      d.getA match {
       /* case "users/:name" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]"
        case "users/:age" => s"[${d.a},${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()},${d.e},${d.t}]"
        case "users/:color" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]"
        case "users/:height" => s"[${d.a},${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()},${d.e},${d.t}]"*/
        case "users/:height" => s"[${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()}]"
        case _ => ""
      }
    }

   // implicit val storage = new CassandraStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES, "indexes")

    val index = new QueryableIndex[Datom, Bytes]()

    for(i<-0 until iter){
      rand.nextInt(1, 2) match {
        case 1 => insert(index)
        /*case 2 => update(index)
        case 3 => remove(index)*/
        case _ =>
      }

      Await.ready(ctx.save(), Duration.Inf)
    }

    val tdata = data.sortBy(_._1)
    val idata = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)

    logger.debug(s"tdata: ${tdata.map{case (k, v) => printDatom(k) -> new String(v)}}\n")
    logger.debug(s"idata: ${idata.map{case (k, v) => printDatom(k) -> new String(v)}}")

    assert(isColEqual(idata, tdata))

    if(tdata.length < 0){
      println()

      val inclusive = rand.nextBoolean()

      val term = Datom(Some("users/:height"), Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(rand.nextInt(180, 210)).flip().array())))
      val prefix = Datom(a = Some("users/:height"))

      val prefixOrd = new Ordering[Datom] {
        override def compare(x: Datom, y: Datom): Int = {
          x.getA.compareTo("users/:height")
        }
      }

      var termOrd = new Ordering[Datom] {
        override def compare(x: Datom, y: Datom): Int = {
          /*val r = x.a.compareTo(y.a)

          if(r != 0) return -1
*/
          x.getV.toByteArray.compareTo(y.getV.toByteArray)
        }
      }

      /*val dgreater = tdata.filter{case (k, _) => if(inclusive) termOrd.gteq(k, d) else termOrd.gt(k, d)}
      val igreater = Await.result(TestHelper.all(index.gt(d, inclusive)(termOrd)), Duration.Inf)

      logger.debug(s"dgreater > ${printDatom(d)}: ${dgreater.map{case (k, v) => printDatom(k) -> new String(v)}}\n")
      logger.debug(s"igreater > ${printDatom(d)}: ${igreater.map{case (k, v) => printDatom(k) -> new String(v)}}")

      assert(isColEqual(igreater, dgreater))

      println()*/

      termOrd = new Ordering[Datom] {
        override def compare(x: Datom, y: Datom): Int = {
          x.getV.toByteArray.compareTo(y.getV.toByteArray)
        }
      }

      def check(prefix: Datom, term: Datom, k: Datom, inclusive: Boolean): Boolean = {
        prefixOrd.equiv(k, prefix) && ((inclusive && termOrd.lteq(k, term)) || termOrd.lt(k, term))
      }

      val dlower = tdata.filter{case (k, _) => check(prefix, term, k, inclusive)}
      val ilower = Await.result(TestHelper.all(index.lt(prefix, term, inclusive)(prefixOrd, termOrd)), Duration.Inf)

      logger.debug(s"${Console.MAGENTA_B}dlower < ${printDatom(term)}: ${dlower.map{case (k, v) => printDatom(k) -> new String(v)}}${Console.RESET}\n")
      logger.debug(s"${Console.BLUE_B}ilower < ${printDatom(term)}: ${ilower.map{case (k, v) => printDatom(k) -> new String(v)}}\n${Console.RESET}")

      assert(isColEqual(ilower, dlower))

    }

  }

}
