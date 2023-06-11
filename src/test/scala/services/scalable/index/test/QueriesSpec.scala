package services.scalable.index.test

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import services.scalable.index.grpc._
import services.scalable.index.impl._
import services.scalable.index.{Bytes, Commands, DefaultComparators, DefaultPrinters, DefaultSerializers, IndexBuilder, QueryableIndex, QueryableIndexTest}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class QueriesSpec extends Repeatable with Matchers {

  override val times: Int = 1000

  "queries" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = String
    type V = String

    import services.scalable.index.DefaultSerializers._
    import services.scalable.index.DefaultComparators._

    val NUM_LEAF_ENTRIES = 4//rand.nextInt(4, 64)
    val NUM_META_ENTRIES = 4//rand.nextInt(4, 64)

    val indexId = UUID.randomUUID().toString

    val storage = new MemoryStorage()

    implicit val grpcIntIntSerializer = new GrpcByteSerializer[Int, Int]()
    implicit val grpcStringStringSerializer = new GrpcByteSerializer[String, String]()

    val ordering = ordString

    /*val builder = IndexBuilder.create[K, V](DefaultComparators.ordInt)
      .storage(storage)
      .serializer(DefaultSerializers.grpcIntIntSerializer)
      .keyToStringConverter(DefaultPrinters.intToStringPrinter)*/

    val builder = IndexBuilder.create[K, V](ordering)
      .storage(storage)
      .serializer(grpcStringStringSerializer)
      //.keyToStringConverter(DefaultPrinters.intToStringPrinter)

    val indexContext = Await.result(TestHelper.loadOrCreateIndex(IndexContext(
      indexId,
      NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES
    ))(storage, global), Duration.Inf).get

    var data = Seq.empty[(K, V, Boolean)]
    var index = new QueryableIndexTest[K, V](indexContext)(builder)

    val prefixes = (0 until 10).map{_ => RandomStringUtils.randomAlphabetic(3)}
      .distinct.toList

    def insert(): Unit = {

      val descriptorBackup = index.descriptor

      val n = rand.nextInt(1, 1000)
      var list = Seq.empty[Tuple3[K, V, Boolean]]

      val insertDup = rand.nextBoolean()

      for(i<-0 until n){

        rand.nextBoolean() match {
          case x if x && list.length > 0 && insertDup =>

            // Inserts some duplicate
            val (k, v, _) = list(rand.nextInt(0, list.length))
            list = list :+ (k, v, false)

          case _ =>

            //val k = rand.nextInt(1, 1000)//RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
            //val v = rand.nextInt(1, 1000)//RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

            val k = s"${prefixes(rand.nextInt(0, prefixes.length))}${RandomStringUtils.randomAlphanumeric(5, 10)}"
            val v = RandomStringUtils.randomAlphanumeric(5)


            if (!data.exists { case (k1, _, _) => ordering.equiv(k, k1) } &&
              !list.exists { case (k1, _, _) => ordering.equiv(k, k1) }) {
              list = list :+ (k, v, false)
            }
        }
      }

      //logger.debug(s"${Console.GREEN_B}INSERTING ${list.map{case (k, v, _) => new String(k)}}${Console.RESET}")

      val cmds = Seq(
        Commands.Insert(indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if(result.success){
        logger.debug(s"${Console.GREEN_B}INSERTION OK: ${list.map{case (k, v, _) => builder.ks(k)}}${Console.RESET}")

        val newDescriptor = Await.result(index.save(), Duration.Inf)
        index = new QueryableIndexTest[K, V](newDescriptor)(builder)

        data = data ++ list

        return
      }

      logger.debug(s"${Console.RED_B}INSERTION FAIL: ${list.map{case (k, v, _) => builder.ks(k)}}${Console.RESET}")

      index = new QueryableIndexTest[K, V](descriptorBackup)(builder)
      result.error.get.printStackTrace()
    }

    def update(): Unit = {

      val lastVersion: Option[String] = rand.nextBoolean() match {
        case true => Some(index.ctx.id)
        case false => Some(UUID.randomUUID.toString)
      }

      val descriptorBackup = index.descriptor

      val n = if(data.length >= 2) rand.nextInt(1, data.length) else 1
      val list = scala.util.Random.shuffle(data).slice(0, n).map { case (k, v, _) =>
        (k, RandomStringUtils.randomAlphanumeric(5), lastVersion)
      }

      val cmds = Seq(
        Commands.Update(indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if(result.success){

        logger.debug(s"${Console.MAGENTA_B}UPDATED RIGHT LAST VERSION ${list.map{case (k, _, _) => builder.ks(k)}}...${Console.RESET}")

        val newDescriptor = Await.result(index.save(), Duration.Inf)
        index = new QueryableIndexTest[K, V](newDescriptor)(builder)

        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _, _) => ordering.equiv(k, k1) } }
        data = data ++ list.map { case (k, v, _) => (k, v, true) }

        return
      }

      index = new QueryableIndexTest[K, V](descriptorBackup)(builder)
      result.error.get.printStackTrace()
      logger.debug(s"${Console.CYAN_B}UPDATED WRONG LAST VERSION ${list.map { case (k, _, _) => builder.ks(k) }}...${Console.RESET}")
    }

    def remove(): Unit = {

      val lastVersion: Option[String] = rand.nextBoolean() match {
        case true => Some(index.ctx.id)
        case false => Some(UUID.randomUUID.toString)
      }

      val descriptorBackup = index.descriptor

      val n = if(data.length >= 2) rand.nextInt(1, data.length) else 1
      val list: Seq[Tuple2[K, Option[String]]] = scala.util.Random.shuffle(data).slice(0, n).map { case (k, _, _) =>
        (k, lastVersion)
      }

      val cmds = Seq(
        Commands.Remove[K, V](indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if(result.success){

        val newDescriptor = Await.result(index.save(), Duration.Inf)
        index = new QueryableIndexTest[K, V](newDescriptor)(builder)

        logger.debug(s"${Console.YELLOW_B}REMOVED RIGHT VERSION ${list.map { case (k, _) => builder.ks(k) }}...${Console.RESET}")
        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _) => ordering.equiv(k, k1) } }

        return
      }

      index = new QueryableIndexTest[K, V](descriptorBackup)(builder)
      result.error.get.printStackTrace()
      logger.debug(s"${Console.RED_B}REMOVED WRONG VERSION ${list.map { case (k, _) => builder.ks(k) }}...${Console.RESET}")
    }

    val n = 10

    for(i<-0 until n){
      rand.nextInt(1, 4) match {
        case 1 => insert()
        case 2 if !data.isEmpty => update()
        case 3 if !data.isEmpty => remove()
        case _ => insert()
      }
    }

    data = data.sortBy(_._1)

    val dlist = data.map{case (k, v, _) => k -> v}
    val ilist = Await.result(TestHelper.all(index.inOrder()), Duration.Inf).map{case (k, v, _) => k -> v}

    logger.debug(s"${Console.GREEN_B}tdata: ${dlist.map{case (k, v) => builder.ks(k) -> builder.vs(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata: ${ilist.map{case (k, v) => builder.ks(k) -> builder.vs(v)}}${Console.RESET}\n")

    Await.result(storage.close(), Duration.Inf)

    assert(TestHelper.isColEqual(dlist, ilist))

    if(!data.isEmpty) {

      val inclusive = rand.nextBoolean()
      val pos = rand.nextInt(0, data.length)
      val (k, _, _) = data(pos)

      var idx = data.indexWhere{x => if(inclusive) builder.ord.gteq(x._1, k) else builder.ord.gt(x._1, k)}
      val gtData = (if(idx >= 0) data.slice(idx, data.length) else Seq.empty[(K, V, Boolean)])
        .map(x => x._1 -> x._2).toList

      idx = data.indexWhere { x => builder.ord.gteq(x._1, k) }
      val ltData = (if (idx >= 0) data.slice(0, if(inclusive) idx + 1 else idx) else Seq.empty[(K, V, Boolean)])
        .map(x => x._1 -> x._2).toList

      var itr = index.gt(k, inclusive)
      val gtIndex = Await.result(TestHelper.all(itr), Duration.Inf).map(x => x._1 -> x._2).toList

      itr = index.lt(k, inclusive)
      val ltIndex = Await.result(TestHelper.all(itr), Duration.Inf).map(x => x._1 -> x._2).toList

      itr = index.ltr(k, inclusive)
      val ltrIndex = Await.result(TestHelper.all(itr), Duration.Inf).map(x => x._1 -> x._2).toList

      itr = index.gtr(k, inclusive)
      val gtrIndex = Await.result(TestHelper.all(itr), Duration.Inf).map(x => x._1 -> x._2).toList

      val cr1 = gtIndex == gtData
      val cr2 = ltIndex == ltData

      if(!cr1){
        println()
      }

      if (!cr2) {
        println()
      }

      assert(cr1 && cr2)

      val cr3 = gtrIndex == gtData.reverse
      val cr4 = ltrIndex == ltData.reverse

      if (!cr3) {
        println()
      }

      if (!cr3) {
        println()
      }

      assert(cr3 && cr4)

      /*val beforeKey: Option[K] = if(pos == 0) None else Some(data(pos - 1)._1)
      val beforeKeyIndex = Await.result(index.beforeKey(k), Duration.Inf)*/

      /*val afterKey: Option[K] = if (pos < data.length - 1) Some(data(pos + 1)._1) else None
      val afterKeyIndex = Await.result(index.afterKey(k), Duration.Inf)*/

      /*val afterKey: Option[K] = if (pos < data.length - 1) Some(data(pos + 1)._1) else None
      val afterKeyIndex = Await.result(index.gt(k, false)(builder.ord).next(),
        Duration.Inf).headOption.map(_._1)*/

      //println(s"before key: ${k}: ${beforeKey} after key: ${afterKey} beforeKeyIndex: ${beforeKeyIndex} afterKeyIndex: ${afterKeyIndex}")

      //assert(beforeKey == beforeKeyIndex)
      //assert(afterKey == afterKeyIndex)

      logger.info(Await.result(index.save(), Duration.Inf).toString)
      println()

    }

  }

}
