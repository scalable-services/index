package services.scalable.index.test

import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import services.scalable.index.grpc._
import services.scalable.index.impl._
import services.scalable.index.{Commands, IndexBuilder, QueryableIndexDev}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class QueriesSpecDev2 extends Repeatable with Matchers {

  override val times: Int = 1000

  "queries" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = String
    type V = String

    import services.scalable.index.DefaultComparators._
    import services.scalable.index.DefaultSerializers._

    val NUM_LEAF_ENTRIES = 8//rand.nextInt(4, 64)
    val NUM_META_ENTRIES = 8//rand.nextInt(4, 64)

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
    var index = new QueryableIndexDev[K, V](indexContext)(builder)

    val prefixes = (0 until 10).map{_ => RandomStringUtils.randomAlphabetic(3).toLowerCase}
      .distinct.toList

    def insert(): Unit = {

      val descriptorBackup = index.descriptor

      val n = 1000//rand.nextInt(20, 1000)
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

            val k = s"${prefixes(rand.nextInt(0, prefixes.length))}${RandomStringUtils.randomAlphabetic(5)}".toLowerCase
            val v = RandomStringUtils.randomAlphabetic(5).toLowerCase


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
        index = new QueryableIndexDev[K, V](newDescriptor)(builder)

        data = data ++ list

        return
      }

      logger.debug(s"${Console.RED_B}INSERTION FAIL: ${list.map{case (k, v, _) => builder.ks(k)}}${Console.RESET}")

      index = new QueryableIndexDev[K, V](descriptorBackup)(builder)
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
        (k, RandomStringUtils.randomAlphabetic(5).toLowerCase, lastVersion)
      }

      val cmds = Seq(
        Commands.Update(indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if(result.success){

        logger.debug(s"${Console.MAGENTA_B}UPDATED RIGHT LAST VERSION ${list.map{case (k, _, _) => builder.ks(k)}}...${Console.RESET}")

        val newDescriptor = Await.result(index.save(), Duration.Inf)
        index = new QueryableIndexDev[K, V](newDescriptor)(builder)

        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _, _) => ordering.equiv(k, k1) } }
        data = data ++ list.map { case (k, v, _) => (k, v, true) }

        return
      }

      index = new QueryableIndexDev[K, V](descriptorBackup)(builder)
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
        index = new QueryableIndexDev[K, V](newDescriptor)(builder)

        logger.debug(s"${Console.YELLOW_B}REMOVED RIGHT VERSION ${list.map { case (k, _) => builder.ks(k) }}...${Console.RESET}")
        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _) => ordering.equiv(k, k1) } }

        return
      }

      index = new QueryableIndexDev[K, V](descriptorBackup)(builder)
      result.error.get.printStackTrace()
      logger.debug(s"${Console.RED_B}REMOVED WRONG VERSION ${list.map { case (k, _) => builder.ks(k) }}...${Console.RESET}")
    }

    val n = 1

    for(i<-0 until n){
      /*rand.nextInt(1, 4)*/1 match {
        case 1 => insert()
        case 2 if !data.isEmpty => update()
        case 3 if !data.isEmpty => remove()
        case _ => insert()
      }
    }

    val list = data
    data = data.sortBy(_._1)

    val dlist = data.map{case (k, v, _) => k -> v}
    val ilist = Await.result(TestHelper.all(index.inOrder()), Duration.Inf).map{case (k, v, _) => k -> v}

    logger.debug(s"${Console.GREEN_B}tdata: ${dlist.map{case (k, v) => builder.ks(k) -> builder.vs(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata: ${ilist.map{case (k, v) => builder.ks(k) -> builder.vs(v)}}${Console.RESET}\n")

    Await.result(storage.close(), Duration.Inf)

    assert(TestHelper.isColEqual(dlist, ilist))

    if(data.length > 1) {

      val suffixComp = new Ordering[K] {
        override def compare(k: K, term: K): Int = {
          val suffix = k.slice(3, k.length)

          ordering.compare(suffix, term)
        }
      }

      val fromInclusive = rand.nextBoolean()
      val toInclusive = rand.nextBoolean()

      val posFrom = rand.nextInt(0, data.length)
      val posTo = rand.nextInt(posFrom, data.length)

      val (kFrom, _, _) = data(posFrom)
      val (kTo, _, _) = data(posTo)

      val prefixFrom = kFrom.slice(0, 3)
      val prefixTo = kTo.slice(0, 3)

      val termFrom = kFrom.slice(3, kFrom.length)
      val termTo = kTo.slice(3, kTo.length)

      val prefixComp = new Ordering[K] {
        override def compare(x: K, prefix: K): Int = {
          val pk = x.slice(0, 3)
          ordering.compare(pk, prefix)
        }
      }

      val reverse = false//rand.nextBoolean()

      /*val idx2 = data.indexWhere(x => prefixComp.equiv(x._1, prefix) && (inclusive && suffixComp.gteq(x._1, term) || suffixComp.gt(x._1, term)))
      var slice2 = if (idx2 >= 0) data.slice(idx2, data.length).filter { x => prefixComp.equiv(x._1, prefix) && (inclusive && suffixComp.gteq(x._1, term) || suffixComp.gt(x._1, term)) }.map { x => x._1 -> x._2 }
        else Seq.empty[(K, V)]*/

      /*val idx2 = data.indexWhere(x => prefixComp.equiv(x._1, prefix) && (inclusive && suffixComp.lteq(x._1, term) || suffixComp.lt(x._1, term)))
      var slice2 = if (idx2 >= 0) data.slice(idx2, data.length).filter { x => prefixComp.equiv(x._1, prefix) && (inclusive && suffixComp.lteq(x._1, term) || suffixComp.lt(x._1, term)) }.map { x => x._1 -> x._2 }
      else Seq.empty[(K, V)]*/

      val idx2 = data.indexWhere(k => (fromInclusive && builder.ord.gteq(k._1, kFrom)) || builder.ord.gt(k._1, kFrom))
      var slice2 = if (idx2 >= 0) data.slice(idx2, data.length).filter { k =>
        ((fromInclusive && builder.ord.gteq(k._1, kFrom)) || builder.ord.gt(k._1, kFrom)) &&
          ((toInclusive && builder.ord.lteq(k._1, kTo)) || builder.ord.lt(k._1, kTo))
      }.map{x => x._1 -> x._2}.toList else Seq.empty[(K, V)]

      slice2 = if(reverse) slice2.reverse else slice2

      //val itr = index.gt(prefix, k, inclusive, reverse)(prefixComp, ordering)
      //val itr = index.lt(prefix, k, inclusive, reverse)(prefixComp, ordering)
      val itr = index.range(kFrom, kTo, fromInclusive, toInclusive, reverse)
      val gtPrefixIndex = Await.result(TestHelper.all(itr), Duration.Inf).map(x => x._1 -> x._2).toList

      val cr = slice2 == gtPrefixIndex

      if(!cr){
        //index.prettyPrint()
        //logger.debug(s"""${Console.GREEN_B}INSERTING ${list.map{case (k, v, _) => s""""${builder.ks(k)}" -> "${builder.vs(v)}"""" }}${Console.RESET}""")

        println()
      }

      assert(cr)

      logger.info(Await.result(index.save(), Duration.Inf).toString)
      println()

    }

  }

}
