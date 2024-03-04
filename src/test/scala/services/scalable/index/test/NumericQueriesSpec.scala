package services.scalable.index.test

import ch.qos.logback.classic.{Level, Logger}
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import services.scalable.index.grpc._
import services.scalable.index.impl._
import services.scalable.index.{Commands, DefaultSerializers, IndexBuilder, QueryableIndex}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class NumericQueriesSpec extends Repeatable with Matchers {

  LoggerFactory.getLogger("services.scalable.index.Context").asInstanceOf[Logger].setLevel(Level.INFO)
  LoggerFactory.getLogger("services.scalable.index.impl.GrpcByteSerializer").asInstanceOf[Logger].setLevel(Level.INFO)

  override val times: Int = 10000

  "queries" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Long
    type V = Long

    import services.scalable.index.DefaultComparators._
    import services.scalable.index.DefaultSerializers._

    val NUM_LEAF_ENTRIES = rand.nextInt(4, 128)
    val NUM_META_ENTRIES = rand.nextInt(4, 128)

    val indexId = UUID.randomUUID().toString

    val storage = new MemoryStorage()
    val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)

    implicit val grpcLongLongSerializer = new GrpcByteSerializer[Long, Long]()

    val ordering = ordLong

    val indexContext = Await.result(TestHelper.loadOrCreateIndex(IndexContext(
      indexId,
      NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES,
      maxNItems = -1L
    ))(storage, global), Duration.Inf).get

    val builder = IndexBuilder.create[K, V](global, ordering,
        indexContext.numLeafItems, indexContext.numMetaItems, indexContext.maxNItems,
        DefaultSerializers.longSerializer, DefaultSerializers.longSerializer)
      .storage(storage)
      .serializer(grpcLongLongSerializer)
    //.keyToStringConverter(DefaultPrinters.intToStringPrinter)
    .build()

    var data = Seq.empty[(K, V, Option[String])]
    var index = new QueryableIndex[K, V](indexContext)(builder)

    def insert(): Unit = {

      val currentVersion = Some(index.ctx.id)
      val indexBackup = index

      val n = rand.nextInt(1, 1000)
      var list = Seq.empty[Tuple3[K, V, Boolean]]

      for (i <- 0 until n) {
        val k = rand.nextLong(0, Long.MaxValue)
        val v = rand.nextLong(0, 1000)

        if (!data.exists { case (k1, _, _) => ordering.equiv(k, k1) } &&
          !list.exists { case (k1, _, _) => ordering.equiv(k, k1) }) {
          list = list :+ (k, v, false)
        }
      }

      //logger.debug(s"${Console.GREEN_B}INSERTING ${list.map{case (k, v, _) => builder.ks(k)}}${Console.RESET}")

      val cmds = Seq(
        Commands.Insert(indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if (result.success) {
        logger.debug(s"${Console.GREEN_B}INSERTION OK: ${list.map { case (k, v, _) => builder.ks(k) }}${Console.RESET}")

        val newDescriptor = Await.result(index.save(), Duration.Inf)
        index = new QueryableIndex[K, V](newDescriptor)(builder)

        data = data ++ list.map { case (k, v, _) => (k, v, currentVersion) }

        return
      }

      logger.debug(s"${Console.RED_B}INSERTION FAIL: ${list.map { case (k, v, _) => builder.ks(k) }}${Console.RESET}")

      index = indexBackup
      result.error.get.printStackTrace()
    }

    val n = rand.nextInt(1, 10)

    for(i<-0 until n){
      insert()
    }

    data = data.sortBy(_._1)

    val dlist = data.map{case (k, v, _) => k -> v}
    val ilist = Await.result(index.all(), Duration.Inf).map{case (k, v, _) => k -> v}

    logger.debug(s"${Console.GREEN_B}tdata: ${dlist.map{case (k, v) => builder.ks(k) -> builder.vs(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata: ${ilist.map{case (k, v) => builder.ks(k) -> builder.vs(v)}}${Console.RESET}\n")

    Await.result(storage.close(), Duration.Inf)

    assert(TestHelper.isColEqual(dlist, ilist))

    if(data.length > 1) {

      def testGt(term: K, inclusive: Boolean, reverse: Boolean)(termComp: Ordering[K]): Unit = {
        var idx = data.reverse.indexWhere { case (k, _, _) =>
          inclusive && termComp.gteq(k, term) || termComp.gt(k, term)
        }

        idx = if (idx < 0) 0 else idx

        var slice = if (idx >= 0) data.slice(idx, data.length).filter { x =>
          inclusive && termComp.gteq(x._1, term) || termComp.gt(x._1, term)
        }.map { x => x._1 -> x._2 } else Seq.empty[(K, V)]

        slice = if (reverse) slice.reverse else slice

        val itr = index.gt(term, inclusive, reverse)(termComp)
        val indexData = Await.result(index.all(itr), Duration.Inf).map(x => x._1 -> x._2).toList

        val cr = slice == indexData

        if (!cr) {
          println()
        }

        assert(cr)
      }

      def testLt(term: K, inclusive: Boolean, reverse: Boolean)(termComp: Ordering[K]): Unit = {
        var idx = data.indexWhere { case (k, _, _) =>
          inclusive && termComp.lteq(k, term) || termComp.lt(k, term)
        }

        idx = if (idx < 0) 0 else idx

        var slice = if (idx >= 0) data.slice(idx, data.length).filter { x =>
          inclusive && termComp.lteq(x._1, term) || termComp.lt(x._1, term)
        }.map { x => x._1 -> x._2 } else Seq.empty[(K, V)]

        slice = if (reverse) slice.reverse else slice

        logger.debug(s"term: ${builder.ks(term)}...")

        val itr = index.lt(term, inclusive, reverse)(termComp)
        val indexData = Await.result(index.all(itr), Duration.Inf).map(x => x._1 -> x._2).toList

        val cr = slice == indexData

        if (!cr) {
          println()
        }

        assert(cr)
      }

      def testRange(from: K, to: K, inclusiveFrom: Boolean, inclusiveTo: Boolean, reverse: Boolean)(termComp: Ordering[K]): Unit = {
        var idx = data.indexWhere { case (k, _, _) =>
          inclusiveFrom && termComp.gteq(k, from) || termComp.gt(k, from)
        }

        idx = if (idx < 0) 0 else idx

        var slice = if (idx >= 0) data.slice(idx, data.length).filter { k =>
          ((inclusiveFrom && termComp.gteq(k._1, from)) || termComp.gt(k._1, from)) &&
            ((inclusiveTo && termComp.lteq(k._1, to)) || termComp.lt(k._1, to))
        }.map { x => x._1 -> x._2 } else Seq.empty[(K, V)]

        slice = if (reverse) slice.reverse else slice

        val itr = index.range(from, to, inclusiveFrom, inclusiveTo, reverse)(termComp)
        val indexData = Await.result(index.all(itr), Duration.Inf).map(x => x._1 -> x._2).toList

        val cr = slice == indexData

        if (!cr) {
          println()
        }

        assert(cr)
      }

      val idx = rand.nextInt(0, data.length)

      val from = data(idx)._1
      val remaining = data.sortBy(_._1).slice(idx, data.length)
      val to = remaining(rand.nextInt(0, remaining.length))._1

      var FROM = if(rand.nextBoolean()) from + rand.nextLong(0, 1000) else from
      var TO = if(rand.nextBoolean()) to + rand.nextLong(0, 1000) else to

      FROM = if(ordering.gt(from, to)) to else from
      TO = if(ordLong.gt(from, to)) from else to

      testGt(FROM, rand.nextBoolean(), rand.nextBoolean())(ordering)
      testLt(TO, rand.nextBoolean(), rand.nextBoolean())(ordering)

      if (data.length > 1){
        testRange(FROM, TO, rand.nextBoolean(), rand.nextBoolean(), rand.nextBoolean())(ordering)
      }

      logger.info(Await.result(index.save(), Duration.Inf).toString)

      index.ctx.clear()
      storage.close()
      cache.invalidateAll()

      System.gc()

    }
  }

}
