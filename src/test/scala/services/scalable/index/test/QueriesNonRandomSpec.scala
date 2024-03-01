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

class QueriesNonRandomSpec extends Repeatable with Matchers {

  LoggerFactory.getLogger("services.scalable.index.Context").asInstanceOf[Logger].setLevel(Level.INFO)
  LoggerFactory.getLogger("services.scalable.index.impl.GrpcByteSerializer").asInstanceOf[Logger].setLevel(Level.INFO)

  override val times: Int = 10000

  "queries" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = String
    type V = String

    import services.scalable.index.DefaultComparators._
    import services.scalable.index.DefaultSerializers._

    val NUM_LEAF_ENTRIES = rand.nextInt(4, 16)
    val NUM_META_ENTRIES = rand.nextInt(4, 16)

    val indexId = UUID.randomUUID().toString

    val storage = new MemoryStorage()
    val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)

    implicit val grpcStringStringSerializer = new GrpcByteSerializer[String, String]()

    val ordering = ordString

    val indexContext = Await.result(TestHelper.loadOrCreateIndex(IndexContext(
      indexId,
      NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES,
      maxNItems = -1L
    ))(storage, global), Duration.Inf).get

    val builder = IndexBuilder.create[K, V](global, ordering,
        indexContext.numLeafItems, indexContext.numMetaItems, indexContext.maxNItems,
        DefaultSerializers.stringSerializer, DefaultSerializers.stringSerializer)
      .storage(storage)
      .cache(cache)
      .serializer(grpcStringStringSerializer)
    //.keyToStringConverter(DefaultPrinters.intToStringPrinter)
    .build()


    var data = Seq.empty[(K, V, String)]
    var index = new QueryableIndex[K, V](indexContext)(builder)

    val prefixes = Seq("a", "e", "i", "o", "u")
    val prefixes1 = Seq("b", "c", "d", "f", "g")

    for(prefix  <- prefixes){
      for(i<-10 to 99){
        val word = s"$prefix${i}"
        data = data :+ (word, word, "v1")
      }
    }

    val r = Await.result(index.execute(Seq(Commands.Insert("demo", data.map{x => (x._1, x._2, false)})), "v1"), Duration.Inf)

    assert(r.success)

    val indexData = Await.result(index.all(), Duration.Inf)

    assert(indexData == data)

    val idx0 = rand.nextInt(0, prefixes.length - 1)
    val v0 = prefixes(idx0)
    val v1 = prefixes(rand.nextInt(idx0 + 1, prefixes.length))

    val c0 = prefixes1(idx0)
    val c1 = prefixes1(rand.nextInt(idx0 + 1, prefixes1.length))

    val prefixComp = new Ordering[K] {
      override def compare(x: K, prefix: K): Int = {
        val px = x.slice(0, 1)
        ordering.compare(px, prefix)
      }
    }

    def testPrefix(prefix: K, reverse: Boolean)(prefixComp: Ordering[K]): Unit = {
      var slice = data.filter{case (k, _, _) => prefixComp.equiv(k, prefix)}.map{x => x._1 -> x._2}.toList
      slice = if (reverse) slice.reverse else slice

      val itr = index.prefix(prefix, reverse)(prefixComp)
      val indexData = Await.result(index.all(itr), Duration.Inf).map(x => x._1 -> x._2).toList

      val cr = slice == indexData

      if (!cr) {
        index.prettyPrint()
        println()
      }

      assert(cr)
    }

    /*def testPrefixRange(fromPrefix: K, toPrefix: K, fromInclusive: Boolean, toInclusive: Boolean,
                        reverse: Boolean)(prefixComp: Ordering[K]): Unit = {
      var slice = data.filter{case (k, _, _) =>

        ((fromInclusive && prefixComp.gteq(k, fromPrefix)) || prefixComp.gt(k, fromPrefix)) &&
          ((toInclusive && prefixComp.lteq(k, toPrefix)) || prefixComp.lt(k, toPrefix))

      }.map{x => x._1 -> x._2}.toList

      slice = if (reverse) slice.reverse else slice

      val itr = index.prefixRange(fromPrefix, toPrefix, fromInclusive, toInclusive, reverse)(prefixComp)
      val indexData = Await.result(index.all(itr), Duration.Inf).map(x => x._1 -> x._2).toList

      val cr = slice == indexData

      println(s"slice: ${slice.map(_._1)}")
      println(s"index: ${indexData.map(_._1)}")

      if (!cr) {
        index.prettyPrint()
        println()
      }

      assert(cr)
    }*/

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
        index.prettyPrint()
        println()
      }

      assert(cr)
    }

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
        index.prettyPrint()
        println()
      }

      assert(cr)
    }

    def testGtPrefix(prefix: K, term: K, inclusive: Boolean, reverse: Boolean)(prefixComp: Ordering[K], termComp: Ordering[K]): Unit = {
      var idx = data.reverse.indexWhere { case (k, _, _) =>
        prefixComp.gteq(k, prefix) && (inclusive && termComp.gteq(k, term) || termComp.gt(k, term))
      }

      idx = if (idx < 0) 0 else idx

      var slice = if (idx >= 0) data.slice(idx, data.length).filter { x =>
        prefixComp.equiv(x._1, prefix) &&
          (inclusive && termComp.gteq(x._1, term) || termComp.gt(x._1, term))
      }.map { x => x._1 -> x._2 } else Seq.empty[(K, V)]

      slice = if (reverse) slice.reverse else slice

      val itr = index.gt(prefix, term, inclusive, reverse)(prefixComp, termComp)
      val indexData = Await.result(index.all(itr), Duration.Inf).map(x => x._1 -> x._2).toList

      val cr = slice == indexData

      if (!cr) {
        index.prettyPrint()
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
        index.prettyPrint()
        println()
      }

      assert(cr)
    }

    def testLtPrefix(prefix: K, term: K, inclusive: Boolean, reverse: Boolean)(prefixComp: Ordering[K], termComp: Ordering[K]): Unit = {
      var idx = data.indexWhere { case (k, _, _) =>
        prefixComp.lteq(k, prefix) && (inclusive && termComp.lteq(k, term) || termComp.lt(k, term))
      }

      idx = if (idx < 0) 0 else idx

      var slice = if (idx >= 0) data.slice(idx, data.length).filter { x =>
        prefixComp.equiv(x._1, prefix) &&
          (inclusive && termComp.lteq(x._1, term) || termComp.lt(x._1, term))
      }.map { x => x._1 -> x._2 } else Seq.empty[(K, V)]

      slice = if (reverse) slice.reverse else slice

      val itr = index.lt(prefix, term, inclusive, reverse)(prefixComp, termComp)
      val indexData = Await.result(index.all(itr), Duration.Inf).map(x => x._1 -> x._2).toList

      val cr = slice == indexData

      if (!cr) {
        index.prettyPrint()
        println()
      }

      assert(cr)
    }

    val p0 = if(rand.nextBoolean()) c0 else v0
    val p1 = if(rand.nextBoolean()) c1 else v1

    val prefixInverted = prefixComp.compare(p0, p1) > 0

    val widx0 = rand.nextInt(0, data.length - 1)
    val w0 = data(widx0)._1
    val w1 = data(rand.nextInt(widx0 + 1, data.length))._1

    val makeItEqual = rand.nextBoolean()

    val W0 = if(rand.nextBoolean()) w0.replaceFirst(w0(0).toString, RandomStringUtils.randomAlphabetic(1)(0).toString)
      else w0
    val W1 = if(makeItEqual) W0 else if(rand.nextBoolean()) w1.replaceFirst(w1(0).toString, RandomStringUtils.randomAlphabetic(1)(0).toString)
      else w1

    testLt(W0, rand.nextBoolean(), rand.nextBoolean())(ordString)
    testGt(W0, rand.nextBoolean(), rand.nextBoolean())(ordString)

    testLtPrefix(if(rand.nextBoolean()) w1 else w1(0).toString, w1, rand.nextBoolean(), rand.nextBoolean())(prefixComp, ordString)
    testGtPrefix(if(rand.nextBoolean()) w0 else w0(0).toString, w0, rand.nextBoolean(), rand.nextBoolean())(prefixComp, ordString)

    val termInverted = ordString.compare(W0, W1) > 0
    testRange(if(termInverted) W1 else W0, if(termInverted) W0 else W1, rand.nextBoolean(), rand.nextBoolean(), rand.nextBoolean())(ordString)

    testPrefix(p0, rand.nextBoolean())(prefixComp)

    //testPrefixRange(if(prefixInverted) p1 else p0, if(prefixInverted) p0 else p1, rand.nextBoolean(), rand.nextBoolean(),
      //rand.nextBoolean())(prefixComp)

    index.ctx.clear()
    storage.close()
    cache.invalidateAll()

    System.gc()
  }

}
