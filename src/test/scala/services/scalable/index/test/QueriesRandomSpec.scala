package services.scalable.index.test

import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import services.scalable.index.grpc._
import services.scalable.index.impl._
import services.scalable.index.{Commands, DefaultSerializers, IndexBuilder, QueryableIndex}

import java.util.UUID
import ch.qos.logback.classic.{Level, Logger}
import services.scalable.index.Commands.{Command, Insert, Remove, Update}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class QueriesRandomSpec extends Repeatable with Matchers {

  LoggerFactory.getLogger("services.scalable.index.Context").asInstanceOf[Logger].setLevel(Level.INFO)
  LoggerFactory.getLogger("services.scalable.index.impl.GrpcByteSerializer").asInstanceOf[Logger].setLevel(Level.INFO)

  override val times: Int = 1

  "queries" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = String
    type V = String

    import services.scalable.index.DefaultComparators._
    import services.scalable.index.DefaultSerializers._

    val version = UUID.randomUUID().toString

    val NUM_LEAF_ENTRIES = rand.nextInt(4, 64)
    val NUM_META_ENTRIES = rand.nextInt(4, 64)

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

    val rangeBuilder = IndexBuilder.create[K, V](global, ordering,
        indexContext.numLeafItems, indexContext.numMetaItems, indexContext.maxNItems,
        DefaultSerializers.stringSerializer, DefaultSerializers.stringSerializer)
      .storage(storage)
      .serializer(grpcStringStringSerializer)
    //.keyToStringConverter(DefaultPrinters.intToStringPrinter)
    .build()

   // var data = Seq.empty[(K, V, Option[String])]
    //var index = new QueryableIndex[K, V](indexContext)(builder)

    val prefixes = (0 until 10).map{_ => RandomStringUtils.randomAlphabetic(3).toLowerCase}
      .distinct.toList

    def insert(data: Seq[(K, V)], upsert: Boolean = false): (Boolean, Seq[Command[K, V]]) = {
      val n = rand.nextInt(100, 1000)
      var list = Seq.empty[(K, V, Boolean)]

      for(i<-0 until n){
        val k = RandomStringUtils.randomAlphabetic(10).toLowerCase()
        val v = k

        if(!data.exists{case (k1, _) => rangeBuilder.ord.equiv(k, k1)} && !list.exists{case (k1, _, _) =>
          rangeBuilder.ord.equiv(k, k1)}){
          list = list :+ (k, v, upsert)
        }
      }

      if(list.isEmpty) {
        println("no unique data to insert!")
        return true -> Seq.empty[Command[K, V]]
      }

      val insertDups = rand.nextInt(1, 100) % 7 == 0

      println(s"${Console.GREEN}INSERTING...${Console.RESET}")

      if(insertDups){
        list = list :+ list.head
      }

      !insertDups -> Seq(Insert(indexId, list, Some(version)))
    }

    def remove(data: Seq[(K, V)]): (Boolean, Seq[Command[K, V]]) = {

      if(data.isEmpty) {
        println("no data to remove! Index is empty already!")
        return true -> Seq.empty[Command[K, V]]
      }

      val keys = data.map(_._1)
      var toRemoveRandom = (if(keys.length > 1) scala.util.Random.shuffle(keys).slice(0, rand.nextInt(1, keys.length))
      else keys).map { _ -> Some(version)}

      val removalError = rand.nextInt(1, 100) match {
        case i if i % 3 == 0 =>
          val elem = toRemoveRandom(0)
          toRemoveRandom = toRemoveRandom :+ (elem._1 + "x" , elem._2)
          true

        case i if i % 5 == 0 =>
          val elem = toRemoveRandom(0)
          toRemoveRandom = toRemoveRandom :+ (elem._1 , Some(UUID.randomUUID().toString))
          true

        case _ => false
      }

      println(s"${Console.RED_B}REMOVING...${Console.RESET}")
      !removalError -> Seq(Remove(indexId, toRemoveRandom, Some(version)))
    }

    def update(data: Seq[(K, V)]): (Boolean, Seq[Command[K, V]]) = {
      if(data.isEmpty) {
        println("no data to update! Index is empty!")
        return true -> Seq.empty[Command[K, V]]
      }

      var toUpdateRandom = (if(data.length > 1) scala.util.Random.shuffle(data).slice(0, rand.nextInt(1, data.length))
      else data).map { case (k, v) => (k, RandomStringUtils.randomAlphabetic(10), Some(version))}

      val updateError = rand.nextInt(1, 100) match {
        case i if i % 3 == 0 =>
          val elem = toUpdateRandom(0)
          toUpdateRandom = toUpdateRandom :+ (elem._1 + "x" , elem._2, elem._3)
          true

        case i if i % 5 == 0 =>
          val elem = toUpdateRandom(0)
          toUpdateRandom = toUpdateRandom :+ (elem._1 , elem._2, Some(UUID.randomUUID().toString))
          true

        case _ => false
      }

      println(s"${Console.BLUE_B}UPDATING...${Console.RESET}")

      !updateError -> Seq(Update(indexId, toUpdateRandom, Some(version)))
    }

    var data = Seq.empty[(K, V, Option[String])]
    val runtimes = rand.nextInt(5, 100)

    for(j<-0 until runtimes){

      val ctx = Await.result(storage.loadIndex(indexId), Duration.Inf).get
      val index = new QueryableIndex[K, V](ctx)(rangeBuilder)
      var indexData = index.allSync().map(x => (x._1, x._2))

      println(s"indexData: ${indexData.length}")

      val nCommands = rand.nextInt(1, 100)
      var cmds = Seq.empty[Command[K, V]]

      for(i<-0 until nCommands){
        cmds ++= (rand.nextInt(1, 10000) match {
          case i if !indexData.isEmpty && i % 5 == 0 =>

            val (ok, cmds) = update(indexData)

            if(ok){
              val list = cmds(0).asInstanceOf[Update[K, V]].list
              indexData = indexData.filterNot{case (k, v) => list.exists{case (k1, _, _) => rangeBuilder.ord.equiv(k, k1)}}
              indexData = indexData ++ list.map(x => x._1 -> x._2)
            }

            cmds

          case i if !indexData.isEmpty && i % 3 == 0 =>

            val (ok, cmds) = remove(indexData)

            if(ok){
              val list = cmds(0).asInstanceOf[Remove[K, V]].keys
              indexData = indexData.filterNot{case (k, v) => list.exists{case (k1, _) => rangeBuilder.ord.equiv(k, k1)}}
            }

            cmds

          case _ =>
            val (ok, cmds) = insert(indexData)

            if(ok){
              val list = cmds(0).asInstanceOf[Insert[K, V]].list
              indexData = indexData ++ list.map(x => (x._1, x._2))
            }

            cmds
        })
      }

      val r0 = Await.result(index.execute(cmds, version), Duration.Inf)

      if(!r0.success){
        println(r0.error.get.getClass)
        //assert(false)
      } else {

        // When everything is ok (all or nothing) executes it...
        for(i<-0 until cmds.length){
          cmds(i) match {
            case cmd: Update[K, V] =>

              val list = cmd.list
              data = data.filterNot{case (k, v, _) => list.exists{case (k1, _, _) => rangeBuilder.ord.equiv(k, k1)}}
              data = data ++ list

            case cmd: Remove[K, V] =>

              val list = cmd.keys
              data = data.filterNot{case (k, v, _) => list.exists{case (k1, _) => rangeBuilder.ord.equiv(k, k1)}}

            case cmd: Insert[K, V] =>
              val list = cmd.list
              data = data ++ list.map{x => (x._1, x._2, Some(version))}
          }
        }

        Await.result(index.save(), Duration.Inf)
      }
    }

    data = data.sortBy(_._1)

    val ctx = Await.result(storage.loadIndex(indexId), Duration.Inf).get
    val index = new QueryableIndex[K, V](ctx)(rangeBuilder)

    val dlist = data.map{case (k, v, _) => k -> v}
    val ilist = Await.result(index.all(), Duration.Inf).map{case (k, v, _) => k -> v}

    logger.debug(s"${Console.GREEN_B}tdata: ${dlist.map{case (k, v) => rangeBuilder.ks(k) -> rangeBuilder.vs(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata: ${ilist.map{case (k, v) => rangeBuilder.ks(k) -> rangeBuilder.vs(v)}}${Console.RESET}\n")

    Await.result(storage.close(), Duration.Inf)

    assert(TestHelper.isColEqual(dlist, ilist))

    val suffixComp = new Ordering[K] {
      override def compare(k: K, term: K): Int = {
        val suffix = k.slice(3, k.length)

        ordering.compare(suffix, term)
      }
    }

    if(data.length > 1) {

      var (kFrom, _, _) = data(rand.nextInt(0, data.length - 1))
      var (kTo, _, _) = data.filterNot{x => x._1 == kFrom}(rand.nextInt(0, data.length - 2))

      if(kFrom > kTo) {
        val aux = kFrom
        kFrom = kTo
        kTo = aux
      }

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

        logger.debug(s"term: ${rangeBuilder.ks(term)}...")

        val itr = index.lt(term, inclusive, reverse)(termComp)
        val indexData = Await.result(index.all(itr), Duration.Inf).map(x => x._1 -> x._2).toList

        val cr = slice == indexData

        if (!cr) {
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

        if (!cr) {
          index.prettyPrint()
          println()
        }

        assert(cr)
      }*/

      var PREFROM = prefixFrom
      var PRETO = prefixTo
      var KFROM = kFrom
      var KTO = kTo

      if(rand.nextBoolean()){
        KFROM = KFROM.replace(KFROM(rand.nextInt(0, KFROM.length - 1)), RandomStringUtils.randomAlphabetic(1)(0))
      }

      if(rand.nextBoolean()){
        KTO = KTO.replace(KTO(rand.nextInt(0, KTO.length - 1)), RandomStringUtils.randomAlphabetic(1)(0))
      }

      if(KFROM > KTO){
        val aux = KFROM
        KFROM = KTO
        KTO = aux
      }

      if(rand.nextBoolean()){
        PREFROM = PREFROM.replace(PREFROM(rand.nextInt(0, PREFROM.length - 1)), RandomStringUtils.randomAlphabetic(1)(0))
      }

      if(rand.nextBoolean()){
        PRETO = PRETO.replace(PRETO(rand.nextInt(0, PRETO.length - 1)), RandomStringUtils.randomAlphabetic(1)(0))
      }

      if(PREFROM > PRETO){
        val aux = PREFROM
        PREFROM = PRETO
        PRETO = aux
      }

      testLtPrefix(PREFROM, PREFROM, rand.nextBoolean(), rand.nextBoolean())(prefixComp, ordering)
      testGtPrefix(PREFROM, PREFROM, rand.nextBoolean(), rand.nextBoolean())(prefixComp, ordering)

      testPrefix(prefixFrom, rand.nextBoolean())(prefixComp)
      testPrefix(prefixTo, rand.nextBoolean())(prefixComp)
      testGt(KFROM, rand.nextBoolean(), rand.nextBoolean())(ordering)
      testGtPrefix(prefixFrom, kFrom, rand.nextBoolean(), rand.nextBoolean())(prefixComp, ordering)
      testLt(KFROM, rand.nextBoolean(), rand.nextBoolean())(ordering)
      testLtPrefix(prefixFrom, kFrom, rand.nextBoolean(), rand.nextBoolean())(prefixComp, ordering)

      if (data.length > 1){
        testRange(KFROM, KTO, rand.nextBoolean(), rand.nextBoolean(), rand.nextBoolean())(ordering)
      }

      //testPrefixRange(PREFROM, PRETO, rand.nextBoolean(), rand.nextBoolean(), rand.nextBoolean())(prefixComp)

      logger.info(Await.result(index.save(), Duration.Inf).toString)

      index.ctx.clear()
      storage.close()
      cache.invalidateAll()

      System.gc()

    }
  }

}
