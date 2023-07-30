package services.scalable.index.test

import com.github.benmanes.caffeine.cache.{Caffeine, RemovalCause, RemovalListener}
import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.{IndexContext, TemporalContext}
import services.scalable.index.impl._
import services.scalable.index.{Cache, Commands, DefaultComparators, DefaultPrinters, DefaultSerializers, IndexBuilder, QueryableIndex, TemporalIndex}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class TemporalIndexSpec extends Repeatable {

  override val times: Int = 1

  "operations" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)
        
    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Array[Byte]
    type V = Array[Byte]

    import services.scalable.index.DefaultComparators._

    val NUM_LEAF_ENTRIES = TestConfig.NUM_LEAF_ENTRIES
    val NUM_META_ENTRIES = TestConfig.NUM_META_ENTRIES

    val historyIndexId = TestConfig.DATABASE
    val indexId = UUID.randomUUID().toString

    import services.scalable.index.DefaultSerializers._

    implicit val storage = new MemoryStorage()
    //val session = TestHelper.createCassandraSession()
    //implicit val storage = new CassandraStorage(session, true)

    val tctx = Await.result(TestHelper.loadOrCreateTemporalIndex(TemporalContext(
      historyIndexId,
      IndexContext(historyIndexId, NUM_LEAF_ENTRIES, NUM_META_ENTRIES),
      IndexContext(s"$historyIndexId-history", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    )), Duration.Inf).get

    val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)

    val indexBuilder = IndexBuilder.create[K, V](DefaultComparators.bytesOrd, DefaultSerializers.bytesSerializer, DefaultSerializers.bytesSerializer)
      .storage(storage)
      .cache(cache)
      .serializer(grpcBytesBytesSerializer)
      .keyToStringConverter(DefaultPrinters.byteArrayToStringPrinter)

    val historyBuilder = IndexBuilder.create[Long, IndexContext](DefaultComparators.ordLong, DefaultSerializers.longSerializer, DefaultSerializers.indexContextSerializer)
      .storage(storage)
      .cache(cache)
      .serializer(DefaultSerializers.grpcLongIndexContextSerializer)

    val tcache = Caffeine.newBuilder()
      .maximumSize(1000)
      .removalListener((key: (String, Long), value: Option[QueryableIndex[K, V]], cause: RemovalCause) => {
        logger.debug(s"temporal index removal cause for time t = ${key}: ${cause}")
      })
      .build[(String, Long), Option[QueryableIndex[K, V]]]()

    var hDB = new TemporalIndex[K, V](tctx)(indexBuilder, historyBuilder, tcache)
    var data = Seq.empty[(K, V, Option[String])]
    var snapshots = Seq.empty[(Long, Seq[(K, V, Option[String])])]

    def insert(): Unit = {

      val hDBBackup = hDB
      val index = hDB.findIndex()
      val currentVersion = Some(index.ctx.id)

      val n = rand.nextInt(1, 100)
      var list = Seq.empty[Tuple3[K, V, Boolean]]

      for(i<-0 until n){
        val k =  RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)
        val v = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        if(!data.exists{case (k1, _, _) => bytesOrd.equiv(k, k1)} && !list.exists{case (k1, _, _) => bytesOrd.equiv(k, k1)}){
          list = list :+ (k, v, false)
        }
      }

      val cmds = Seq(
        Commands.Insert(indexId, list)
      )

      val result = Await.result(hDB.execute(cmds), Duration.Inf)

      assert(result.success)

      if (result.success) {
        data ++= list.map{case (k, v, _) => (k, v, currentVersion)}

        val tmp = System.nanoTime()
        val (_, bresult) = Await.result(hDB.snapshot(), Duration.Inf)

        assert(bresult.success)

        snapshots = snapshots :+ tmp -> data.sortBy(_._1)

        val newDescriptor = Await.result(hDB.save(), Duration.Inf)
        hDB = new TemporalIndex[K, V](newDescriptor)(indexBuilder, historyBuilder, tcache)

        return
      }

      hDB = hDBBackup
      result.error.get.printStackTrace()
    }

    def update(): Unit = {

      val hDBBackup = hDB
      val index = hDB.findIndex()
      val currentVersion = Some(index.ctx.id)

      val n = if (data.length >= 2) rand.nextInt(1, data.length) else 1
      val list = scala.util.Random.shuffle(data).slice(0, n).map { case (k, v, lv) =>
        (k, RandomStringUtils.randomAlphanumeric(10).getBytes(Charsets.UTF_8), lv)
      }

      val cmds = Seq(
        Commands.Update(indexId, list)
      )

      val result = Await.result(hDB.execute(cmds), Duration.Inf)

      if (result.success) {
        logger.debug(s"${Console.MAGENTA_B}UPDATED RIGHT LAST VERSION ${list.map { case (k, _, _) => indexBuilder.ks(k) }}...${Console.RESET}")

        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _, _) => bytesOrd.equiv(k, k1) } }
        data = data ++ list.map { case (k, v, _) => (k, v, currentVersion) }

        val tmp = System.nanoTime()
        val (_, bresult) = Await.result(hDB.snapshot(), Duration.Inf)

        assert(bresult.success)

        snapshots = snapshots :+ tmp -> data.sortBy(_._1)

        val newDescriptor = Await.result(hDB.save(), Duration.Inf)
        hDB = new TemporalIndex[K, V](newDescriptor)(indexBuilder, historyBuilder, tcache)

        return
      }

      hDB = hDBBackup
      result.error.get.printStackTrace()
      logger.debug(s"${Console.RED_B}UPDATED WRONG LAST VERSION ${list.map { case (k, _, _) => indexBuilder.ks(k) }}...${Console.RESET}")
    }

    def remove(): Unit = {

      val hDBBackup = hDB
      val index = hDB.findIndex()
      val currentVersion = Some(index.ctx.id)

      val n = if (data.length >= 2) rand.nextInt(1, data.length) else 1
      val list: Seq[Tuple2[K, Option[String]]] = scala.util.Random.shuffle(data).slice(0, n).map { case (k, _, lv) =>
        (k, lv)
      }

      val cmds = Seq(
        Commands.Remove[K, V](indexId, list)
      )

      val result = Await.result(hDB.execute(cmds), Duration.Inf)

      if (result.success) {
        logger.debug(s"${Console.RED_B}REMOVED RIGHT VERSION ${list.map { case (k, _) => indexBuilder.ks(k) }}...${Console.RESET}")
        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _) => bytesOrd.equiv(k, k1) } }

        val tmp = System.nanoTime()
        val (_, bresult) = Await.result(hDB.snapshot(), Duration.Inf)

        assert(bresult.success)

        snapshots = snapshots :+ tmp -> data.sortBy(_._1)

        val newDescriptor = Await.result(hDB.save(), Duration.Inf)
        hDB = new TemporalIndex[K, V](newDescriptor)(indexBuilder, historyBuilder, tcache)

        return
      }

      hDB = hDBBackup
      result.error.get.printStackTrace()
      logger.debug(s"${Console.RED_B}REMOVED WRONG VERSION ${list.map { case (k, _) => indexBuilder.ks(k) }}...${Console.RESET}")
    }

    val n = 100

    for(i<-0 until n){

      rand.nextInt(1, 4) match {
        case 1 => insert()
        case 2 if !data.isEmpty => update()
        case 3 if !data.isEmpty => remove()
        case _ => insert()
      }

    }

    val hdbCtxSaved = Await.result(hDB.save(), Duration.Inf)

    val hDBFromDisk = new TemporalIndex[K, V](hdbCtxSaved)(indexBuilder, historyBuilder, tcache)

    snapshots.foreach { case (tmp, data) =>
      val ldata = data.map{x => x._1 -> x._2}.toList

      val idx = Await.result(hDBFromDisk.findIndex(tmp), Duration.Inf).get
      val idata = Await.result(TestHelper.all(idx.inOrder()), Duration.Inf).map{x => x._1 -> x._2}

      logger.debug(s"${Console.GREEN_B}idata: ${idata.map { case (k, _) => indexBuilder.ks(k) }}${Console.RESET}\n")
      logger.debug(s"${Console.GREEN_B}ldata: ${ldata.map { case (k, _) => indexBuilder.ks(k) }}${Console.RESET}\n")

      assert(TestHelper.isColEqual(ldata, idata))
    }

    Await.result(storage.close(), Duration.Inf)
  }

}
