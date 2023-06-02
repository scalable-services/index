package services.scalable.index.test

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.{IndexContext, TemporalContext}
import services.scalable.index.impl._
import services.scalable.index.{Commands, DefaultComparators, DefaultPrinters, DefaultSerializers, IndexBuilder, QueryableIndex, TemporalIndex}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class TemporalIndexSpec2 extends Repeatable {

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

    //implicit val storage = new MemoryStorage()

    val session = TestHelper.createCassandraSession()
    implicit val storage = new CassandraStorage(session, true)

    val tctx = Await.result(TestHelper.loadOrCreateTemporalIndex(TemporalContext(
      historyIndexId,
      IndexContext(historyIndexId, NUM_LEAF_ENTRIES, NUM_META_ENTRIES),
      IndexContext(s"$historyIndexId-history", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    )), Duration.Inf).get

    val indexBuilder = IndexBuilder.create[K, V](DefaultComparators.bytesOrd)
      .storage(storage)
      .serializer(grpcBytesBytesSerializer)
      .keyToStringConverter(DefaultPrinters.byteArrayToStringPrinter)

    val historyBuilder = IndexBuilder.create[Long, IndexContext](DefaultComparators.ordLong)
      .storage(storage)
      .serializer(DefaultSerializers.grpcLongIndexContextSerializer)

    val hDB = new TemporalIndex[K, V](tctx)(indexBuilder, historyBuilder)
    var data = Seq.empty[(K, V, Boolean)]
    var snapshots = Seq.empty[(Long, Seq[(K, V, Boolean)])]

    def insert(): Unit = {
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
        data ++= list

        val tmp = System.nanoTime()
        val (_, bresult) = Await.result(hDB.snapshot(), Duration.Inf)

        assert(bresult.success)

        snapshots = snapshots :+ tmp -> data.sortBy(_._1)

        return
      }

      result.error.get.printStackTrace()
    }

    def update(): Unit = {

      val lastVersion: Option[String] = rand.nextBoolean() match {
        case true => None
        case false => Some(UUID.randomUUID.toString)
      }

      val index = hDB.findIndex()
      index.beginTx()
      //val backupCtx = index.snapshot()

      val n = if (data.length >= 2) rand.nextInt(1, data.length) else 1
      val list = scala.util.Random.shuffle(data).slice(0, n).map { case (k, v, _) =>
        (k, RandomStringUtils.randomAlphanumeric(10).getBytes(Charsets.UTF_8), lastVersion)
      }

      val cmds = Seq(
        Commands.Update(indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if (result.success) {
        logger.debug(s"${Console.MAGENTA_B}UPDATED RIGHT LAST VERSION ${list.map { case (k, _, _) => new String(k) }}...${Console.RESET}")

        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _, _) => bytesOrd.equiv(k, k1) } }
        data = data ++ list.map { case (k, v, _) => (k, v, true) }

        index.commitTx()

        return
      }

      result.error.get.printStackTrace()
      logger.debug(s"${Console.RED_B}UPDATED WRONG LAST VERSION ${list.map { case (k, _, _) => new String(k) }}...${Console.RESET}")
      //index = new QueryableIndex[K, V](backupCtx)(indexBuilder)

      index.rollback()
    }

    def remove(): Unit = {

      val lastVersion: Option[String] = rand.nextBoolean() match {
        case true => None
        case false => Some(UUID.randomUUID.toString)
      }

      val index = hDB.findIndex()
      index.beginTx()
      //val backupCtx = index.snapshot()

      val n = if (data.length >= 2) rand.nextInt(1, data.length) else 1
      val list: Seq[Tuple2[K, Option[String]]] = scala.util.Random.shuffle(data).slice(0, n).map { case (k, _, _) =>
        (k, lastVersion)
      }

      val cmds = Seq(
        Commands.Remove[K, V](indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if (result.success) {
        logger.debug(s"${Console.RED_B}REMOVED RIGHT VERSION ${list.map { case (k, _) => new String(k) }}...${Console.RESET}")
        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _) => bytesOrd.equiv(k, k1) } }

        index.commitTx()

        return
      }

      result.error.get.printStackTrace()
      logger.debug(s"${Console.RED_B}REMOVED WRONG VERSION ${list.map { case (k, _) => new String(k) }}...${Console.RESET}")
      //index = new QueryableIndex[K, V](backupCtx)(builder)

      index.rollback()
    }

    /*var result = Await.result(hDB.execute(insert()), Duration.Inf)

    // Explicitly save snapshot
    val snapshot0 = data.sortBy(_._1)
    Await.result(hDB.snapshot(), Duration.Inf)

    result = Await.result(hDB.execute(insert()), Duration.Inf)

    // Explicitly save snapshot
    val snapshot1 = data.sortBy(_._1)
    Await.result(hDB.snapshot(), Duration.Inf)

    logger.info(s"\n${Console.MAGENTA_B}result: ${result}${Console.RESET}\n")*/

    val n = 10

    for(i<-0 until n){

      1 match {
        case 1 => insert()
        /*case 2 if !data.isEmpty => update()
        case 3 if !data.isEmpty => remove()
        case _ => insert()*/
      }

    }

    val hdbCtxSaved = Await.result(hDB.save(), Duration.Inf)

    val hDBFromDisk = new TemporalIndex[K, V](hdbCtxSaved)(indexBuilder, historyBuilder)

    snapshots.foreach { case (tmp, data) =>
      val ldata = data.map{x => x._1 -> x._2}.toList

      val idx = Await.result(hDBFromDisk.findIndex(tmp), Duration.Inf).get
      val idata = Await.result(TestHelper.all(idx.inOrder()), Duration.Inf).map{x => x._1 -> x._2}

      logger.debug(s"${Console.GREEN_B}idata: ${idata.map { case (k, v) => indexBuilder.ks(k) }}${Console.RESET}\n")
      logger.debug(s"${Console.GREEN_B}ldata: ${ldata.map { case (k, v) => indexBuilder.ks(k) }}${Console.RESET}\n")

      assert(TestHelper.isColEqual(ldata, idata))
    }

    Await.result(storage.close(), Duration.Inf)
  }

}
