package services.scalable.index.test

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import services.scalable.index.grpc._
import services.scalable.index.impl._
import services.scalable.index.{Bytes, Commands, DefaultComparators, DefaultPrinters, DefaultSerializers, IndexBuilder, QueryableIndex}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.jdk.FutureConverters.CompletionStageOps

class CassandraSpec extends Repeatable with Matchers {

  override val times: Int = 1

  "operations" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Bytes
    type V = Bytes

    import services.scalable.index.DefaultComparators._

    val NUM_LEAF_ENTRIES = TestConfig.NUM_LEAF_ENTRIES
    val NUM_META_ENTRIES = TestConfig.NUM_META_ENTRIES

    val indexId = UUID.randomUUID().toString

    val session = TestHelper.createCassandraSession()
    val storage = new CassandraStorage(session, true)

    val builder = IndexBuilder.create[K, V](DefaultComparators.bytesOrd)
      .storage(storage)
      .serializer(DefaultSerializers.grpcBytesBytesSerializer)
      .keyToStringConverter(DefaultPrinters.byteArrayToStringPrinter)

    val indexContext = Await.result(TestHelper.loadOrCreateIndex(IndexContext(
      indexId,
      NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES
    ))(storage, global), Duration.Inf).get

    var data = Seq.empty[(K, V, Boolean)]
    var index = builder.build(indexContext)

    def insert(): Unit = {
      val n = rand.nextInt(1, 1000)
      var list = Seq.empty[Tuple3[K, V, Boolean]]

      for(i<-0 until n){
        val k = RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
        val v = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        if(!data.exists { case (k1, _, _) => bytesOrd.equiv(k, k1) } &&
          !list.exists { case (k1, _, _) => bytesOrd.equiv(k, k1) }){
          list = list :+ (k, v, false)
        }
      }

      logger.debug(s"${Console.GREEN_B}INSERTING ${list.map{case (k, v, _) => new String(k)}}${Console.RESET}")

      val cmds = Seq(
        Commands.Insert(indexId, list)
      )
      val result = Await.result(index.execute(cmds), Duration.Inf)

      assert(result.success)

      if(result.success){
        data = data ++ list
        return
      }

      result.error.get.printStackTrace()
    }

    def update(): Unit = {

      val lastVersion: Option[String] = rand.nextBoolean() match {
        case true => None
        case false => Some(UUID.randomUUID.toString)
      }

      val backupCtx = index.snapshot()

      val n = if(data.length >= 2) rand.nextInt(1, data.length) else 1
      val list = scala.util.Random.shuffle(data).slice(0, n).map { case (k, v, _) =>
        (k, RandomStringUtils.randomAlphanumeric(10).getBytes(Charsets.UTF_8), lastVersion)
      }

      val cmds = Seq(
        Commands.Update(indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if(result.success){
        logger.debug(s"${Console.MAGENTA_B}UPDATED RIGHT LAST VERSION ${list.map{case (k, _, _) => new String(k)}}...${Console.RESET}")

        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _, _) => bytesOrd.equiv(k, k1) } }
        data = data ++ list.map { case (k, v, _) => (k, v, true) }

        return
      }

      result.error.get.printStackTrace()
      logger.debug(s"${Console.RED_B}UPDATED WRONG LAST VERSION ${list.map { case (k, _, _) => new String(k) }}...${Console.RESET}")
      index = new QueryableIndex[K, V](backupCtx)(builder)
    }

    def remove(): Unit = {

      val lastVersion: Option[String] = rand.nextBoolean() match {
        case true => None
        case false => Some(UUID.randomUUID.toString)
      }

      val backupCtx = index.snapshot()

      val n = if(data.length >= 2) rand.nextInt(1, data.length) else 1
      val list: Seq[Tuple2[K, Option[String]]] = scala.util.Random.shuffle(data).slice(0, n).map { case (k, _, _) =>
        (k, lastVersion)
      }

      val cmds = Seq(
        Commands.Remove[K, V](indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if(result.success){
        logger.debug(s"${Console.RED_B}REMOVED RIGHT VERSION ${list.map { case (k, _) => new String(k) }}...${Console.RESET}")
        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _) => bytesOrd.equiv(k, k1) } }
        return
      }

      result.error.get.printStackTrace()
      logger.debug(s"${Console.RED_B}REMOVED WRONG VERSION ${list.map { case (k, _) => new String(k) }}...${Console.RESET}")
      index = new QueryableIndex[K, V](backupCtx)(builder)
    }

    def loadFromDisk(): QueryableIndex[K, V] = {
      val session = TestHelper.createCassandraSession()
      val storage = new CassandraStorage(session, false)
      val builderDisk = IndexBuilder.create[K, V](DefaultComparators.bytesOrd)
        .storage(storage)
        .serializer(DefaultSerializers.grpcBytesBytesSerializer)
        .keyToStringConverter(DefaultPrinters.byteArrayToStringPrinter)

      val indexContextFromDisk = Await.result(TestHelper.loadIndex(indexId)(storage, global), Duration.Inf).get
      new QueryableIndex[K, V](indexContextFromDisk)(builderDisk)
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

    logger.info(Await.result(index.save(true), Duration.Inf).toString)

    val dlist = data.sortBy(_._1).map{case (k, v, _) => k -> v}

    // Tries to load from disk to check against it...
    val indexFromDisk = loadFromDisk()
    val ilist = Await.result(TestHelper.all(indexFromDisk.inOrder()), Duration.Inf).map{case (k, v, _) => k -> v}

    logger.debug(s"${Console.GREEN_B}tdata: ${dlist.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata: ${ilist.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

    Await.result(storage.close().flatMap(_ => indexFromDisk.builder.storage.close()), Duration.Inf)

    assert(TestHelper.isColEqual(dlist, ilist))
  }

}