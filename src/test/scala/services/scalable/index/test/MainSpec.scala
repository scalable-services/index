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

class MainSpec extends Repeatable with Matchers {

  override val times: Int = 1

  "operations" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Bytes
    type V = Bytes

    import services.scalable.index.DefaultComparators._

    val NUM_LEAF_ENTRIES = rand.nextInt(4, 64)
    val NUM_META_ENTRIES = rand.nextInt(4, 64)

    val indexId = UUID.randomUUID().toString

    //val session = TestHelper.createCassandraSession()
    val storage = /*new CassandraStorage(session, true)*/new MemoryStorage()

    val indexContext = Await.result(TestHelper.loadOrCreateIndex(IndexContext(
      indexId,
      NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES,
      maxNItems = -1L
    ))(storage, global), Duration.Inf).get

    val builder = IndexBuilder.create[K, V](global, DefaultComparators.bytesOrd,
        indexContext.numLeafItems, indexContext.numMetaItems, indexContext.maxNItems,
        DefaultSerializers.bytesSerializer, DefaultSerializers.bytesSerializer)
      .storage(storage)
      .serializer(DefaultSerializers.grpcBytesBytesSerializer)
      .keyToStringConverter(DefaultPrinters.byteArrayToStringPrinter)
      .build()

    var data = Seq.empty[(K, V, Option[String])]
    var ctx: IndexContext = indexContext

    def insert(): Unit = {

      val currentVersion = Some(ctx.id)
      val index = new QueryableIndex[K, V](ctx)(builder)

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

            val k = RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
            val v = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

            if (!data.exists { case (k1, _, _) => bytesOrd.equiv(k, k1) } &&
              !list.exists { case (k1, _, _) => bytesOrd.equiv(k, k1) }) {
              list = list :+ (k, v, false)
            }
        }
      }

      //logger.debug(s"${Console.GREEN_B}INSERTING ${list.map{case (k, v, _) => builder.ks(k)}}${Console.RESET}")

      val cmds = Seq(
        Commands.Insert(indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if(result.success){
        logger.debug(s"${Console.GREEN_B}INSERTION OK: ${list.map{case (k, v, _) => builder.ks(k)}}${Console.RESET}")

        val newDescriptor = Await.result(index.save(), Duration.Inf)
        //index = new QueryableIndex[K, V](newDescriptor)(builder)
        ctx = newDescriptor

        data = data ++ list.map{case (k, v, _) => (k, v, currentVersion)}

        return
      }

      logger.debug(s"${Console.RED_B}INSERTION FAIL: ${list.map{case (k, v, _) => builder.ks(k)}}${Console.RESET}")

      result.error.get.printStackTrace()
    }

    def update(): Unit = {

      val currentVersion = Some(ctx.id)
      val index = new QueryableIndex[K, V](ctx)(builder)

      val introduceError = rand.nextBoolean()
      val errorTx = Some(UUID.randomUUID.toString)

      val n = if(data.length >= 2) rand.nextInt(1, data.length) else 1
      val list = scala.util.Random.shuffle(data).slice(0, n).map { case (k, v, lv) =>
        (k, RandomStringUtils.randomAlphanumeric(10).getBytes(Charsets.UTF_8),
         if(introduceError) errorTx else lv)
      }

      val cmds = Seq(
        Commands.Update(indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if(result.success){

        logger.debug(s"${Console.MAGENTA_B}UPDATED RIGHT LAST VERSION ${list.map{case (k, _, _) => builder.ks(k)}}...${Console.RESET}")

        val newDescriptor = Await.result(index.save(), Duration.Inf)
        ctx = newDescriptor

        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _, _) => bytesOrd.equiv(k, k1) } }
        data = data ++ list.map { case (k, v, _) => (k, v, currentVersion) }

        return
      }

      result.error.get.printStackTrace()
      logger.debug(s"${Console.CYAN_B}UPDATED WRONG LAST VERSION ${list.map { case (k, _, _) => builder.ks(k) }}...${Console.RESET}")
    }

    def remove(): Unit = {

      val currentVersion = Some(ctx.id)
      val index = new QueryableIndex[K, V](ctx)(builder)

      val introduceError = rand.nextBoolean()
      val errorTx = Some(UUID.randomUUID.toString)

      val n = if(data.length >= 2) rand.nextInt(1, data.length) else 1
      val list: Seq[Tuple2[K, Option[String]]] = scala.util.Random.shuffle(data).slice(0, n).map { case (k, _, lv) =>
        (k, if(introduceError) errorTx else lv)
      }

      val cmds = Seq(
        Commands.Remove[K, V](indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if(result.success){
        val newDescriptor = Await.result(index.save(), Duration.Inf)
        ctx = newDescriptor

        logger.debug(s"${Console.YELLOW_B}REMOVED RIGHT VERSION ${list.map { case (k, _) => builder.ks(k) }}...${Console.RESET}")
        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _) => bytesOrd.equiv(k, k1) } }

        return
      }

      result.error.get.printStackTrace()
      logger.debug(s"${Console.RED_B}REMOVED WRONG VERSION ${list.map { case (k, _) => builder.ks(k) }}...${Console.RESET}")
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

    val index = new QueryableIndex[K, V](ctx)(builder)
    logger.info(Await.result(index.save(), Duration.Inf).toString)

    val dlist = data.sortBy(_._1).map{case (k, v, _) => k -> v}
    val ilist = Await.result(index.all(), Duration.Inf).map{case (k, v, _) => k -> v}

    logger.debug(s"${Console.GREEN_B}tdata: ${dlist.map{case (k, v) => builder.ks(k) -> builder.vs(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata: ${ilist.map{case (k, v) => builder.ks(k) -> builder.vs(v)}}${Console.RESET}\n")

    Await.result(storage.close(), Duration.Inf)

    assert(TestHelper.isColEqual(dlist, ilist))
  }

}
