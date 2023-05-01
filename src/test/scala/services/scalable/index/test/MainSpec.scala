package services.scalable.index.test

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import services.scalable.index.grpc._
import services.scalable.index.impl._
import services.scalable.index.{Bytes, Commands, Context, IdGenerator, QueryableIndex}

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

    val indexId = "mysusindex"//UUID.randomUUID().toString

    import services.scalable.index.DefaultSerializers._
    import services.scalable.index.DefaultPrinters._

    implicit val idGenerator = new IdGenerator {
      override def generateId[K, V](ctx: Context[K, V]): String = UUID.randomUUID.toString
      override def generatePartition[K, V](ctx: Context[K, V]): String = "p0"
    }

    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new MemoryStorage()
    //implicit val storage = new CassandraStorage(TestConfig.KEYSPACE, false)

    val indexContext = Await.result(TestHelper.loadOrCreateIndex(IndexContext(
      indexId,
      NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES
    )), Duration.Inf).get

    var data = Seq.empty[(K, V, Boolean)]
    var index = new QueryableIndex[K, V](indexContext)

    def insert(): Unit = {
      val n = rand.nextInt(1, 1000)
      var list = Seq.empty[Tuple3[K, V, Boolean]]

      for(i<-0 until n){
        val k = RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
        val v = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        if(!data.exists { case (k1, _, _) => ord.equiv(k, k1) } &&
          !list.exists { case (k1, _, _) => ord.equiv(k, k1) }){
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

        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _, _) => ord.equiv(k, k1) } }
        data = data ++ list.map { case (k, v, _) => (k, v, true) }

        return
      }

      result.error.get.printStackTrace()
      logger.debug(s"${Console.RED_B}UPDATED WRONG LAST VERSION ${list.map { case (k, _, _) => new String(k) }}...${Console.RESET}")
      index = new QueryableIndex[K, V](backupCtx)
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
        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _) => ord.equiv(k, k1) } }
        return
      }

      result.error.get.printStackTrace()
      logger.debug(s"${Console.RED_B}REMOVED WRONG VERSION ${list.map { case (k, _) => new String(k) }}...${Console.RESET}")
      index = new QueryableIndex[K, V](backupCtx)
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

    logger.info(Await.result(index.save(), Duration.Inf).toString)

    val dlist = data.sortBy(_._1).map{case (k, v, _) => k -> v}
    val ilist = Await.result(TestHelper.all(index.inOrder()), Duration.Inf).map{case (k, v, _) => k -> v}

    logger.debug(s"${Console.GREEN_B}tdata: ${dlist.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata: ${ilist.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

    assert(TestHelper.isColEqual(dlist, ilist))
  }

}