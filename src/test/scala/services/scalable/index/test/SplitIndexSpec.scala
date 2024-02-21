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

class SplitIndexSpec extends Repeatable with Matchers {

  override val times: Int = 10

  "operations" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Bytes
    type V = Bytes

    import services.scalable.index.DefaultComparators._

    val NUM_LEAF_ENTRIES = rand.nextInt(4, 32)
    val NUM_META_ENTRIES = rand.nextInt(4, 32)

    val indexId = UUID.randomUUID().toString

    val session = TestHelper.createCassandraSession()
    val storage = new CassandraStorage(session, true)//new MemoryStorage()

    val MAX_ITEMS = 300

    val indexContext = Await.result(TestHelper.loadOrCreateIndex(IndexContext(
      indexId,
      NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES,
      maxNItems = MAX_ITEMS
    ))(storage, global), Duration.Inf).get

    val builder = IndexBuilder.create[K, V](global, DefaultComparators.bytesOrd,
       indexContext.numLeafItems, indexContext.numMetaItems, indexContext.maxNItems,
        DefaultSerializers.bytesSerializer, DefaultSerializers.bytesSerializer)
      .storage(storage)
      .serializer(DefaultSerializers.grpcBytesBytesSerializer)
      .keyToStringConverter(DefaultPrinters.byteArrayToStringPrinter)
      .valueToStringConverter(DefaultPrinters.byteArrayToStringPrinter)
      .build()

    val version = UUID.randomUUID.toString

    var data = Seq.empty[(K, V, String)]
    val index = new QueryableIndex[K, V](indexContext)(builder)

    def insert(): Seq[Commands.Command[K, V]] = {
      val n = rand.nextInt(1, 1000)
      var list = Seq.empty[Tuple3[K, V, Boolean]]

      for(i<-0 until n){
        val k = RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
        val v = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        if (!data.exists { case (k1, _, _) => bytesOrd.equiv(k, k1) } &&
          !list.exists { case (k1, _, _) => bytesOrd.equiv(k, k1) }) {
          list = list :+ (k, v, false)
        }
      }

      data ++= list.map{case (k, v, _) => (k, v, version)}

      Seq(Commands.Insert(indexId, list))
    }

    def update(): Seq[Commands.Command[K, V]] = {
      val n = if(data.length >= 2) rand.nextInt(1, data.length) else 1
      val list = scala.util.Random.shuffle(data).slice(0, n).map { case (k, _, vs) =>
        (k, RandomStringUtils.randomAlphanumeric(10).getBytes(Charsets.UTF_8), Some(vs))
      }

      data = data.filterNot{case (k, _, _) => list.exists{case (k1, _, _) => builder.ord.equiv(k, k1)}}
      data ++= list.map{case (k, v, _) => (k, v, version)}

      Seq(Commands.Update(indexId, list))
    }

    def remove(): Seq[Commands.Command[K, V]] = {
      val n = if(data.length >= 2) rand.nextInt(1, data.length) else 1
      val list: Seq[Tuple2[K, Option[String]]] = scala.util.Random.shuffle(data).slice(0, n)
        .map { case (k, _, vs) => k -> Some(vs)}

      data = data.filterNot{case (k, _, _) => list.exists{case (k1, _) => builder.ord.equiv(k, k1)}}

      Seq(Commands.Remove[K, V](indexId, list))
    }

    val n = 100
    var cmds = Seq.empty[Commands.Command[K, V]]

    for(i<-0 until n){
      cmds ++= (rand.nextInt(1, 4) match {
        case 1 => insert()
        case 2 if !data.isEmpty => update()
        case 3 if !data.isEmpty => remove()
        case _ => insert()
      })
    }

    val result = Await.result(index.execute(cmds, version), Duration.Inf)

    assert(result.success, result.error)

     //index.copy()

    val dlist = data.sortBy(_._1).map{case (k, v, _) => k -> v}
    val ilist = Await.result(index.all(), Duration.Inf).map{case (k, v, _) => k -> v}

    logger.debug(s"${Console.GREEN_B}tdata [${dlist.length}]: ${dlist.map{case (k, v) => builder.ks(k) -> builder.vs(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata [${ilist.length}]: ${ilist.map{case (k, v) => builder.ks(k) -> builder.vs(v)}}${Console.RESET}\n")

    assert(TestHelper.isColEqual(dlist, ilist))

    val savedCtx = Await.result(index.save(), Duration.Inf)

    logger.info(savedCtx.toString)

    val copy = new QueryableIndex[K, V](savedCtx)(builder)

    if(copy.isFull()){
      logger.info(s"splitting index...")

      val right = Await.result(copy.split(), Duration.Inf)

      val leftList = Await.result(copy.all(), Duration.Inf).map { case (k, v, _) => k -> v }
      val rightList = Await.result(right.all(), Duration.Inf).map { case (k, v, _) => k -> v }

      val mergeSplits = leftList ++ rightList

      assert(TestHelper.isColEqual(mergeSplits, ilist))
    }

    Await.result(session.closeAsync().asScala, Duration.Inf)

  }

}
