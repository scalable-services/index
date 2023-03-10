package services.scalable.index

import com.google.common.base.Charsets
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc._
import services.scalable.index.grpc.tuple._
import services.scalable.index.impl._

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Random

class LogicalDeletionSpec extends Repeatable {

  override val times: Int = 1

  "operations" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = EAVT
    type V = Bytes

    import services.scalable.index.DefaultComparators._

    val NUM_LEAF_ENTRIES = 8//rand.nextInt(5, 64)
    val NUM_META_ENTRIES = 8//rand.nextInt(5, 64)

    val indexId = "mysusindex"//UUID.randomUUID().toString

    import services.scalable.index.DefaultSerializers._

    implicit val eavtOrderInsertion = new Ordering[EAVT] {
      override def compare(x: EAVT, y: EAVT): Int = {
        var c = x.e.compareTo(y.e)
        if(c != 0) return c

        c = x.a.compareTo(y.a)

        if(c != 0) return c

        c = ord.compare(x.v.toByteArray, y.v.toByteArray)

        if(c != 0) return c

        c = x.enabled.compareTo(y.enabled)

        if(c != 0) return c

        x.t.compareTo(y.t)
      }
    }

    implicit val eavtSerializer = new Serializer[EAVT] {
      override def serialize(t: EAVT): Bytes = Any.pack(t).toByteArray
      override def deserialize(b: Bytes): EAVT = Any.parseFrom(b).unpack(EAVT)
    }

    implicit val grpcEAVTBytesContextSerializer = new GrpcByteSerializer[EAVT, Bytes]()

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

    var data = Seq.empty[(K, V)]
    val index = new QueryableIndex[K, V](indexContext)

    def insert(): Unit = {
      val n = 100//rand.nextInt(1, 100)
      var list = Seq.empty[Tuple2[K, V]]

      for(i<-0 until n){
        val k = RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
        val v = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        val uuid = UUID.randomUUID.toString()
        val now = System.nanoTime()

        val eavt = EAVT()
          .withE(uuid)
          .withA("name")
          .withV(ByteString.copyFrom(k))
          .withT(now)
          .withEnabled(true)

        if(!data.exists { case (k1, _) => eavtOrderInsertion.equiv(eavt, k1) } &&
          !list.exists { case (k1, _) => eavtOrderInsertion.equiv(eavt, k1) }){
          list = list :+ (eavt -> Array.empty[Byte])
        }
      }

      val cmds = Seq(
        Commands.Insert(indexId, list)
      )
      val result = Await.result(index.execute(cmds), Duration.Inf)

      //index.snapshot()

      assert(result)

      if(result){
        data = data ++ list
      }
    }

    insert()
    //insert()

    val colorFact = EAVT()
      .withE("lucaskey1")
      .withA("color")
      .withV(ByteString.copyFrom("green".getBytes(Charsets.UTF_8)))
      .withT(System.nanoTime())
      .withEnabled(true) -> Array.empty[Byte]

    //data = data :+ colorFact

    Await.result(index.execute(Seq(
      Commands.Insert(indexId, Seq(
        colorFact
      ))
    )), Duration.Inf)

    val remove = Random.shuffle(data).slice(0, 5).map(_._1)

    println(s"data len before: ${data.length}")

    data = data.filterNot { case (k, v) =>
      remove.exists{case k1 => eavtOrderInsertion.equiv(k, k1)}
    }

    println(s"data len after: ${data.length}")

    Await.result(index.update(remove.map { k =>
      (k.withEnabled(true), Array.empty[Byte], index.ctx.id)
    }, index.ctx.id, x => (x._1.withEnabled(false), x._2, x._3)), Duration.Inf)

    val colorFact2 = EAVT()
      .withE("lucaskey1")
      .withA("color")
      .withV(ByteString.copyFrom("blue".getBytes(Charsets.UTF_8)))
      .withT(System.nanoTime())
      .withEnabled(true) -> Array.empty[Byte]

    Await.result(index.update(Seq(
      (colorFact._1, colorFact._2, index.ctx.id)
    ), index.ctx.id, x => (x._1.withEnabled(false), x._2, x._3)), Duration.Inf)

    Await.result(index.insert(Seq(
      colorFact2
    )), Duration.Inf)

    data = data :+ colorFact2

    logger.info(Await.result(index.save(), Duration.Inf).toString)

    val dlist = data.sortBy(_._1).toList
    val ilist = Await.result(TestHelper.all(index.inOrder(k => k._1.enabled)), Duration.Inf).map{case (k, v, _) => k -> v}.toList

    logger.debug(s"${Console.GREEN_B}tdata: ${dlist.map{case (k, v) => k.e -> new String(k.v.toByteArray, Charsets.UTF_8)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata: ${ilist.map{case (k, v) => k.e -> new String(k.v.toByteArray, Charsets.UTF_8)}}${Console.RESET}\n")

    println(s"sizes: dlist: ${dlist.length} idata: ${ilist.length}")

    assert(TestHelper.isColEqual(dlist, ilist))
  }

}
