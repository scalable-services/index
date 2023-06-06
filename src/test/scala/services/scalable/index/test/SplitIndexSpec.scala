package services.scalable.index.test

import org.apache.commons.lang3.RandomStringUtils
import services.scalable.index.DefaultComparators.bytesOrd
import services.scalable.index.DefaultSerializers._
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl._
import services.scalable.index.{Block, Bytes, Commands, Context, DefaultComparators, DefaultSerializers, IdGenerator, IndexBuilder, QueryableIndex}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import services.scalable.index.DefaultPrinters._

class SplitIndexSpec extends Repeatable {

  override val times: Int = 1

  "operations" should " run successfully" in {

    val rand = ThreadLocalRandom.current()

    type K = Bytes
    type V = Bytes

    val NUM_LEAF_ENTRIES = 8
    val NUM_META_ENTRIES = 8

    implicit val idGenerator = new IdGenerator {
      override def generateId[K, V](ctx: Context[K, V]): String = UUID.randomUUID.toString

      override def generatePartition[K, V](ctx: Context[K, V]): String = "p0"
    }

    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new MemoryStorage()
    val session = TestHelper.createCassandraSession()
    implicit val storage = new CassandraStorage(session, false)

    val MAX_ITEMS = 250

    val ctx = Await.result(TestHelper.loadOrCreateIndex(IndexContext("test-index", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)),
      Duration.Inf).get
      .withMaxNItems(MAX_ITEMS)
      .withNumLeafItems(NUM_LEAF_ENTRIES)
      .withNumMetaItems(NUM_META_ENTRIES)

    val indexBuilder = IndexBuilder.create[K, V](DefaultComparators.bytesOrd)
      .storage(storage)
      .serializer(DefaultSerializers.grpcBytesBytesSerializer)

    val index = new QueryableIndex[K, V](ctx)(indexBuilder)

    var data = Seq.empty[(Bytes, Bytes)]

    def insert(): Commands.Command[K, V] = {
      val n = 3000//rand.nextInt(1000, 2000) //rand.nextInt(1, 1000)

      var list = Seq.empty[(Bytes, Bytes, Boolean)]

      for (i <- 0 until n) {
        val k = RandomStringUtils.randomAlphanumeric(7).getBytes("UTF-8")

        if (!data.exists(x => bytesOrd.equiv(k, x._1)) && !list.exists { case (k1, v, _) => bytesOrd.equiv(k, k1) }) {
          data = data :+ k -> k
          list = list :+ (k, k, false)
        }
      }

      Commands.Insert("main", list)
    }

    val cmds = Seq[Commands.Command[K, V]](insert())

    val n = Await.result(index.execute(cmds), Duration.Inf)

    //val savedMetaContext = Await.result(index.save(), Duration.Inf)

    println(s"inserted: ${n}")

    //Await.result(index.save(), Duration.Inf)

    val dlist = data.sortBy(_._1).map { case (k, v) => new String(k) }.toList
    val fullList = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)
      .map { case (k, v, _) => new String(k) }.toList

    println(s"dlist: ${Console.GREEN_B}${dlist}${Console.RESET}")
    println()
    println(s"ilist: ${Console.MAGENTA_B}${fullList}${Console.RESET}")

    println(s"is index full: ${index.isFull()}")

    assert(dlist == fullList)

    //Await.result(index.save(false), Duration.Inf)

    println("index saving", Await.result(storage.save(index.tmpCtx.snapshot()), Duration.Inf))

    val l = index.copy()
    val r = Await.result(l.split(), Duration.Inf)

    println("left id", l.tmpCtx.indexId, "right id", r.tmpCtx.indexId)

    Await.result(TestHelper.loadOrCreateIndex(IndexContext(l.tmpCtx.indexId, NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES)), Duration.Inf).get

    Await.result(TestHelper.loadOrCreateIndex(IndexContext(r.tmpCtx.indexId, NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES)), Duration.Inf).get

    /*println("left saving", Await.result(l.save(true), Duration.Inf))
    println("right saving", Await.result(r.save(true), Duration.Inf))*/

    println("left saving", Await.result(storage.save(l.tmpCtx.snapshot()), Duration.Inf))
    println("right saving", Await.result(storage.save(r.tmpCtx.snapshot()), Duration.Inf))

    Await.result(storage.save(cache.newBlocks.map{case (id, block) => id -> grpcBytesBytesSerializer.serialize(block.asInstanceOf[Block[K, V]])}.toMap),
      Duration.Inf)

    Await.result(storage.close(), Duration.Inf)

  }

}
