package services.scalable.index

import org.apache.commons.lang3.RandomStringUtils
import services.scalable.index.DefaultComparators.ord
import services.scalable.index.DefaultSerializers._
import services.scalable.index.grpc.{IndexContext, RootRef}
import services.scalable.index.impl._

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

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
    implicit val storage = new CassandraStorage("history", false)

    val txId = UUID.randomUUID().toString

    val MAX_ITEMS = 250

    val ctx = Await.result(TestHelper.loadOrCreateIndex(IndexContext("test-index", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)),
      Duration.Inf).get
      .withMaxNItems(MAX_ITEMS)
      .withNumLeafItems(NUM_LEAF_ENTRIES)
      .withNumMetaItems(NUM_META_ENTRIES)

    val index = new QueryableIndex[K, V](ctx)

    var data = Seq.empty[(Bytes, Bytes)]

    def insert(): Commands.Command[K, V] = {
      val n = 3000//rand.nextInt(1000, 2000) //rand.nextInt(1, 1000)

      var list = Seq.empty[(Bytes, Bytes)]

      for (i <- 0 until n) {
        val k = RandomStringUtils.randomAlphanumeric(7).getBytes("UTF-8")

        if (!data.exists(x => ord.equiv(k, x._1)) && !list.exists { case (k1, v) => ord.equiv(k, k1) }) {
          data = data :+ k -> k
          list = list :+ k -> k
        }
      }

      Commands.Insert("main", list)
    }

    val cmds = Seq[Commands.Command[K, V]](insert())

    val n = Await.result(index.execute(cmds, txId), Duration.Inf)

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

    println("index saving", Await.result(storage.save(index.ctx.snapshot()), Duration.Inf))

    val l = index.copy()
    val r = Await.result(l.split(), Duration.Inf)

    println("left id", l.ctx.indexId, "right id", r.ctx.indexId)

    Await.result(TestHelper.loadOrCreateIndex(IndexContext(l.ctx.indexId, NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES)), Duration.Inf).get

    Await.result(TestHelper.loadOrCreateIndex(IndexContext(r.ctx.indexId, NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES)), Duration.Inf).get

    /*println("left saving", Await.result(l.save(true), Duration.Inf))
    println("right saving", Await.result(r.save(true), Duration.Inf))*/

    println("left saving", Await.result(storage.save(l.ctx.snapshot()), Duration.Inf))
    println("right saving", Await.result(storage.save(r.ctx.snapshot()), Duration.Inf))

    Await.result(storage.save(cache.newBlocks.map{case (id, block) => id -> grpcBytesSerializer.serialize(block.asInstanceOf[Block[K, V]])}.toMap),
      Duration.Inf)

    Await.result(storage.close(), Duration.Inf)

  }

}
