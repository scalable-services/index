package services.scalable.index

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.{DatabaseContext, IndexContext}
import services.scalable.index.impl.{CassandraStorage, DefaultCache, DefaultContext, GrpcByteSerializer, MemoryStorage}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class MainSpec extends Repeatable {

  override val times: Int = 1

  "operations" should " run successfully" in {

    type T = services.scalable.index.QueryableIndex[K, V]
    val logger = LoggerFactory.getLogger(this.getClass)
    
    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Bytes
    type V = Bytes

    import services.scalable.index.DefaultComparators._

    val NUM_LEAF_ENTRIES = 4//rand.nextInt(5, 64)
    val NUM_META_ENTRIES = 4//rand.nextInt(5, 64)

    val indexId = UUID.randomUUID().toString

    import services.scalable.index.DefaultSerializers._

    implicit val cache = new DefaultCache[K, V](MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new MemoryStorage[K, V](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    implicit val storage = new CassandraStorage[K, V](TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, false)

    var db = Await.result(storage.loadOrCreate("test"), Duration.Inf)
    val indexContext = if(db.indexes.isEmpty) IndexContext("test", NUM_LEAF_ENTRIES, NUM_META_ENTRIES, None, 0, 0) else db.indexes.head

    val ctx = new DefaultContext[K, V](indexContext.id, indexContext.root, indexContext.numLeafItems, indexContext.numMetaItems)

    var data = Seq.empty[(K, V)]
    val index = new QueryableIndex[K, V](ctx)

    def insert(): Unit = {
      val n = 100//rand.nextInt(1, 100)
      var list = Seq.empty[Tuple[K, V]]

      for(i<-0 until n){
        val k = RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
        val v = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        if(!data.exists{case (k1, _) => ord.equiv(k, k1)}){
          list = list :+ (k -> v)
        }
      }

      val result = Await.result(index.insert(list), Duration.Inf)

      if(result > 0){
        data = data ++ list.slice(0, result)
      }
    }

    //insert()

    val indexCtx = index.ctx.asInstanceOf[DefaultContext[Bytes, Bytes]]
    //indexCtx.save()

    db = db.withIndexes(Seq(
      IndexContext("test-main-index", NUM_LEAF_ENTRIES, NUM_META_ENTRIES, indexCtx.root, indexCtx.levels, indexCtx.num_elements)
    ))

    logger.info(Await.result(storage.save(db, indexCtx.blocks.map{case (id, block) => id -> bytesSerializer.serialize(block)}.toMap),
      Duration.Inf).toString)

    val dlist = data.sortBy(_._1)
    val ilist = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)

    logger.debug(s"${Console.GREEN_B}tdata: ${dlist.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata: ${ilist.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

    assert(isColEqual(dlist, ilist))
  }

}
