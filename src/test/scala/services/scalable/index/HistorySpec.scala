package services.scalable.index

import com.google.common.base.Charsets
import com.google.protobuf.ByteString
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.Commands._
import services.scalable.index.grpc._
import services.scalable.index.impl.{CassandraStorage, DefaultCache, DefaultContext}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

class HistorySpec extends Repeatable {

  override val times: Int = 1

  val logger = LoggerFactory.getLogger(this.getClass)

  "operations" should " run successfully" in {

    import DefaultSerializers._
    import DefaultComparators._
    import DefaultIdGenerators._

    type K = Bytes
    type V = Bytes

    val indexName = "history-test"

    val rand = ThreadLocalRandom.current()

    val NUM_LEAF_ENTRIES = 8
    val NUM_META_ENTRIES = 8

    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new CassandraStorage(TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, false)

    val db = Await.result(storage.loadOrCreate(indexName), Duration.Inf)

    val hindex = new HIndex(db, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    var data = Seq.empty[(K, V)]

    implicit def insertGrpcToInsertCommand(cmd: InsertCommand): Command[Bytes, Bytes] = {
      Insert(cmd.list.map{p => p.key.toByteArray -> p.value.toByteArray})
    }

    implicit def updateGrpcToInsertCommand(cmd: UpdateCommand): Command[Bytes, Bytes] = {
      Update(cmd.list.map{p => p.key.toByteArray -> p.value.toByteArray})
    }

    implicit def removeGrpcToInsertCommand(cmd: RemoveCommand): Command[Bytes, Bytes] = {
      Remove(cmd.keys.map(_.toByteArray))
    }

    def insert(): Command[K, V] = {
      var list = Seq.empty[(K, V)]

      for(i<-0 until 20){
        val k = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)
        val v = k.clone()//RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        if(!list.exists{ case (k1, _) => ord.equiv(k, k1)}){
          list = list :+ k -> v
        }
      }

      data = data ++ list

      InsertCommand(list.map{ case (k, v) => KVPair(ByteString.copyFrom(k), ByteString.copyFrom(v))})
    }

    def remove(): Command[K, V] = {
      val bound = if(data.length == 1) 1 else rand.nextInt(1, data.length)
      val list = scala.util.Random.shuffle(data.slice(0, bound)).map(_._1)

      data = data.filterNot{case (k, _) => list.exists{k1 => ord.equiv(k, k1)}}

      RemoveCommand(list.map{ByteString.copyFrom(_)})
    }

    def update(): Command[K, V] = {
      val bound = if(data.length == 1) 1 else rand.nextInt(1, data.length)
      val list = scala.util.Random.shuffle(data.slice(0, bound))
        .map{case (k, _) => k -> RandomStringUtils.randomAlphabetic(5).getBytes("UTF-8")}

      val notin = data.filterNot{case (k1, _) => list.exists{case (k, _) => ord.equiv(k, k1)}}
      data = (notin ++ list).sortBy(_._1)

      UpdateCommand(list.map{case (k, v) => KVPair(ByteString.copyFrom(k), ByteString.copyFrom(v))})
    }

    /*for(i<-0 until 20){

      var cmds = Seq.empty[Command[K, V]]

      for(i<-0 until 100){
        rand.nextInt(1, 4) match {
          case 1 => cmds = cmds :+ insert()
          case 2 if !data.isEmpty => cmds = cmds :+ remove()
          case 3 if !data.isEmpty => cmds = cmds :+ update()
          case _ =>
        }
      }

      Await.result(hindex.execute(cmds), Duration.Inf)
    }*/

    val index = new QueryableIndex[K, V](new DefaultContext[K, V]("main", hindex.indexContext.root, hindex.indexContext.numElements,
      hindex.indexContext.levels, NUM_LEAF_ENTRIES, NUM_META_ENTRIES))

    val dlist = data.sortBy(_._1)
    val ilist = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)

    isColEqual(dlist, ilist)

    val history = new QueryableIndex[Long, IndexContext](
      new DefaultContext[Long, IndexContext]("history", hindex.historyContext.root,
        hindex.historyContext.numElements, hindex.historyContext.levels, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    )

    val t = Await.result(history.first().map(_.get.first), Duration.Inf)

    //logger.setLevel(Level.INFO)

    implicit def longToStr(l: Long): String = l.toString
    implicit def optToStr(opt: IndexContext): String = opt.id

    history.prettyPrint()

    val root = Await.result(hindex.findT(t), Duration.Inf)

    def printTree(opt: Option[IndexContext]): Unit = {
      if(opt.isEmpty) return

      val ctx = opt.get

      val index = new QueryableIndex[K, V](new DefaultContext[K, V](ctx.id, ctx.root,
        ctx.numElements, ctx.levels, NUM_LEAF_ENTRIES, NUM_META_ENTRIES))
      val idata = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)
      println(s"${Console.GREEN_B}${idata.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v, Charsets.UTF_8)}}${Console.RESET}")
    }

    printTree(root)
    println()
    val hctx = hindex.indexContext
    printTree(Some(IndexContext(hctx.id, hctx.numLeafItems, hctx.numMetaItems,
      hctx.root, hctx.levels, hctx.numElements)))

    storage.close()
  }

}
