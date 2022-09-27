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

  override val times: Int = 1000

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
    implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    //implicit val storage = new CassandraStorage("history", NUM_LEAF_ENTRIES, NUM_META_ENTRIES, false)

    val txId = UUID.randomUUID().toString

    val index = new QueryableIndex[K, V](IndexContext("test-index")
    .withNumLeafItems(NUM_LEAF_ENTRIES).withNumMetaItems(NUM_META_ENTRIES))

    var data = Seq.empty[(Bytes, Bytes)]

    def insert(): Commands.Command[K, V] = {
      val n = rand.nextInt(1000, 2000) //rand.nextInt(1, 1000)

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

    val ctx = Await.result(index.save(), Duration.Inf)

    println(ctx)

    val dlist = data.sortBy(_._1).map { case (k, v) => new String(k) }.toList
    val fullList = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)
      .map { case (k, v, _) => new String(k) }.toList

    println(s"dlist: ${Console.GREEN_B}${dlist}${Console.RESET}")
    println()
    println(s"ilist: ${Console.MAGENTA_B}${fullList}${Console.RESET}")

    assert(dlist == fullList)

    /*def split(index: QueryableIndex[K, V]): Future[(QueryableIndex[K, V], QueryableIndex[K, V])] = {
      implicit val ctx = index.ctx

      for {
        leftRoot <- ctx.getMeta(ctx.root.get).map(_.copy())
        rightRoot = leftRoot.split()

        leftMeta <- if(leftRoot.length == 1) ctx.get(leftRoot.pointers(0)._2.unique_id) else
            Future.successful(leftRoot)

        rightMeta <- if(rightRoot.length == 1) ctx.get(rightRoot.pointers(0)._2.unique_id) else
            Future.successful(rightRoot)

        leftIndexCtx = IndexContext(index.ctx.id)
          .withNumLeafItems(index.ctx.NUM_LEAF_ENTRIES)
          .withNumMetaItems(index.ctx.NUM_META_ENTRIES)
          .withNumElements(leftMeta.nSubtree)
          .withLevels(leftMeta.level)
          .withRoot(RootRef(leftMeta.unique_id._1, leftMeta.unique_id._2))

        rightIndexCtx = IndexContext(UUID.randomUUID.toString)
          .withNumLeafItems(index.ctx.NUM_LEAF_ENTRIES)
          .withNumMetaItems(index.ctx.NUM_META_ENTRIES)
          .withNumElements(rightMeta.nSubtree)
          .withLevels(rightMeta.level)
          .withRoot(RootRef(rightMeta.unique_id._1, rightMeta.unique_id._2))

        snapshot = index.snapshot()

        leftIndex = new QueryableIndex[K, V](leftIndexCtx)
        rightIndex = new QueryableIndex[K, V](rightIndexCtx)

      } yield {
        (leftIndex, rightIndex)
      }
    }*/

    val left = index.copy()
    val right = Await.result(left.split(), Duration.Inf)

    val leftList = Await.result(TestHelper.all(left.inOrder()), Duration.Inf)
      .map { case (k, v, _) => new String(k) }.toList

    val rightList = Await.result(TestHelper.all(right.inOrder()), Duration.Inf)
      .map { case (k, v, _) => new String(k) }.toList

    println(s"left: ${Console.GREEN_B}${leftList}${Console.RESET}")
    println()
    println(s"right: ${Console.MAGENTA_B}${rightList}${Console.RESET}")

    assert(fullList == (leftList ++ rightList))

  }

}
