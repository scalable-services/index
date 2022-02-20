package services.scalable.index

import ch.qos.logback.classic.Level
import com.google.common.base.Charsets
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.impl.{DefaultCache, DefaultContext, MemoryStorage}

import java.util.UUID
import java.util.concurrent.{Executors, ThreadLocalRandom}
import scala.concurrent.ExecutionContext.Implicits.global
import services.scalable.index.DefaultComparators._
import services.scalable.index.grpc.IndexContext

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object Main {

  val logger = LoggerFactory.getLogger(this.getClass).asInstanceOf[ch.qos.logback.classic.Logger]

  logger.setLevel(Level.OFF)

  class HIndex[K, V](val indexId: String, val NUM_LEAF_ENTRIES: Int, val NUM_META_ENTRIES: Int)(implicit val ord: Ordering[K]) {

    implicit def longToStr(l: Long): String = l.toString
    implicit def optToStr(opt: IndexContext): String = opt.toString

    implicit val cache = new DefaultCache[K, V](MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new MemoryStorage[K, V](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    var ctx: Context[K, V] = new DefaultContext[K, V](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    implicit val hcache = new DefaultCache[Long, IndexContext]()
    implicit val hstorage = new MemoryStorage[Long, IndexContext](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    var hctx: Context[Long, IndexContext] = new DefaultContext[Long, IndexContext](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    protected def inserth(list: Seq[(Long, IndexContext)]): Future[Boolean] = {
      val index = new QueryableIndex[Long, IndexContext](hctx)

      index.insert(list).map(_ == 1).flatMap {
        case true => index.save().map { hctx =>
          this.hctx = hctx
          true
        }
        case false => Future.successful(false)
      }
    }

    def insert(list: Seq[(K, V)]): Future[Boolean] = {
      val index = new QueryableIndex[K, V](ctx)

      val now = System.nanoTime()

      index.insert(list).map(_ == list.length).flatMap{ ok =>
        if(ok){
          index.save().flatMap { ctx =>
            this.ctx = ctx
            inserth(List(now -> IndexContext(ctx.indexId, ctx.NUM_LEAF_ENTRIES, ctx.NUM_META_ENTRIES, ctx.root, ctx.levels, ctx.num_elements)))
          }
        } else {
          Future.successful(false)
        }
      }
    }

    def findT(t: Long): Future[Option[IndexContext]] = {
      val history = new QueryableIndex[Long, IndexContext](hctx)

      history.findPath(t).map(_.map { leaf =>
        var pos = leaf.binSearch(t, 0, leaf.tuples.length - 1)._2
        pos = if(pos == leaf.length) pos - 1 else pos
        leaf.tuples(pos)._2
      })
    }
  }

  def all[K, V](it: AsyncIterator[Seq[Tuple[K, V]]]): Future[Seq[Tuple[K, V]]] = {
    it.hasNext().flatMap {
      case true => it.next().flatMap { list =>
        all(it).map{list ++ _}
      }
      case false => Future.successful(Seq.empty[Tuple[K, V]])
    }
  }

  def main(args: Array[String]): Unit = {

    val rand = ThreadLocalRandom.current()

    type K = Bytes
    type V = Bytes

    val NUM_LEAF_ENTRIES = 4
    val NUM_META_ENTRIES = 4

    val indexId = UUID.randomUUID().toString

    val hindex = new HIndex[K, V](indexId, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    def insert(): Future[Boolean] = {
      var list = Seq.empty[(K, V)]

      for(i<-0 until 20){
        val k = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)
        val v = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        if(!list.exists{case (k1, _) => ord.equiv(k, k1)}){
          list = list :+ k -> v
        }
      }

      hindex.insert(list)
    }

    var tasks = insert()

    for(i<-1 until 10){
      tasks = tasks.flatMap(_ => insert())
    }

    val hc = Await.result(tasks, Duration.Inf)

    import hindex._

    val t = System.nanoTime()

    val history = new QueryableIndex[Long, IndexContext](hindex.hctx)

    logger.setLevel(Level.INFO)

    history.prettyPrint()

    val root = Await.result(hindex.findT(t), Duration.Inf)

    def printTree(opt: Option[IndexContext]): Unit = {
      if(opt.isEmpty) return

      val root = opt.get.root

      val index = new QueryableIndex[K, V](new DefaultContext[K, V](indexId, root, NUM_LEAF_ENTRIES, NUM_META_ENTRIES))
      val idata = Await.result(all(index.inOrder()), Duration.Inf)
      println(s"${Console.GREEN_B}${idata.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v, Charsets.UTF_8)}}${Console.RESET}")
    }

    printTree(root)
    println()
    printTree(Some(IndexContext(hindex.ctx.indexId, hindex.ctx.NUM_LEAF_ENTRIES, hindex.ctx.NUM_META_ENTRIES,
      hindex.ctx.root, hindex.ctx.levels, hindex.ctx.num_elements)))
  }

}
