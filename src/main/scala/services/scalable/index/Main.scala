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
import services.scalable.index.grpc._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object Main {

  val logger = LoggerFactory.getLogger(this.getClass).asInstanceOf[ch.qos.logback.classic.Logger]

  logger.setLevel(Level.OFF)

  trait IndexCommand
  case class InsertCommand[K, V](list: Seq[(K, V)]) extends IndexCommand
  case class RemoveCommand[K, V](keys: Seq[K]) extends IndexCommand
  case class UpdateCommand[K, V](list: Seq[(K, V)]) extends IndexCommand

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

    def seqFutures[T, U](items: IterableOnce[T])(fn: T => Future[U]): Future[List[U]] = {
      items.iterator.foldLeft(Future.successful[List[U]](Nil)) {
        (f, item) => f.flatMap {
          x => fn(item).map(_ :: x)
        }
      } map (_.reverse)
    }

    def execute(cmds: Seq[IndexCommand]): Future[Boolean] = {

      val index = new QueryableIndex[K, V](ctx)
      val now = System.nanoTime()

      def process(pos: Int, previous: Boolean): Future[Boolean] = {
        if(!previous) return Future.successful(false)
        if(pos == cmds.length) return Future.successful(true)

        val cmd = cmds(pos)

        (cmd match {
          case cmd: InsertCommand[K, V] => index.insert(cmd.list).map(_ == cmd.list.length)
          case cmd: RemoveCommand[K, V] => index.remove(cmd.keys).map(_ == cmd.keys.length)
          case cmd: UpdateCommand[K, V] => index.update(cmd.list).map(_ == cmd.list.length)
        }).flatMap(ok => process(pos + 1, ok))
      }

      /*seqFutures(cmds){
        case cmd: InsertCommand[K, V] => index.insert(cmd.list).map(_ == cmd.list.length)
        case cmd: RemoveCommand[K, V] => index.remove(cmd.keys).map(_ == cmd.keys.length)
        case cmd: UpdateCommand[K, V] => index.update(cmd.list).map(_ == cmd.list.length)
      }*/

      process(0, true).flatMap { ok =>
        if(/*ok.forall(_ == true)*/ok){
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
    var data = Seq.empty[(K, V)]

    def insert(): IndexCommand = {
      var list = Seq.empty[(K, V)]

      for(i<-0 until 20){
        val k = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)
        val v = k.clone()//RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        if(!list.exists{case (k1, _) => ord.equiv(k, k1)}){
          list = list :+ k -> v
        }
      }

      data = data ++ list

      InsertCommand(list)
    }

    def remove(): IndexCommand = {
      val bound = if(data.length == 1) 1 else rand.nextInt(1, data.length)
      val list = scala.util.Random.shuffle(data.slice(0, bound)).map(_._1)

      data = data.filterNot{case (k, _) => list.exists{k1 => ord.equiv(k, k1)}}

      RemoveCommand(list)
    }

    def update(): IndexCommand = {
      val bound = if(data.length == 1) 1 else rand.nextInt(1, data.length)
      val list = scala.util.Random.shuffle(data.slice(0, bound))
        .map{case (k, _) => k -> RandomStringUtils.randomAlphabetic(5).getBytes("UTF-8")}

      val notin = data.filterNot{case (k1, _) => list.exists{case (k, _) => ord.equiv(k, k1)}}
      data = (notin ++ list).sortBy(_._1)

      UpdateCommand(list)
    }

    var cmds = Seq.empty[IndexCommand]

    for(i<-0 until 100){
      rand.nextInt(1, 4) match {
        case 1 => cmds = cmds :+ insert()
        case 2 if !data.isEmpty => cmds = cmds :+ remove()
        case 3 if !data.isEmpty => cmds = cmds :+ update()
        case _ =>
      }
    }

    import hindex._

    val hc = Await.result(hindex.execute(cmds), Duration.Inf)

    val index = new QueryableIndex[K, V](new DefaultContext[K, V](indexId, hindex.ctx.root, NUM_LEAF_ENTRIES, NUM_META_ENTRIES))

    val dlist = data.sortBy(_._1)
    val ilist = Await.result(all(index.inOrder()), Duration.Inf)

    isColEqual(dlist, ilist)

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
