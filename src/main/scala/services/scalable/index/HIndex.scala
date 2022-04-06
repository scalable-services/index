package services.scalable.index

import services.scalable.index.Commands._
import services.scalable.index.grpc.{DatabaseContext, IndexContext, RootRef}
import services.scalable.index.impl.DefaultContext

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class HIndex[K, V](db: DatabaseContext,
                         val NUM_LEAF_ENTRIES: Int,
                         val NUM_META_ENTRIES: Int)
                        (implicit val ec: ExecutionContext,
                         val ordering: Ordering[K],
                         val serializer: Serializer[Block[K, V]],
                         val storage: Storage,
                         val cache: Cache) {

  import DefaultSerializers._

  protected val mainIndex = db.indexes.find(_.id.compareTo("main") == 0)
  protected val historyIndex = db.indexes.find(_.id.compareTo("history") == 0)

  var indexContext = if(mainIndex.isEmpty) IndexContext("main", NUM_LEAF_ENTRIES, NUM_META_ENTRIES, None, 0, 0) else mainIndex.get
  var historyContext = if(historyIndex.isEmpty) IndexContext("history", NUM_LEAF_ENTRIES, NUM_META_ENTRIES, None, 0, 0) else historyIndex.get

  protected def inserth(list: Seq[(Long, IndexContext)])(implicit idGenerator: IdGenerator): Future[Option[DefaultContext[Long, IndexContext]]] = {
    val ctx = new DefaultContext[Long, IndexContext](historyContext.id, historyContext.root.map{r => (r.partition, r.id)}, historyContext.numElements,
      historyContext.levels, historyContext.numLeafItems, historyContext.numMetaItems)
    val index = new QueryableIndex[Long, IndexContext](ctx)

    index.insert(list).map(_ == 1).map {
      case true => Some(index.ctx.asInstanceOf[DefaultContext[Long, IndexContext]])
      case false => None
    }
  }

  def seqFutures[T, U](items: IterableOnce[T])(fn: T => Future[U]): Future[List[U]] = {
    items.iterator.foldLeft(Future.successful[List[U]](Nil)) {
      (f, item) => f.flatMap {
        x => fn(item).map(_ :: x)
      }
    } map (_.reverse)
  }

  def execute(cmds: Seq[Command[K, V]]): Future[Boolean] = {

    implicit val idGenerator = new IdGenerator {
      val partition = UUID.randomUUID.toString

      override def generateId[K, V](ctx: Context[K, V]): String = partition
      override def generatePartition[K, V](ctx: Context[K, V]): String = UUID.randomUUID.toString
    }

    val ctx = new DefaultContext[K, V](indexContext.id, indexContext.root.map{r => (r.partition, r.id)}, indexContext.numElements, indexContext.levels,
      indexContext.numLeafItems, indexContext.numMetaItems)
    val index = new QueryableIndex[K, V](ctx)
    val now = System.nanoTime()

    def process(pos: Int, previous: Boolean): Future[Boolean] = {

      if(!previous) return Future.successful(false)
      if(pos == cmds.length) return Future.successful(true)

      val cmd = cmds(pos)

      (cmd match {
        case cmd: Insert[K, V] => index.insert(cmd.list).map(_ == cmd.list.length)
        case cmd: Remove[K, V] => index.remove(cmd.keys).map(_ == cmd.keys.length)
        case cmd: Update[K, V] => index.update(cmd.list).map(_ == cmd.list.length)
      }).flatMap(ok => process(pos + 1, ok))
    }

    /*seqFutures(cmds){
      case cmd: InsertCommand[K, V] => index.insert(cmd.list).map(_ == cmd.list.length)
      case cmd: RemoveCommand[K, V] => index.remove(cmd.keys).map(_ == cmd.keys.length)
      case cmd: UpdateCommand[K, V] => index.update(cmd.list).map(_ == cmd.list.length)
    }*/

    process(0, true).flatMap {
      case true =>

        val c = index.ctx.asInstanceOf[DefaultContext[K, V]]
        val icontext = IndexContext("main", c.NUM_LEAF_ENTRIES, c.NUM_META_ENTRIES, c.root.map{r => RootRef(r._1, r._2)}, c.levels, c.num_elements)

        inserth(Seq(now -> icontext)).flatMap {
          case Some(h) =>

            h.save()
            c.save()

            val hcontext = new IndexContext("history", h.NUM_LEAF_ENTRIES, h.NUM_META_ENTRIES, h.root.map{r => RootRef(r._1, r._2)}, h.levels, h.num_elements)
            val database = db.withIndexes(Seq(icontext, hcontext))

            val blocks = c.blocks.map{case (_, block) => (block.partition, block.id) -> serializer.serialize(block)}.toMap ++
              h.blocks.map{case (_, block) => (block.partition, block.id) -> grpcHistorySerializer.serialize(block)}.toMap

            storage.save(database, blocks).map {
              case true =>

                this.indexContext = icontext
                this.historyContext = hcontext

                true

              case false => false
            }

          case None => Future.successful(false)
        }

      case false => Future.successful(false)
    }
  }

  def findT(t: Long): Future[Option[IndexContext]] = {

    import DefaultIdGenerators._

    val ctx = new DefaultContext[Long, IndexContext](historyContext.id, historyContext.root.map{r => (r.partition, r.id)}, historyContext.numElements,
      historyContext.levels, historyContext.numLeafItems, historyContext.numMetaItems)
    val history = new QueryableIndex[Long, IndexContext](ctx)

    history.findPath(t).map(_.map { leaf =>
      var pos = leaf.binSearch(t, 0, leaf.tuples.length - 1)._2
      pos = if(pos == leaf.length) pos - 1 else pos
      leaf.tuples(pos)._2
    })
  }
}
