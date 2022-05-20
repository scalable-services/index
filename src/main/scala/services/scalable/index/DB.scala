package services.scalable.index

import jdk.internal.org.jline.reader.History
import services.scalable.index.grpc._

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

case class DBExecutionResult[K, V](ok: Boolean = false,
                                   ctx: Option[DBContext] = None,
                                   blocks: Map[(String, String), Array[Byte]] = Map.empty[(String, String), Array[Byte]])

class DB[K, V](var ctx: DBContext = DBContext())(implicit val ec: ExecutionContext,
                               val storage: Storage,
                               val serializer: Serializer[Block[K, V]],
                               val cache: Cache,
                               val ord: Ordering[K],
                               val idGenerator: IdGenerator){
  import DefaultSerializers._

  def createHistory(id: String, num_leaf_entries: Int, num_meta_entries: Int): DBContext = {
    ctx = ctx
      .withHistory(IndexContext(id, num_leaf_entries, num_meta_entries))
    ctx
  }

  def createIndex(id: String, num_leaf_entries: Int, num_meta_entries: Int): DBContext = {
    var view = ctx.latest
    view = view.withIndexes(view.indexes + (id -> IndexContext(id, num_leaf_entries, num_meta_entries)))

    this.ctx = ctx
      .withLatest(view)

    ctx
  }

  def execute(cmds: Seq[Commands.Command[K, V]]): Future[DBExecutionResult[K, V]] = {
    var indexes = Map.empty[String, Index[K, V]]

    val history: Option[QueryableIndex[Long, IndexView]] = if(ctx.history.isEmpty) None
      else Some(new QueryableIndex[Long, IndexView](ctx.history.get))
    var view = ctx.latest

    def getIndex(id: String): Index[K, V] = {
      val opt = indexes.get(id)

      if(opt.isEmpty){
        val index = new QueryableIndex[K, V](view.indexes(id))
        indexes = indexes + (id -> index)
        return index
      }

      opt.get
    }

    def exec(id: String, cmds: Seq[Commands.Command[K, V]]): Future[Option[Context[K, V]]] = {
      val index = getIndex(id)

      index.execute(cmds).map {
        case false => None
        case true => Some(index.ctx)
      }
    }

    val groups = cmds.groupBy(_.id)

    Future.sequence(groups.map{ case (id, cmds) => exec(id, cmds)}).flatMap {
      case results if results.exists(_.isEmpty) => Future.successful(DBExecutionResult[K, V](false))
      case results =>

        val ctxs = results.map(_.get)
        val time = System.nanoTime()

        var indexes = view.indexes

        ctxs.foreach { i =>
          indexes = indexes + (i.indexId -> i.save())
        }

        view = view.withIndexes(indexes)

        history match {
          case None =>

            this.ctx = ctx.withLatest(view)

            Future.successful(DBExecutionResult(true,
              Some(this.ctx), ctxs.map(_.blocks).foldLeft(TrieMap.empty[(String, String), Block[K, V]]){ case (p, n) =>
                p ++ n
              }.map{case (id, block) => id -> serializer.serialize(block)}.toMap))

          case Some(history) =>

            history.insert(Seq(time -> view)).map {
              case n if n == 1 =>

                this.ctx = ctx
                  .withLatest(view)
                  .withHistory(history.save())

                DBExecutionResult(true,
                  Some(this.ctx), ctxs.map(_.blocks).foldLeft(TrieMap.empty[(String, String), Block[K, V]]){ case (p, n) =>
                    p ++ n
                  }.map{case (id, block) => id -> serializer.serialize(block)}.toMap
                    ++ history.ctx.blocks.map{case (id, block) => id -> grpcHistorySerializer.serialize(block)})

              case _ => DBExecutionResult[K, V](false)
            }
        }


    }
  }

  def findT(t: Long): Future[Option[IndexView]] = {
    if(ctx.history.isEmpty) return Future.successful(None)

    val h = ctx.history.get
    val history = new QueryableIndex[Long, IndexView](h)

    history.findPath(t).map(_.map { leaf =>
      var pos = leaf.binSearch(t, 0, leaf.tuples.length - 1)._2
      pos = if(pos == leaf.length) pos - 1 else pos
      leaf.tuples(pos)._2
    })
  }

  def findIndex(t: Long, index: String): Future[Option[QueryableIndex[K, V]]] = {
    findT(t).map {
      case None => None
      case Some(view) => view.indexes.get(index).map(new QueryableIndex[K, V](_))
    }
  }

  def findLatestIndex(index: String): Option[QueryableIndex[K, V]] = {
    ctx.latest.indexes.get(index).map(new QueryableIndex[K, V](_))
  }

  def findT(t: Long, index: String): Future[Option[IndexContext]] = {
    findT(t).map {
      case None => None
      case Some(view) => view.indexes.get(index)
    }
  }

  def latest(): IndexView = {
    ctx.latest
  }

  def latest(index: String): Option[IndexContext] = {
    ctx.latest.indexes.get(index)
  }
}
