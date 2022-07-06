package services.scalable.index

import services.scalable.index.grpc._
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class DB[K, V](var ctx: DBContext = DBContext())(implicit val ec: ExecutionContext,
                               val storage: Storage,
                               val serializer: Serializer[Block[K, V]],
                               val cache: Cache,
                               val ord: Ordering[K],
                               val idGenerator: IdGenerator){
  import DefaultSerializers._

  var indexes = TrieMap.empty[String, QueryableIndex[K, V]]
  var history: Option[QueryableIndex[Long, IndexView]] = None

  /**
   * Fills up the indexes (if any) from the provided context
   */
  if(!ctx.latest.indexes.isEmpty){
    ctx.latest.indexes.foreach { case (id, ictx) =>
      indexes.put(id, new QueryableIndex[K, V](ictx))
    }
  }

  if(ctx.history.isDefined){
    history = Some(new QueryableIndex[Long, IndexView](ctx.history.get))
  }

  protected def putIndex(id: String): QueryableIndex[K, V] = {
    val index = new QueryableIndex[K, V](ctx.latest.indexes(id))
    indexes += (id -> index)
    index
  }

  def createHistory(id: String, num_leaf_entries: Int, num_meta_entries: Int): DBContext = {
    if(ctx.history.isDefined) return ctx

    val hIndexCtx = IndexContext(id, num_leaf_entries, num_meta_entries)

    history = Some(new QueryableIndex[Long, IndexView](hIndexCtx))

    ctx = ctx
      .withHistory(hIndexCtx)

    ctx
  }

  def createIndex(id: String, num_leaf_entries: Int, num_meta_entries: Int): DBContext = {

    if(ctx.latest.indexes.isDefinedAt(id)) return ctx

    var view = ctx.latest
    view = view.withIndexes(view.indexes + (id -> IndexContext(id, num_leaf_entries, num_meta_entries)))

    ctx = ctx
      .withLatest(view)

    putIndex(id)

    ctx
  }

  def execute(cmds: Seq[Commands.Command[K, V]]): Future[Boolean] = {
    def exec(id: String, cmds: Seq[Commands.Command[K, V]]): Future[Option[IndexContext]] = {
      val index = indexes(id)

      index.execute(cmds).map {
        case false => None
        case true => Some(index.snapshot())
      }
    }

    val groups = cmds.groupBy(_.id)

    Future.sequence(groups.map{ case (id, cmds) => exec(id, cmds)}).flatMap {
      case results if results.exists(_.isEmpty) => Future.successful(false)
      case results =>

        val time = System.nanoTime()

        val ictxs = results.map(_.get).map{c => c.id -> c}.toMap
        val view = ctx.latest.withIndexes(ictxs).withTime(time)

        history match {
          case None =>

            ctx = ctx.withLatest(view)

            Future.successful(true)
          case Some(history) =>

            ctx = ctx
              .withLatest(view)
              .withHistory(history.snapshot())

            history.execute(Seq(Commands.Insert(history.ctx.indexId, Seq(time -> view))))
        }
    }
  }

  def save(): Future[DBContext] = {
    /*val ictxs = indexes.map{case (id, i) => id -> i.snapshot()}
    val view = ctx.latest.withIndexes(indexes.map{case (id, i) => id -> ictxs(id)})*/

    if(history.isEmpty) {
      /*ctx = ctx
        .withLatest(view)*/

      return storage.save(ctx, indexes.map(_._2.ctx.getBlocks()).foldLeft(TrieMap.empty[(String, String), Block[K, V]]){ case (p, n) =>
        p ++ n
      }.map{case (id, block) => id -> serializer.serialize(block)}.toMap).map { r =>
        indexes.foreach(_._2.ctx.clear())
        ctx
      }
    }

    /*ctx = ctx
      .withLatest(view)
      .withHistory(history.get.snapshot())*/

      storage.save(ctx, indexes.map(_._2.ctx.getBlocks()).foldLeft(TrieMap.empty[(String, String), Block[K, V]]){ case (p, n) =>
        p ++ n
      }.map{case (id, block) => id -> serializer.serialize(block)}.toMap
        ++ history.get.ctx.getBlocks().map{case (id, block) => id -> grpcHistorySerializer.serialize(block)}).map { r =>
        indexes.foreach(_._2.ctx.clear())
        ctx
      }
  }

  def findT(t: Long): Future[Option[IndexView]] = {
    if(ctx.history.isEmpty) return Future.successful(None)

    history.get.findPath(t).map(_.map { leaf =>
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
    indexes.get(index)
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
