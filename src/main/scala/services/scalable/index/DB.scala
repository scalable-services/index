package services.scalable.index

import services.scalable.index.grpc._

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

case class DBExecutionResult(ok: Boolean = false,
                                   ctx: Option[DBContext] = None,
                                   blocks: Map[(String, String), Array[Byte]] = Map.empty[(String, String), Array[Byte]])

class DB[K, V](var ctx: DBContext = DBContext())(implicit val ec: ExecutionContext,
                               val storage: Storage,
                               val serializer: Serializer[Block[K, V]],
                               val cache: Cache,
                               val ord: Ordering[K],
                               val idGenerator: IdGenerator){
  import DefaultSerializers._

  protected var indexes = Map.empty[String, Index[K, V]]
  var history: Option[QueryableIndex[Long, IndexView]] = None

  protected def putIndex(id: String): QueryableIndex[K, V] = {
    val index = new QueryableIndex[K, V](ctx.latest.indexes(id))
    indexes = indexes + (id -> index)
    index
  }

  def createHistory(id: String, num_leaf_entries: Int, num_meta_entries: Int): DBContext = {
    val hIndexCtx = IndexContext(id, num_leaf_entries, num_meta_entries)

    history = Some(new QueryableIndex[Long, IndexView](hIndexCtx))

    ctx = ctx
      .withHistory(hIndexCtx)

    ctx
  }

  def createIndex(id: String, num_leaf_entries: Int, num_meta_entries: Int): DBContext = {
    var view = ctx.latest
    view = view.withIndexes(view.indexes + (id -> IndexContext(id, num_leaf_entries, num_meta_entries)))

    ctx = ctx
      .withLatest(view)

    putIndex(id)

    ctx
  }

  def execute(cmds: Seq[Commands.Command[K, V]]): Future[Boolean] = {
    //var view = ctx.latest

    def exec(id: String, cmds: Seq[Commands.Command[K, V]]): Future[Option[IndexContext]] = {
      val index = indexes(id)

      index.execute(cmds).map {
        case false => None
        case true => Some(index.ctx.save())
      }
    }

    val groups = cmds.groupBy(_.id)

    Future.sequence(groups.map{ case (id, cmds) => exec(id, cmds)}).flatMap {
      case results if results.exists(_.isEmpty) => Future.successful(false)
      case results =>

        val time = System.nanoTime()

        val ictxs = results.map(_.get).map{c => c.id -> c}.toMap
        val view = ctx.latest.withIndexes(ictxs).withTime(time)

        println(s"\n\nexec view: ${view}\n\n")

        history match {
          case None => Future.successful(true)
          case Some(history) =>

            history.execute(Seq(Commands.Insert("history", Seq(time -> view)))).map {
              case true =>

                println()
                println(Await.result(all(history.inOrder()), Duration.Inf))
                println()

                //ctx = ctx.withLatest(view).withHistory(history.save())

                true
              case _ => false
            }
        }
    }
  }

  def all[K, V](it: AsyncIterator[Seq[Tuple[K, V]]])(implicit ec: ExecutionContext): Future[Seq[Tuple[K, V]]] = {
    it.hasNext().flatMap {
      case true => it.next().flatMap { list =>
        all(it).map{list ++ _}
      }
      case false => Future.successful(Seq.empty[Tuple[K, V]])
    }
  }

  def save(): DBExecutionResult = {
    val ictxs = indexes.map{case (id, i) => id -> i.save()}
    val view = ctx.latest.withIndexes(indexes.map{case (id, i) => id -> ictxs(id)})

    if(history.isEmpty) {
      ctx = ctx
        .withLatest(view)

      return DBExecutionResult(true,
        Some(ctx), indexes.map(_._2.ctx.blocks).foldLeft(TrieMap.empty[(String, String), Block[K, V]]){ case (p, n) =>
          p ++ n
        }.map{case (id, block) => id -> serializer.serialize(block)}.toMap)
    }

    /*println()
    println(Await.result(all(history.get.inOrder()), Duration.Inf))
    println()*/

    println(history.get.ctx.indexId)

    ctx = ctx
      .withLatest(view)
      .withHistory(history.get.save())

    DBExecutionResult(true,
      Some(ctx), indexes.map(_._2.ctx.blocks).foldLeft(TrieMap.empty[(String, String), Block[K, V]]){ case (p, n) =>
        p ++ n
      }.map{case (id, block) => id -> serializer.serialize(block)}.toMap
        ++ history.get.ctx.blocks.map{case (id, block) => id -> grpcHistorySerializer.serialize(block)})
  }

  def findT(t: Long): Future[Option[IndexView]] = {
    if(ctx.history.isEmpty) return Future.successful(None)

    history.get.findPath(t).map(_.map { leaf =>
      var pos = leaf.binSearch(t, 0, leaf.tuples.length - 1)._2
      pos = if(pos == leaf.length) pos - 1 else pos

      println(s"prox ${t} => ${leaf.tuples(pos)._1}")

      leaf.tuples(pos)._2
    })
  }

  def findIndex(t: Long, index: String): Future[Option[QueryableIndex[K, V]]] = {
    findT(t).map {
      case None => None
      case Some(view) =>

        println(s"t: ${t} view: ${view}")

        view.indexes.get(index).map(new QueryableIndex[K, V](_))
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
