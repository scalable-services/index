package services.scalable.index

import services.scalable.index.grpc._

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

case class DBExecutionResult[K, V](ok: Boolean = false,
                                   ctx: Option[DBContext] = None,
                                   blocks: Map[(String, String), Array[Byte]] = Map.empty[(String, String), Array[Byte]])

class DB[K, V](dbctx: DBContext)(implicit val ec: ExecutionContext,
                               val storage: Storage,
                               val serializer: Serializer[Block[K, V]],
                               val cache: Cache,
                               val ord: Ordering[K],
                               val idGenerator: IdGenerator){
  import DefaultSerializers._

  var ctx = dbctx

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

    Future.sequence(groups.map{case (id, cmds) => exec(id, cmds)}).flatMap {
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

            Future.successful(DBExecutionResult(true,
              Some(dbctx
                .withLatest(view)), ctxs.map(_.blocks).foldLeft(TrieMap.empty[(String, String), Block[K, V]]){ case (p, n) =>
                p ++ n
              }.map{case (id, block) => id -> serializer.serialize(block)}.toMap))

          case Some(history) =>

            history.insert(Seq(time -> view)).map {
              case n if n == 1 =>
                DBExecutionResult(true,
                  Some(dbctx
                    .withLatest(view)
                    .withHistory(history.save())), ctxs.map(_.blocks).foldLeft(TrieMap.empty[(String, String), Block[K, V]]){ case (p, n) =>
                    p ++ n
                  }.map{case (id, block) => id -> serializer.serialize(block)}.toMap
                    ++ history.ctx.blocks.map{case (id, block) => id -> grpcHistorySerializer.serialize(block)})

              case _ => DBExecutionResult[K, V](false)
            }
        }


    }
  }
}
