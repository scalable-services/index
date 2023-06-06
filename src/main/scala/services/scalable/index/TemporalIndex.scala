package services.scalable.index

import services.scalable.index.grpc._
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class TemporalIndex[K, V](private val tctx: TemporalContext)
                         (val indexBuilder: IndexBuilder[K, V],
                          val historyBuilder: IndexBuilder[Long, IndexContext]){

  import indexBuilder._
  import DefaultSerializers._

  protected val index = new QueryableIndex[K, V](tctx.latest)(indexBuilder)
  protected val history = new QueryableIndex[Long, IndexContext](tctx.history)(historyBuilder)

  def execute(cmds: Seq[Commands.Command[K, V]]): Future[BatchResult] = {
    index.execute(cmds)
  }

  def snapshot(): Future[(Long, BatchResult)] = {
    val tmp = System.nanoTime()

    history.execute(Seq(
      Commands.Insert(tctx.history.id, Seq(Tuple3(tmp, index.snapshot(), false)))
    )).map(tmp -> _)
  }

  def find() = index.snapshot()

  def find(t: Long): Future[Option[IndexContext]] = {
    history.findPath(t).map(_.map { leaf =>
      var pos = leaf.binSearch(t, 0, leaf.tuples.length - 1)._2
      pos = if (pos == leaf.length) pos - 1 else pos

      leaf.tuples(pos)._2
    })
  }

  def findIndex(t: Long): Future[Option[QueryableIndex[K, V]]] = {
    find(t).map(_.map(new QueryableIndex[K, V](_)(indexBuilder)))
  }

  def findIndex(): QueryableIndex[K, V] = index

  def save(): Future[TemporalContext] = {
    val ctx = TemporalContext(tctx.id, index.snapshot(), history.snapshot())

    val blocks = index.tmpCtx.newBlocks.map { case (id, block) =>
      id -> serializer.serialize(block)
    } ++ history.tmpCtx.newBlocks.map { case (id, block) =>
      id -> grpcLongIndexContextSerializer.serialize(block)
    }

    storage.save(ctx, blocks.toMap).map { _ =>
      ctx
    }
  }

}
