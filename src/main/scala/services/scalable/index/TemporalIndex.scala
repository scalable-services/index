package services.scalable.index

import services.scalable.index.grpc._
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class TemporalIndex[K, V](private val tctx: TemporalContext)(implicit val ec: ExecutionContext,
                                                     val storage: Storage,
                                                     val serializer: Serializer[Block[K, V]],
                                                     val cache: Cache,
                                                     val ord: Ordering[K],
                                                     val idGenerator: IdGenerator){
  import DefaultSerializers._

  protected val index = new QueryableIndex[K, V](tctx.latest)
  protected val history = new QueryableIndex[Long, IndexContext](tctx.history)

  def execute(cmds: Seq[Commands.Command[K, V]]): Future[Boolean] = {
    index.execute(cmds)
  }

  def snapshot(): Future[Boolean] = {
    val tmp = System.nanoTime()

    history.execute(Seq(
      Commands.Insert(tctx.history.id, Seq(tmp -> index.snapshot()), false)
    ))
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
    find(t).map(_.map(new QueryableIndex[K, V](_)))
  }

  def findIndex(): QueryableIndex[K, V] = index

  def save(): Future[TemporalContext] = {
    val ctx = TemporalContext(tctx.id, index.snapshot(), history.snapshot())

    val blocks = index.ctx.getBlocks().map { case (id, block) =>
      id -> serializer.serialize(block)
    } ++ history.ctx.getBlocks().map { case (id, block) =>
      id -> grpcIndexSerializer.serialize(block)
    }

    storage.save(ctx, blocks).map { _ =>
      ctx
    }
  }

}