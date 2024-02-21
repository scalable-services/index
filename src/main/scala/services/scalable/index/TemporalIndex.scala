package services.scalable.index

import services.scalable.index.grpc._
import scala.concurrent.Future

class TemporalIndex[K, V](val descriptor: TemporalContext)
                         (val indexBuilder: IndexBuilt[K, V],
                          val historyBuilder: IndexBuilt[Long, IndexContext],
                          val cache: com.github.benmanes.caffeine.cache.Cache[(String, Long), Option[QueryableIndex[K, V]]]){

  import DefaultSerializers._
  import indexBuilder._

  protected val index = new QueryableIndex[K, V](descriptor.latest)(indexBuilder)
  protected val history = new QueryableIndex[Long, IndexContext](descriptor.history)(historyBuilder)

  def execute(cmds: Seq[Commands.Command[K, V]]): Future[BatchResult] = {
    index.execute(cmds)
  }

  def snapshot(): Future[(Long, BatchResult)] = {
    val tmp = System.nanoTime()

    history.execute(Seq(
      Commands.Insert(descriptor.history.id, Seq(Tuple3(tmp, index.snapshot(), false)))
    )).map(tmp -> _)
  }

  def find(): IndexContext = index.ctx.currentSnapshot()

  def find(t: Long): Future[Option[IndexContext]] = {
    history.findPath(t).map(_.map { leaf =>
      var pos = leaf.binSearch(t, 0, leaf.tuples.length - 1)._2
      pos = if (pos == leaf.length) pos - 1 else pos

      leaf.tuples(pos)._2
    })
  }

  def findIndex(t: Long): Future[Option[QueryableIndex[K, V]]] = {
    val index = cache.getIfPresent(descriptor.id -> t)

    if(index == null){
      return find(t).map(_.map(new QueryableIndex[K, V](_)(indexBuilder))).map { index =>
        cache.put(descriptor.id -> t, index)
        index
      }
    }

    Future.successful(index)
  }

  def findIndex(): QueryableIndex[K, V] = index

  def save(): Future[TemporalContext] = {
    val ctx = TemporalContext(descriptor.id, index.snapshot(), history.snapshot())

    val indexBlockRefs = index.ctx.newBlocksReferences
    val historyBlockRefs = history.ctx.newBlocksReferences

    val indexBlocks = indexBlockRefs.map { case (id, _) => id -> indexBuilder.cache.get(id) }
      .filter(_._2.isDefined).map { case (id, opt) => id -> opt.get.asInstanceOf[Block[K, V]] }.toMap

    val historyBlocks = historyBlockRefs.map { case (id, _) => id -> historyBuilder.cache.get(id) }
      .filter(_._2.isDefined).map { case (id, opt) => id -> opt.get.asInstanceOf[Block[Long, IndexContext]] }.toMap

    if(indexBlockRefs.size != indexBlocks.size){
      return Future.failed(new RuntimeException("Some index blocks were evicted from cache before saving to disk!"))
    }

    if (historyBlockRefs.size != historyBlocks.size) {
      return Future.failed(new RuntimeException("Some history blocks were evicted from cache before saving to disk!"))
    }

    val blocks = indexBlocks.map { case (id, block) =>
      id -> serializer.serialize(block)
    } ++ historyBlocks.map { case (id, block) =>
      id -> grpcLongIndexContextSerializer.serialize(block)
    }

    storage.save(ctx, blocks).map { _ =>
      ctx
    }
  }

}
