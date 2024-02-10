package services.scalable.index

import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{DefaultCache, GrpcByteSerializer, MemoryStorage}

import java.util.UUID
import scala.concurrent.ExecutionContext

final class IndexBuilder[K, V](val descriptor: IndexContext)(implicit val ord: Ordering[K],
                               val keySerializer: Serializer[K],
                               val valueSerializer: Serializer[V],
                               val ec: ExecutionContext) {

  implicit var storage: Storage = new MemoryStorage()
  implicit var serializer: Serializer[Block[K, V]] = new GrpcByteSerializer[K, V]()
  implicit var cache: Cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)

  implicit var idGenerator: IdGenerator = DefaultIdGenerators.idGenerator

  implicit var ks: K => String = k => k.toString
  implicit var vs: V => String = k => k.toString

  def storage(storage: Storage): IndexBuilder[K, V] = {
    this.storage = storage
    this
  }

  def serializer(serializer: Serializer[Block[K, V]]): IndexBuilder[K, V] = {
    this.serializer = serializer
    this
  }

  def cache(cache: Cache): IndexBuilder[K, V] = {
    this.cache = cache
    this
  }

  def idGenerator(idGenerator: IdGenerator): IndexBuilder[K, V] = {
    this.idGenerator = idGenerator
    this
  }

  def keyToStringConverter(ks: K => String): IndexBuilder[K, V] = {
    this.ks = ks
    this
  }

  def valueToStringConverter(vs: V => String): IndexBuilder[K, V] = {
    this.vs = vs
    this
  }

  def build(): QueryableIndex[K, V] = {

    assert(descriptor != null, "You must provide an index context!")
    assert(this.storage != null, "You must provide a storage service!")
    assert(this.serializer != null, "You must provide a serializer for data blocks!")

    new QueryableIndex[K, V](descriptor)(this)
  }
}

object IndexBuilder {
  def create[K, V](descriptor: IndexContext)
                  (implicit ec: ExecutionContext, ordering: Ordering[K], keySerializer: Serializer[K], valueSerializer: Serializer[V]): IndexBuilder[K, V] =
    new IndexBuilder[K, V](descriptor)(ordering, keySerializer, valueSerializer, ec)
}
