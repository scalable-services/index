package services.scalable.index

import services.scalable.index.impl.{DefaultCache, GrpcByteSerializer, MemoryStorage}
import scala.concurrent.ExecutionContext

final case class IndexBuilt[K, V]()(implicit val ord: Ordering[K],
                                  val MAX_LEAF_ITEMS: Int,
                                  val MAX_META_ITEMS: Int,
                                  val MAX_N_ITEMS: Long,
                                  val idGenerator: IdGenerator,
                                  val ks: K => String,
                                  val vs: V => String,
                                  val ec: ExecutionContext,
                                  val keySerializer: Serializer[K],
                                  val valueSerializer: Serializer[V],
                                  val storage: Storage,
                                  val serializer: Serializer[Block[K, V]],
                                  val cache: Cache
                                 )

final class IndexBuilder[K, V](implicit val ord: Ordering[K],
                               val MAX_LEAF_ITEMS: Int,
                               val MAX_META_ITEMS: Int,
                               val MAX_N_ITEMS: Long,
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

  def build(): IndexBuilt[K, V] = {

    assert(this.storage != null, "You must provide a storage service!")
    assert(this.serializer != null, "You must provide a serializer for data blocks!")

    new IndexBuilt[K, V]()(ord, MAX_LEAF_ITEMS, MAX_META_ITEMS, MAX_N_ITEMS, idGenerator, ks, vs, ec, keySerializer,
      valueSerializer, storage, serializer, cache)
  }
}

object IndexBuilder {
  def create[K, V](implicit ec: ExecutionContext,
                   ordering: Ordering[K],
                   MAX_LEAF_ITEMS: Int,
                   MAX_META_ITEMS: Int,
                   MAX_N_ITEMS: Long,
                   keySerializer: Serializer[K],
                   valueSerializer: Serializer[V]): IndexBuilder[K, V] =
    new IndexBuilder[K, V]()(ordering, MAX_LEAF_ITEMS, MAX_META_ITEMS, MAX_N_ITEMS, keySerializer, valueSerializer, ec)
}
