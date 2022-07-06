package services.scalable

import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import com.google.common.primitives.UnsignedBytes
import com.google.protobuf.any.Any
import services.scalable.index.IdGenerator
import services.scalable.index.grpc.{DBContext, IndexContext, IndexView}
import services.scalable.index.impl.GrpcByteSerializer

import java.nio.ByteBuffer
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters.toScala
import scala.concurrent.{ExecutionContext, Future}

package object index {

  type Bytes = Array[Byte]
  type Tuple[K, V] = Tuple2[K, V]
  //type Pointer[K] = Tuple2[K, (String, String)]

  case class Pointer(partition: String, id: String, nElements: Long = 0L, level: Int = 0) {
    def unique_id: (String, String) = (partition, id)
  }

  implicit def toScalaFuture[T](cs: CompletionStage[T]) = toScala[T](cs)

  def serialiseFutures[A, B](l: Iterable[A])(fn: A ⇒ Future[B])(implicit ec: ExecutionContext): Future[List[B]] =
    l.foldLeft(Future(List.empty[B])) {
      (previousFuture, next) ⇒
        for {
          previousResults <- previousFuture
          next <- fn(next)
        } yield previousResults :+ next
    }

  val loader =
    DriverConfigLoader.programmaticBuilder()
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(30))
      .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 31768)
      .withInt(DefaultDriverOption.SESSION_LEAK_THRESHOLD, 1000)
      .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
      .withString(DefaultDriverOption.RECONNECTION_POLICY_CLASS, "ExponentialReconnectionPolicy")
      .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, java.time.Duration.ofSeconds(1))
      .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, java.time.Duration.ofSeconds(10))
      /*.startProfile("slow")
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
      .endProfile()*/
      .build()

  object DefaultComparators {
    implicit val ord = new Ordering[Bytes] {
      val comp = UnsignedBytes.lexicographicalComparator()
      override def compare(x: Bytes, y: Bytes): Int = comp.compare(x, y)
    }
  }

  object DefaultIdGenerators {
    implicit val idGenerator = new IdGenerator {
      override def generateId[K,V](ctx: Context[K,V]): String = UUID.randomUUID().toString
      override def generatePartition[K,V](ctx: Context[K,V]): String = UUID.randomUUID().toString
    }
  }

  object DefaultSerializers {
    implicit val byteSerializer = new Serializer[Bytes] {
      override def serialize(t: Bytes): Array[Byte] = t
      override def deserialize(b: Array[Byte]): Bytes = b
    }

    implicit val dbContextSerializer = new Serializer[DBContext] {
      override def serialize(t: DBContext): Array[Byte] = Any.pack(t).toByteArray
      override def deserialize(b: Array[Byte]): DBContext = Any.parseFrom(b).unpack(DBContext)
    }

    implicit val ctxSerializer = new Serializer[IndexView] {
      override def serialize(t: IndexView): Bytes = {
        Any.pack(t).toByteArray
      }
      override def deserialize(b: Bytes): IndexView = {
        Any.parseFrom(b).unpack(IndexView)
      }
    }

    implicit val dbCtxSerializer = new Serializer[DBContext] {
      override def serialize(t: DBContext): Bytes = {
        Any.pack(t).toByteArray
      }
      override def deserialize(b: Bytes): DBContext = {
        Any.parseFrom(b).unpack(DBContext)
      }
    }

    implicit val longSerializer = new Serializer[Long] {
      override def serialize(t: Long): Bytes = {
        ByteBuffer.allocate(8).putLong(t).array()
      }
      override def deserialize(b: Bytes): Long = {
        ByteBuffer.wrap(b).getLong()
      }
    }

    implicit val grpcHistorySerializer = new GrpcByteSerializer[Long, IndexView]()
    implicit val grpcBytesSerializer = new GrpcByteSerializer[Bytes, Bytes]()
    implicit val grpcDBContextSerializer = new GrpcByteSerializer[Bytes, DBContext]()
  }
}
