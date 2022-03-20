package services.scalable

import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import com.google.common.primitives.UnsignedBytes
import com.google.protobuf.any.Any
import services.scalable.index.IdGenerator
import services.scalable.index.grpc.IndexContext
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
  type Pointer[K] = Tuple2[K, String]

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

  def isColEqual[K, V](source: Seq[Tuple2[K, V]], target: Seq[Tuple2[K, V]])(implicit ordk: Ordering[K], ordv: Ordering[V]): Boolean = {
    if(target.length != source.length) return false
    for(i<-0 until source.length){
      val (ks, vs) = source(i)
      val (kt, vt) = target(i)

      if(!(ordk.equiv(kt, ks) && ordv.equiv(vt, vs))){
        return false
      }
    }

    true
  }

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

    implicit val ctxSerializer = new Serializer[IndexContext] {
      override def serialize(t: IndexContext): Bytes = {
        Any.pack(t).toByteArray
      }
      override def deserialize(b: Bytes): IndexContext = {
        Any.parseFrom(b).unpack(IndexContext)
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

    implicit val grpcHistorySerializer = new GrpcByteSerializer[Long, IndexContext]()
    implicit val bytesSerializer = new GrpcByteSerializer[Bytes, Bytes]()
  }
}
