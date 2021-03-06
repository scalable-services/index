package services.scalable

import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import com.google.common.primitives.UnsignedBytes
import services.scalable.index.IdGenerator
import services.scalable.index.impl.GrpcByteSerializer

import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters.toScala
import scala.concurrent.{ExecutionContext, Future}

package object index {

  type Bytes = Array[Byte]
  type Tuple = Tuple2[Bytes, Bytes]
  type Pointer = Tuple2[Bytes, String]

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
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5))
      .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 31768)
      /*.startProfile("slow")
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
      .endProfile()*/
      .build()

  def isColEqual[K, V](source: Seq[Tuple2[K, V]], c1: Seq[Tuple2[K, V]])(implicit ordk: Ordering[K], ordv: Ordering[V]): Boolean = {
    if(source.length != c1.length) return false

    c1.zipWithIndex.forall{case ((k, v), idx) =>
      val (k1, v1) = source(idx)
      ordk.equiv(k1, k) && ordv.equiv(v1, v)
    }
  }

  object DefaultComparators {
    implicit val ord = new Ordering[Bytes] {
      val comp = UnsignedBytes.lexicographicalComparator()
      override def compare(x: Bytes, y: Bytes): Int = comp.compare(x, y)
    }
  }

  object DefaultIdGenerators {
    implicit val idGenerator = new IdGenerator {
      override def generateId(ctx: Context): String = UUID.randomUUID().toString
      override def generatePartition(ctx: Context): String = UUID.randomUUID().toString
    }
  }

  object DefaultSerializers {
    implicit val byteSerializer = new Serializer[Bytes] {
      override def serialize(t: Bytes): Array[Byte] = t
      override def deserialize(b: Array[Byte]): Bytes = b
    }

    implicit val grpcBytesSerializer = new GrpcByteSerializer()
  }

  trait IndexError

  object Errors {
    case object LEAF_BLOCK_FULL extends RuntimeException("Leaf is full!") with IndexError
    case object META_BLOCK_FULL extends RuntimeException("Meta is full!") with IndexError

    case class LEAF_DUPLICATE_KEY(keys: Seq[Tuple], inserting: Seq[Tuple]) extends RuntimeException(s"Duplicate elements on leaf!")
      with IndexError
    case class LEAF_KEY_NOT_FOUND(keys: Seq[Bytes]) extends RuntimeException(s"Missing key on leaf") with IndexError

    case class META_DUPLICATE_KEY(keys: Seq[Pointer], inserting: Seq[Pointer]) extends RuntimeException(s"Duplicate elements on meta!")
      with IndexError
    case class META_KEY_NOT_FOUND(keys: Seq[Bytes]) extends RuntimeException(s"Missing key on meta") with IndexError

    case class BLOCK_NOT_FOUD(id: String) extends RuntimeException(s"Block ${id} not found!") with IndexError

    case class DUPLICATE_KEYS(keys: Seq[Tuple]) extends RuntimeException("Duplicate keys") with IndexError

    case class KEY_NOT_FOUND(k: Bytes) extends RuntimeException(s"Key not found!") with IndexError

    case class BLOCK_NOT_SAME_CONTEXT(broot: Option[String], croot: Option[String])
      extends RuntimeException(s"Current block's root ${broot} is not equal to the current root context: ${croot}") with IndexError

    case class INDEX_NOT_FOUND(id: String) extends RuntimeException(s"Index ${id} not found!") with IndexError

    case class INDEX_CREATION_ERROR(id: String) extends RuntimeException(s"There was a problem creating index ${id}!") with IndexError

    case class INDEX_ALREADY_EXISTS(id: String) extends RuntimeException(s"Index ${id} already exists!") with IndexError
  }

}
