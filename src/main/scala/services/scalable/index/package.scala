package services.scalable

import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import com.google.common.primitives.UnsignedBytes
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import services.scalable.index.grpc.{DBContext, DecimalValue, IndexView}
import services.scalable.index.impl.GrpcByteSerializer

import java.math.{BigInteger, MathContext}
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters.toScala
import scala.concurrent.{ExecutionContext, Future}

package object index {

  type Bytes = Array[Byte]
  type Tuple[K, V] = Tuple3[K, V, String]
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

    implicit val ordInt = new Ordering[Int] {
      override def compare(x: Int, y: Int): Int = x.compare(y)
    }

    implicit val ordLong = new Ordering[Long] {
      override def compare(x: Long, y: Long): Int = x.compare(y)
    }

    implicit val ordBigInt = new Ordering[BigInt] {
      override def compare(x: BigInt, y: BigInt): Int = x.compare(y)
    }

    implicit val ordBigDecimal = new Ordering[BigDecimal] {
      override def compare(x: BigDecimal, y: BigDecimal): Int = x.compare(y)
    }

    implicit val ordShort = new Ordering[Short] {
      override def compare(x: Short, y: Short): Int = x.compare(y)
    }

    implicit val ordByte = new Ordering[Byte] {
      override def compare(x: Byte, y: Byte): Int = x.compare(y)
    }

    implicit val ordChar = new Ordering[Char] {
      override def compare(x: Char, y: Char): Int = x.compare(y)
    }

    implicit val ordBoolean = new Ordering[Boolean] {
      override def compare(x: Boolean, y: Boolean): Int = x.compare(y)
    }

    implicit val ordString = new Ordering[String] {
      override def compare(x: String, y: String): Int = x.compare(y)
    }
  }

  object DefaultIdGenerators {
    implicit val idGenerator = new IdGenerator {
      override def generateId[K,V](ctx: Context[K,V]): String = UUID.randomUUID().toString
      override def generatePartition[K,V](ctx: Context[K,V]): String = UUID.randomUUID().toString
    }
  }

  object DefaultSerializers {
    implicit val bytesSerializer = new Serializer[Bytes] {
      override def serialize(t: Bytes): Array[Byte] = t
      override def deserialize(b: Array[Byte]): Bytes = b
    }

    implicit val stringSerializer = new Serializer[String] {
      override def serialize(t: String): Bytes = t.getBytes("UTF-8")
      override def deserialize(b: Bytes): String = new String(b, "UTF-8")
    }

    implicit val intSerializer = new Serializer[Int] {
      override def serialize(t: Int): Bytes = {
        ByteBuffer.allocate(4).putInt(t).array()
      }

      override def deserialize(b: Bytes): Int = {
        ByteBuffer.wrap(b).getInt()
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

    implicit val shortSerializer = new Serializer[Short] {
      override def serialize(t: Short): Bytes = {
        ByteBuffer.allocate(2).putShort(t).array()
      }

      override def deserialize(b: Bytes): Short = {
        ByteBuffer.wrap(b).getShort()
      }
    }

    implicit val byteSerializer = new Serializer[Byte] {
      override def serialize(t: Byte): Bytes = {
        Array(t)
      }

      override def deserialize(b: Bytes): Byte = {
        b(0)
      }
    }

    implicit val charSerializer = new Serializer[Char] {
      override def serialize(t: Char): Bytes = {
        Array(t.toByte)
      }

      override def deserialize(b: Bytes): Char = {
        b(0).toChar
      }
    }

    implicit val bigintSerializer = new Serializer[BigInt] {
      override def serialize(t: BigInt): Bytes = {
        t.toByteArray
      }

      override def deserialize(b: Bytes): BigInt = {
        BigInt(b)
      }
    }

    implicit val bigDecimalSerializer = new Serializer[BigDecimal] {
      override def serialize(t: BigDecimal): Bytes = {
        Any.pack(DecimalValue()
          .withScale(t.scale)
          .withPrecision(t.precision)
          .withValue(ByteString.copyFrom(t.toString().getBytes("UTF-8")))).toByteArray
      }

      override def deserialize(b: Bytes): BigDecimal = {
        val dec = Any.parseFrom(b).unpack(DecimalValue)
        val mc = new MathContext(dec.scale)
        BigDecimal(dec.value.toString("UTF-8"), mc)
      }
    }

    implicit val booleanSerializer = new Serializer[Boolean] {
      override def serialize(t: Boolean): Bytes = {
        Array(if(t) 1.toByte else 0.toByte)
      }

      override def deserialize(b: Bytes): Boolean = {
        b(0) match {
          case x if x == 1.toByte => true
          case _ => false
        }
      }
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

    implicit val grpcHistorySerializer = new GrpcByteSerializer[Long, IndexView]()
    implicit val grpcBytesSerializer = new GrpcByteSerializer[Bytes, Bytes]()
    implicit val grpcDBContextSerializer = new GrpcByteSerializer[Bytes, DBContext]()
  }
}
