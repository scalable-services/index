package services.scalable

import com.google.common.base.Charsets
import com.google.common.primitives.UnsignedBytes
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import services.scalable.index.grpc.{DecimalValue, IndexContext, TemporalContext}
import services.scalable.index.impl.{GrpcByteSerializer, GrpcCommandSerializer}

import java.math.MathContext
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.CompletionStage
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps

package object index {

  type Bytes = Array[Byte]
  type Tuple[K, V] = Tuple3[K, V, String]
  //type Pointer[K] = Tuple2[K, (String, String)]

  case class Pointer(partition: String, id: String, nElements: Long = 0L, level: Int = 0) {
    def unique_id: (String, String) = (partition, id)
  }

  implicit def toScalaFuture[T](cs: CompletionStage[T]) = cs.asScala

  def serialiseFutures[A, B](l: Iterable[A])(fn: A ⇒ Future[B])(implicit ec: ExecutionContext): Future[List[B]] =
    l.foldLeft(Future(List.empty[B])) {
      (previousFuture, next) ⇒
        for {
          previousResults <- previousFuture
          next <- fn(next)
        } yield previousResults :+ next
    }

  object DefaultComparators {
    implicit val bytesOrd = new Ordering[Bytes] {
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
      override def generateIndexId(): String = UUID.randomUUID.toString
      override def generateBlockId[K,V](ctx: Context[K,V]): String = UUID.randomUUID().toString
      override def generateBlockPartition[K,V](ctx: Context[K,V]): String = UUID.randomUUID().toString
    }
  }

  object DefaultPrinters {
    implicit def byteArrayToStringPrinter(k: Array[Byte]): String = new String(k, Charsets.UTF_8)
    implicit def longToStringPrinter(k: Long): String = k.toString

    implicit def intToStringPrinter(k: Int): String = k.toString

    implicit def indexContextStringPrinter(k: IndexContext): String = k.id
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
        DecimalValue.toByteArray(DecimalValue()
          .withScale(t.scale)
          .withPrecision(t.precision)
          .withValue(ByteString.copyFrom(t.toString().getBytes("UTF-8"))))
      }

      override def deserialize(b: Bytes): BigDecimal = {
        val dec = DecimalValue.parseFrom(b)
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

    implicit val dbContextSerializer = new Serializer[TemporalContext] {
      override def serialize(t: TemporalContext): Array[Byte] = Any.pack(t).toByteArray
      override def deserialize(b: Array[Byte]): TemporalContext = Any.parseFrom(b).unpack(TemporalContext)
    }

    implicit val indexContextSerializer = new Serializer[IndexContext] {
      override def serialize(t: IndexContext): Array[Byte] = Any.pack(t).toByteArray
      override def deserialize(b: Array[Byte]): IndexContext = Any.parseFrom(b).unpack(IndexContext)
    }

    implicit val grpcLongIndexContextSerializer = new GrpcByteSerializer[Long, IndexContext]()

    implicit val grpcLongTemporalContextSerializer = new GrpcByteSerializer[Long, TemporalContext]()
    implicit val grpcBytesIndexContextSerializer = new GrpcByteSerializer[Bytes, IndexContext]()

    implicit val grpcBytesBytesSerializer = new GrpcByteSerializer[Bytes, Bytes]()
    implicit val grpcBytesBytesCommandSerializer = new GrpcCommandSerializer[Bytes, Bytes]()
  }
}
