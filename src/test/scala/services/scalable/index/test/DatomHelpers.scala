package services.scalable.index.test

import com.google.protobuf.ByteString
import services.scalable.index.{Bytes, Serializer}
import services.scalable.index.grpc.test.EAVT
import services.scalable.index.impl.GrpcByteSerializer

import java.nio.ByteBuffer

object DatomHelpers {

  object DatomSerializers {
    implicit def datomSerializer: Serializer[EAVT] = new Serializer[EAVT]{
      override def serialize(t: EAVT): Bytes = EAVT.toByteArray(t)
      override def deserialize(b: Bytes): EAVT = EAVT.parseFrom(b)
    }

    import services.scalable.index.DefaultSerializers._
    implicit val datomEmptyBytesGrpcSerializer = new GrpcByteSerializer[EAVT, Array[Byte]]()
  }

  trait ProductWithId extends Product {
    val id: String
  }

  object DatomIndexType {
    val EAVT = 1
    val VAET = 2
    val AVET = 3
  }

  object DatomFieldType {
    val TEXT = 1
    val NUMBER = 2
    val REF = 3
  }

  object DatomComparators {

    implicit val datomOrdering = new Ordering[EAVT] {

      def compareEAVT(x: EAVT, y: EAVT): Int = {
        if(x.e.isEmpty || y.e.isEmpty) return -1

        var c = x.getE.compareTo(y.getE)

        if(c != 0) return c

        if(x.a.isEmpty || y.a.isEmpty) return c

        c = x.getA.compareTo(y.getA)

        if(c != 0) return c

        if(x.v.isEmpty || y.v.isEmpty) return c

        c = x.valueTpe.compareTo(y.valueTpe)

        if(c != 0) return c

        x.valueTpe match {
          case DatomFieldType.TEXT => x.getV.toStringUtf8.compareTo(y.getV.toStringUtf8)
          case DatomFieldType.NUMBER => x.getV.asReadOnlyByteBuffer().getDouble
            .compareTo(y.getV.asReadOnlyByteBuffer().getDouble)
          case DatomFieldType.REF => x.getV.toStringUtf8.compareTo(y.getV.toStringUtf8)
        }
      }

      def compareAVET(x: EAVT, y: EAVT): Int = {
        if(x.a.isEmpty || y.a.isEmpty) return -1

        var c = x.getA.compareTo(y.getA)

        if(c != 0) return c

        if(x.v.isEmpty || y.v.isEmpty) return c

        c = x.valueTpe.compareTo(y.valueTpe)

        if(c != 0) return c

        c = x.valueTpe match {
          case DatomFieldType.TEXT => x.getV.toStringUtf8.compareTo(y.getV.toStringUtf8)
          case DatomFieldType.NUMBER => x.getV.asReadOnlyByteBuffer().getDouble
            .compareTo(y.getV.asReadOnlyByteBuffer().getDouble)
          case DatomFieldType.REF => x.getV.toStringUtf8.compareTo(y.getV.toStringUtf8)
        }

        if(c != 0) return c

        if(x.e.isEmpty || y.e.isEmpty) return c

        x.getE.compareTo(y.getE)
      }

      def compareVAET(x: EAVT, y: EAVT): Int = {
        if(x.v.isEmpty || y.v.isEmpty) return -1

        var c = x.valueTpe.compareTo(y.valueTpe)

        if(c != 0) return c

        c = x.valueTpe match {
          case DatomFieldType.TEXT => x.getV.toStringUtf8.compareTo(y.getV.toStringUtf8)
          case DatomFieldType.NUMBER => x.getV.asReadOnlyByteBuffer().getDouble
            .compareTo(y.getV.asReadOnlyByteBuffer().getDouble)
          case DatomFieldType.REF => x.getV.toStringUtf8.compareTo(y.getV.toStringUtf8)
        }

        if(c != 0) return c

        if(x.a.isEmpty || y.a.isEmpty) return c

        c = x.getA.compareTo(y.getA)

        if(c != 0) return c

        if(x.e.isEmpty || y.e.isEmpty) return c

        x.getE.compareTo(y.getE)
      }

      override def compare(x: EAVT, y: EAVT): Int = {
        val c = x.indexTpe.compareTo(y.indexTpe)

        if(c != 0) return c

        x.indexTpe match {
          case DatomIndexType.EAVT => compareEAVT(x, y)
          case DatomIndexType.AVET => compareAVET(x, y)
          case DatomIndexType.VAET => compareVAET(x, y)
        }
      }

    }

    /*val eavOrdering = new Ordering[EAVT] {
      override def compare(x: EAVT, y: EAVT): Int = {
        var c = x.e.compareTo(y.e)

        if(c != 0) return c

        c = x.a.compareTo(y.a)

        if(c != 0) return c

        c = x.tpe.compareTo(y.tpe)

        if(c != 0) return c

        x.tpe match {
          case DatomFieldType.TEXT => x.v.toStringUtf8.compareTo(y.v.toStringUtf8)
          case DatomFieldType.NUMBER => x.v.asReadOnlyByteBuffer().getDouble
            .compareTo(y.v.asReadOnlyByteBuffer().getDouble)
          case DatomFieldType.REF => x.v.toStringUtf8.compareTo(y.v.toStringUtf8)
        }
      }
    }

    val aveOrdering = new Ordering[EAVT] {
      override def compare(x: EAVT, y: EAVT): Int = {
        var c = x.a.compareTo(y.a)

        if(c != 0) return c

        c = x.tpe.compareTo(y.tpe)

        if(c != 0) return c

        c = x.tpe match {
          case DatomFieldType.TEXT => x.v.toStringUtf8.compareTo(y.v.toStringUtf8)
          case DatomFieldType.NUMBER => x.v.asReadOnlyByteBuffer().getDouble
            .compareTo(y.v.asReadOnlyByteBuffer().getDouble)
          case DatomFieldType.REF => x.v.toStringUtf8.compareTo(y.v.toStringUtf8)
        }

        if(c != 0) return c

        x.e.compareTo(y.e)
      }
    }

    val vaeOrdering = new Ordering[EAVT] {
      override def compare(x: EAVT, y: EAVT): Int = {
        var c = x.tpe.compareTo(y.tpe)

        if(c != 0) return c

        c = x.tpe match {
          case DatomFieldType.TEXT => x.v.toStringUtf8.compareTo(y.v.toStringUtf8)
          case DatomFieldType.NUMBER => x.v.asReadOnlyByteBuffer().getDouble
            .compareTo(y.v.asReadOnlyByteBuffer().getDouble)
          case DatomFieldType.REF => x.v.toStringUtf8.compareTo(y.v.toStringUtf8)
        }

        if(c != 0) return c

        c = x.a.compareTo(y.a)

        if(c != 0) return c

        x.e.compareTo(y.e)
      }
    }*/

  }

  object DatomPrinters {

    def eavtToString(eavt: EAVT): String = {
      eavt.indexTpe match {
        case DatomIndexType.EAVT =>
          ("[EAVT]", eavt.e, eavt.a, eavt.valueTpe match {
          case DatomFieldType.TEXT => eavt.v.map(_.toStringUtf8)
          case DatomFieldType.NUMBER => eavt.v.map(_.asReadOnlyByteBuffer().getDouble)
          case DatomFieldType.REF => eavt.v.map(_.toStringUtf8)
        }, eavt.t).toString()

        case DatomIndexType.VAET =>
          ("[VAET]", eavt.valueTpe match {
          case DatomFieldType.TEXT  => eavt.v.map(_.toStringUtf8)
          case DatomFieldType.NUMBER => eavt.v.map(_.asReadOnlyByteBuffer().getDouble)
          case DatomFieldType.REF => eavt.v.map(_.toStringUtf8)
        }, eavt.a, eavt.e, eavt.t).toString()

        case DatomIndexType.AVET =>
          ("[AVET]", eavt.a, eavt.valueTpe match {
            case DatomFieldType.TEXT => eavt.v.map(_.toStringUtf8)
            case DatomFieldType.NUMBER => eavt.v.map(_.asReadOnlyByteBuffer().getDouble)
            case DatomFieldType.REF => eavt.v.map(_.toStringUtf8)
          }, eavt.e, eavt.t).toString()

      }
    }
  }

  def caseClassToEAVT[T <: ProductWithId](p: T): Seq[EAVT] = {
    val className = p.getClass.getName
    var datoms = Seq.empty[EAVT]

    val time = System.nanoTime()

    for(i<-0 until p.productArity){
      //println(s"field ${p.productElementName(i)}: ${p.productElement(i)}")

      val field = p.productElementName(i)
      val fieldWithNamepsace = s"${className}/${field}"
      val value = p.productElement(i)

      value match {
        case v: String =>

          val d = EAVT()
            .withE(p.id)
            .withA(fieldWithNamepsace)
            .withV(ByteString.copyFrom(v.getBytes("UTF-8")))
            .withT(time)
            .withValueTpe(DatomFieldType.TEXT)
            .withIndexTpe(DatomIndexType.EAVT)

          datoms :+= d

        case v: Number =>

          val d = EAVT()
            .withE(p.id)
            .withA(fieldWithNamepsace)
            .withV(ByteString.copyFrom(ByteBuffer.allocate(8).putDouble(v.doubleValue()).flip()))
            .withT(time)
            .withValueTpe(DatomFieldType.NUMBER)
            .withIndexTpe(DatomIndexType.EAVT)

          datoms :+= d

        case v: ProductWithId => datoms :++= caseClassToEAVT(v)

        /*case v: Iterable[ProductWithId] =>
          val it = v.iterator

          while(it.hasNext){
            val child = it.next()

            datoms = datoms :+ EAVT()
              .withE(p.id)
              .withA(fieldWithNamepsace)
              .withV(ByteString.copyFrom(child.id.getBytes("UTF-8")))
              .withT(time)
              .withTpe(DatomFieldType.REF)

            // datoms :++= caseClassToEAVT(child)
          }*/

        case v: Iterable[_] =>
          val it = v.iterator

          while(it.hasNext){
            val child = it.next()

            datoms = datoms :+ (child match {
              case v: String =>

                EAVT()
                  .withE(p.id)
                  .withA(fieldWithNamepsace)
                  .withV(ByteString.copyFrom(v.getBytes("UTF-8")))
                  .withT(time)
                  .withValueTpe(DatomFieldType.TEXT)
                  .withIndexTpe(DatomIndexType.EAVT)

              case v: Number =>

                EAVT()
                  .withE(p.id)
                  .withA(fieldWithNamepsace)
                  .withV(ByteString.copyFrom(ByteBuffer.allocate(8).putDouble(v.doubleValue()).flip()))
                  .withT(time)
                  .withValueTpe(DatomFieldType.NUMBER)
                  .withIndexTpe(DatomIndexType.EAVT)

              case v: ProductWithId =>
                EAVT()
                  .withE(p.id)
                  .withA(fieldWithNamepsace)
                  .withV(ByteString.copyFrom(v.id.getBytes("UTF-8")))
                  .withT(time)
                  .withValueTpe(DatomFieldType.REF)
                  .withIndexTpe(DatomIndexType.EAVT)

            })
          }

        case _ =>
          throw new RuntimeException("Field type not supported!")
      }
    }

    datoms
  }

}
