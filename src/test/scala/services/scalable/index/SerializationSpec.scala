package services.scalable.index

import org.scalatest.flatspec.AnyFlatSpec
import services.scalable.index.grpc.LeafBlock

import java.util.UUID

class SerializationSpec extends AnyFlatSpec {

  "it" should "serialize correctly" in {

    import DefaultSerializers._

    val leaf = new Leaf[Bytes, Bytes](UUID.randomUUID.toString, UUID.randomUUID.toString, 3, 3)

    val buf = grpcBytesBytesSerializer.serialize(leaf)
    val leafBack = grpcBytesBytesSerializer.deserialize(buf).asInstanceOf[Leaf[Bytes, Bytes]]

    println(leafBack.inOrder().map{case (k, v, _) => new String(k)})

    println()
  }

}
