package services.scalable.index.impl

import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import services.scalable.index._
import services.scalable.index.grpc._

import scala.collection.mutable.ArrayBuffer

final class GrpcByteSerializer() extends Serializer [Block] {

    override def serialize(block: Block): Bytes = {
      block match {
        case leaf: Leaf =>

          Any.pack(LeafBlock(leaf.id, leaf.partition, leaf.tuples.map { case (k, v) =>
            Tuple(ByteString.copyFrom(k), ByteString.copyFrom(v))
          }, block.MIN, block.MAX, if(leaf.root.isDefined) LeafBlock.OptionalRoot.Root(leaf.root.get)
          else LeafBlock.OptionalRoot.Empty)).toByteArray

        case meta: Meta =>

          Any.pack(MetaBlock(meta.id, meta.partition, meta.pointers.map { case (k, c) =>
            Pointer(ByteString.copyFrom(k), c)
          }, meta.MIN, meta.MAX, if(meta.root.isDefined) MetaBlock.OptionalRoot.Root(meta.root.get)
          else MetaBlock.OptionalRoot.Empty)).toByteArray
      }
    }

    override def deserialize(bytes: Bytes): Block = {

      val parsed = Any.parseFrom(bytes)

      if (parsed.is(LeafBlock)) {

        val leaf = parsed.unpack(LeafBlock)

        val tuples = Array(leaf.tuples.map { t => t.key.toByteArray -> t.value.toByteArray }: _*)

        val block = new Leaf(leaf.id, leaf.partition, leaf.min, leaf.max, bytes.length)

        block.root = leaf.optionalRoot.root

        block.tuples = tuples

        return block
      }

      val meta = parsed.unpack(MetaBlock)

      val pointers = Array(meta.pointers.map { t => t.key.toByteArray -> t.link }: _*)
      val block = new Meta(meta.id, meta.partition, meta.min, meta.max, bytes.length)

      block.root = meta.optionalRoot.root

      block.pointers = pointers

      block
    }
}
