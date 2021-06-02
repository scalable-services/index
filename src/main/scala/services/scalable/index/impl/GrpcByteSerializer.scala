package services.scalable.index.impl

import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import services.scalable.index._
import services.scalable.index.grpc._

final class GrpcByteSerializer[K, V](implicit val ks: Serializer[K], val vs: Serializer[V]) extends Serializer [Block[K,V]] {

    override def serialize(block: Block[K,V]): Bytes = {
      block match {
        case leaf: Leaf[K,V] =>

          Any.pack(LeafBlock(leaf.id, leaf.partition, leaf.tuples.map { case (k, v) =>
            Tuple(ByteString.copyFrom(ks.serialize(k)), ByteString.copyFrom(vs.serialize(v)))
          }, block.MIN, block.MAX, if(leaf.root.isDefined) LeafBlock.OptionalRoot.Root(leaf.root.get)
          else LeafBlock.OptionalRoot.Empty)).toByteArray

        case meta: Meta[K,V] =>

          Any.pack(MetaBlock(meta.id, meta.partition, meta.pointers.map { case (k, c) =>
            Pointer(ByteString.copyFrom(ks.serialize(k)), c)
          }, meta.MIN, meta.MAX, if(meta.root.isDefined) MetaBlock.OptionalRoot.Root(meta.root.get)
          else MetaBlock.OptionalRoot.Empty)).toByteArray
      }
    }

    override def deserialize(bytes: Bytes): Block[K,V] = {

      val parsed = Any.parseFrom(bytes)

      if (parsed.is(LeafBlock)) {

        val leaf = parsed.unpack(LeafBlock)

        val tuples = Array(leaf.tuples.map { t => ks.deserialize(t.key.toByteArray) -> vs.deserialize(t.value.toByteArray) }: _*)

        val block = new Leaf[K,V](leaf.id, leaf.partition, leaf.min, leaf.max, bytes.length)

        block.root = leaf.optionalRoot.root

        block.tuples = tuples

        return block
      }

      val meta = parsed.unpack(MetaBlock)

      val pointers = Array(meta.pointers.map { t => ks.deserialize(t.key.toByteArray) -> t.link }: _*)
      val block = new Meta[K,V](meta.id, meta.partition, meta.min, meta.max, bytes.length)

      block.root = meta.optionalRoot.root

      block.pointers = pointers

      block
    }
}
