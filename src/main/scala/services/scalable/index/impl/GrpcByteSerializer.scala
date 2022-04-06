package services.scalable.index.impl

import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import services.scalable.index._
import services.scalable.index.grpc._

final class GrpcByteSerializer[K, V](implicit val ks: Serializer[K], val vs: Serializer[V]) extends Serializer [Block[K, V]] {

    override def serialize(block: Block[K,V]): Bytes = {
      block match {
        case leaf: Leaf[K,V] =>

          Any.pack(LeafBlock(leaf.id, leaf.partition, leaf.tuples.map { case (k, v) =>
            KVPair(ByteString.copyFrom(ks.serialize(k)), ByteString.copyFrom(vs.serialize(v)))
          }, block.MIN, block.MAX,
            if(leaf.root.isDefined){
              val (partition, id) = leaf.root.get
              Some(RootRef(partition, id))
            } else None
          )).toByteArray

        case meta: Meta[K,V] =>

          Any.pack(MetaBlock(meta.id, meta.partition, meta.pointers.map { case (k, (p, id)) =>
            Pointer(ByteString.copyFrom(ks.serialize(k)), p, id)
          }, meta.MIN, meta.MAX, if(meta.root.isDefined) {
            val (partition, id) = meta.root.get
            Some(RootRef(partition, id))
          }
          else None)).toByteArray
      }
    }

    override def deserialize(bytes: Bytes): Block[K, V] = {

      val parsed = Any.parseFrom(bytes)

      if (parsed.is(LeafBlock)) {

        val leaf = parsed.unpack(LeafBlock)

        val tuples = Array(leaf.tuples.map { t => ks.deserialize(t.key.toByteArray) -> vs.deserialize(t.value.toByteArray) }: _*)

        val block = new Leaf[K,V](leaf.id, leaf.partition, leaf.min, leaf.max, bytes.length)

        block.root = leaf.root.map(r => r.partition -> r.id)

        block.tuples = tuples

        return block
      }

      val meta = parsed.unpack(MetaBlock)

      val pointers = Array(meta.pointers.map { t => ks.deserialize(t.key.toByteArray) -> (t.partition, t.id) }: _*)
      val block = new Meta[K,V](meta.id, meta.partition, meta.min, meta.max, bytes.length)

      block.root = meta.root.map(r => r.partition -> r.id)

      block.pointers = pointers

      block
    }
}
