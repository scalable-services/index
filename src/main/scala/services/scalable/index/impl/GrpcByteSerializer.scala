package services.scalable.index.impl

import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream
import org.slf4j.LoggerFactory
import services.scalable.index._
import services.scalable.index.grpc._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

final class GrpcByteSerializer[K, V](implicit val ks: Serializer[K], val vs: Serializer[V]) extends Serializer [Block[K, V]] {

    val logger = LoggerFactory.getLogger(this.getClass)

    val factory = new CompressorStreamFactory()

    override def serialize(block: Block[K,V]): Bytes = {

      val os = new ByteArrayOutputStream()
      val lz4Out = factory
        .createCompressorOutputStream(CompressorStreamFactory.getLZ4Block, os).asInstanceOf[BlockLZ4CompressorOutputStream]

      val input = block match {
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

      logger.info(s"Compressing block ${block.unique_id}...")

      lz4Out.write(input)
      lz4Out.flush()
      lz4Out.finish()

      val buffer = os.toByteArray

      os.flush()
      os.close()

      buffer
    }

    override def deserialize(bytes: Bytes): Block[K, V] = {

      val is = new ByteArrayInputStream(bytes)
      val lz4In = factory.createCompressorInputStream(CompressorStreamFactory.getLZ4Block, is)

      val parsed = Any.parseFrom(lz4In.readAllBytes())

      is.close()
      lz4In.close()

      if (parsed.is(LeafBlock)) {

        val leaf = parsed.unpack(LeafBlock)

        val tuples = Array(leaf.tuples.map { t => ks.deserialize(t.key.toByteArray) -> vs.deserialize(t.value.toByteArray) }: _*)

        val block = new Leaf[K,V](leaf.id, leaf.partition, leaf.min, leaf.max, bytes.length)

        logger.info(s"Decompressing block ${block.unique_id}...")

        block.root = leaf.root.map(r => r.partition -> r.id)

        block.tuples = tuples
        block.isNew = false

        return block
      }

      val meta = parsed.unpack(MetaBlock)

      val pointers = Array(meta.pointers.map { t => ks.deserialize(t.key.toByteArray) -> (t.partition, t.id) }: _*)
      val block = new Meta[K,V](meta.id, meta.partition, meta.min, meta.max, bytes.length)

      logger.info(s"Decompressing block ${block.unique_id}...")

      block.root = meta.root.map(r => r.partition -> r.id)

      block.pointers = pointers
      block.isNew = false

      block
    }
}
