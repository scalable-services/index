package services.scalable.index.impl

import com.google.protobuf.ByteString
import org.slf4j.LoggerFactory
import services.scalable.index._
import com.google.protobuf.any.Any
import services.scalable.index.grpc._

final class GrpcCommandSerializer[K, V](implicit val ks: Serializer[K], val vs: Serializer[V]) extends Serializer [Commands.Command[K, V]] {

  //val logger = LoggerFactory.getLogger(this.getClass)

  override def serialize(cmd: Commands.Command[K, V]): Bytes = {
    cmd match {
      case c: Commands.Insert[K, V] =>
        val list = c.list.map { case (k, v, upsert) =>
          KVPairInsertion(ByteString.copyFrom(ks.serialize(k)), ByteString.copyFrom(vs.serialize(v)), upsert, cmd.version.get)
        }

        Any.pack(InsertCommand(c.indexId, list, c.version)).toByteArray

      case c: Commands.Update[K, V] =>

        val list = c.list.map { case (k, v, version) =>
          KVPair(ByteString.copyFrom(ks.serialize(k)), ByteString.copyFrom(vs.serialize(v)), version.get)
        }

        Any.pack(UpdateCommand(c.indexId, list, c.version)).toByteArray

      case c: Commands.Remove[K, V] =>

        val list = c.keys.map { case (k, version) =>
          KVPair(ByteString.copyFrom(ks.serialize(k)), ByteString.EMPTY, version.get)
        }

        Any.pack(RemoveCommand(c.indexId, list, c.version)).toByteArray
    }
  }

  override def deserialize(buf: Bytes): Commands.Command[K, V] = {
    Any.parseFrom(buf) match {
      case p if p.is(InsertCommand) =>

        val c = p.unpack(InsertCommand)

        val list = c.list.map { pair =>
          Tuple3(
            ks.deserialize(pair.key.toByteArray) ,
            vs.deserialize(pair.value.toByteArray),
            pair.upsert
          )
        }

        Commands.Insert(c.indexId, list, c.version)

      case p if p.is(UpdateCommand) =>

        val c = p.unpack(UpdateCommand)

        val list = c.list.map { pair =>
          Tuple3(
            ks.deserialize(pair.key.toByteArray),
            vs.deserialize(pair.value.toByteArray),
            Some(pair.version)
          )
        }

        Commands.Update(c.indexId, list, c.version)

      case p if p.is(RemoveCommand) =>

        val c = p.unpack(RemoveCommand)

        val list = c.list.map { pair =>
          Tuple2(
            ks.deserialize(pair.key.toByteArray),
            Some(pair.version)
          )
        }

        Commands.Remove(c.indexId, list, c.version)

    }
  }
}
