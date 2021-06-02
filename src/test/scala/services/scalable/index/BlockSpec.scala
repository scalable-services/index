package services.scalable.index

import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.impl.{CassandraStorage, DefaultCache, DefaultContext}

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

class BlockSpec extends Repeatable {

  val logger = LoggerFactory.getLogger(this.getClass)

  override val times = 1000

  "serialization " must "be equal to deserialization" in {

    val PARTITION = "p0"

    val NUM_ENTRIES = 1000
    val MAX = NUM_ENTRIES
    val MIN = MAX/2

    val n = NUM_ENTRIES

    var list1 = Seq.empty[Tuple2[Bytes, Bytes]]
    var list2 = Seq.empty[Tuple2[Bytes, String]]

    import DefaultComparators._
    implicit val serializer = DefaultSerializers.grpcBytesSerializer

    implicit  val cache = new DefaultCache()
    val upsert = false

    for(i<-0 until n){
      val k = RandomStringUtils.randomAlphanumeric(5).getBytes("UTF-8")
      val v = k.clone()//RandomStringUtils.randomAlphanumeric(5)

      if(!list1.exists{case (k1, _) => ord.equiv(k, k1)}){
        list1 = list1 :+ k -> v
        list2 = list2 :+ k -> UUID.randomUUID.toString()
      }
    }

    val leaf1 = new Leaf(UUID.randomUUID().toString, PARTITION, MIN, MAX)
    leaf1.insert(list1, upsert)

    assert(isColEqual(list1.sortBy(_._1), leaf1.tuples))

    val buf = serializer.serialize(leaf1)
    val leaf2 = serializer.deserialize(buf).asInstanceOf[Leaf]

    val l1 = leaf1.tuples
    val l2 = leaf2.tuples

    logger.debug(s"l1: ${l1.map{case (k, v) => new String(k)}}\n")
    logger.debug(s"l2: ${l2.map{case (k, v) => new String(k)}}\n")

    assert(isColEqual(l1, l2))

    val indexId = "test_index"

    implicit val storage = new CassandraStorage(TestConfig.KEYSPACE, NUM_ENTRIES, NUM_ENTRIES)
    implicit val ctx = new DefaultContext(indexId, None, NUM_ENTRIES, NUM_ENTRIES)

    val meta1 = ctx.createMeta()
    meta1.insert(list2)

    assert(isColEqual(list2.sortBy(_._1), meta1.pointers))

    val buf2 = serializer.serialize(meta1)
    val meta2 = serializer.deserialize(buf2).asInstanceOf[Meta]

    val m1 = meta1.pointers.toSeq
    val m2 = meta2.pointers.toSeq

    logger.debug(s"m1: ${m1.map{case (k, v) => new String(k)}}\n")
    logger.debug(s"m2: ${m2.map{case (k, v) => new String(k)}}\n")

    assert(isColEqual(m1, m2))

  }

}
