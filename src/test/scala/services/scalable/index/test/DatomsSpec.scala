package services.scalable.index.test

import ch.qos.logback.classic.{Level, Logger}
import com.google.protobuf.ByteString
import io.netty.util.internal.ThreadLocalRandom
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.IndexContext
import services.scalable.index.{Bytes, DefaultComparators, DefaultPrinters, DefaultSerializers, IndexBuilder, QueryableIndex}
import services.scalable.index.grpc.test.EAVT
import services.scalable.index.impl.MemoryStorage
import services.scalable.index.test.DatomHelpers.{DatomComparators, DatomFieldType, DatomIndexType, DatomPrinters, DatomSerializers, ProductWithId, caseClassToEAVT}

import java.nio.ByteBuffer
import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class DatomsSpec extends Repeatable with Matchers {

  LoggerFactory.getLogger("services.scalable.index").asInstanceOf[Logger].setLevel(Level.INFO)
 // LoggerFactory.getLogger("services.scalable.index.impl.GrpcByteSerializer").asInstanceOf[Logger].setLevel(Level.INFO)

  override val times: Int = 1

  "operations" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()

    type K = EAVT
    type V = Bytes

    case class Actor(id: String, name: String, luckyNumbers: Seq[Int], luckyNames: Seq[String]) extends ProductWithId
    case class Movie(name: String, id: String, year: Long, actors: Seq[Actor]) extends ProductWithId

    val kate = Actor("3", "Kate", Seq(3, 8, 17), Seq("moon", "sun"))
    val caprio = Actor("2", "Leonardo Di Caprio", Seq(1, 7), Seq("moon"))

    var datoms = caseClassToEAVT(caprio) ++ caseClassToEAVT(kate)

    datoms = datoms ++ caseClassToEAVT(Movie("Titanic", "Movie-1", 1997, Seq(caprio, kate)))

    var all = datoms

    for(d <- datoms){
      all = all :+ d.withIndexTpe(DatomIndexType.VAET)
      all = all :+ d.withIndexTpe(DatomIndexType.AVET)
    }

    val list = all.sorted(DatomComparators.datomOrdering).map(DatomPrinters.eavtToString(_))

    val NUM_LEAF_ENTRIES = 8//rand.nextInt(4, 64)
    val NUM_META_ENTRIES = 8//rand.nextInt(4, 64)

    val indexId = UUID.randomUUID().toString
    val storage = new MemoryStorage()

    val indexContext = Await.result(TestHelper.loadOrCreateIndex(IndexContext(
      indexId,
      NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES,
      maxNItems = -1L
    ))(storage, global), Duration.Inf).get

    val builder = IndexBuilder.create[K, V](global, DatomComparators.datomOrdering,
        indexContext.numLeafItems, indexContext.numMetaItems, indexContext.maxNItems,
        DatomSerializers.datomSerializer, DefaultSerializers.bytesSerializer)
      .storage(storage)
      .serializer(DatomSerializers.datomEmptyBytesGrpcSerializer)
      .keyToStringConverter(DatomPrinters.eavtToString)
      .build()

    var data = all.map{d => (d, Array.empty[Byte], false)}
    val index = new QueryableIndex[K, V](indexContext)(builder)

    val result = Await.result(index.insert(data, "v1"), Duration.Inf)

    assert(result.success)

    val indexData = Await.result(index.all(), Duration.Inf).map{case (k, _, _) => k}.map(DatomPrinters.eavtToString(_))

    val actorSearch = EAVT()
      .withIndexTpe(DatomIndexType.AVET)
      .withA("services.scalable.index.test.DatomsSpec$Actor$1/name")
      .withV(ByteString.copyFrom(caprio.name.getBytes("UTF-8")))
      .withValueTpe(DatomFieldType.TEXT)

    val caprioActor = Await.result(index.get(actorSearch), Duration.Inf)

    /*val caprioActorList = all.find { x =>
      val c = DatomComparators.datomOrdering.equiv(x, actorSearch)
      c
    }*/

    val moviesByCaprioSearch = EAVT()
      .withIndexTpe(DatomIndexType.VAET)
      .withE("Movie-1")
      .withA("services.scalable.index.test.DatomsSpec$Movie$1/actors")
      .withV(ByteString.copyFrom(caprioActor.get._1.e.get.getBytes("UTF-8")))
      .withValueTpe(DatomFieldType.REF)

    val moviesByCaprio = Await.result(index.get(moviesByCaprioSearch), Duration.Inf).map{ x =>

      val movieSearch = EAVT()
        .withIndexTpe(DatomIndexType.EAVT)
        .withE(x._1.e.get)
        .withA("services.scalable.index.test.DatomsSpec$Movie$1/name")
        //.withV(ByteString.copyFrom(caprioActor.get._1.e.get.getBytes("UTF-8")))
        .withValueTpe(DatomFieldType.TEXT)

      Await.result(index.get(movieSearch), Duration.Inf)
    }.map(x => DatomPrinters.eavtToString(x.get._1))

    val luckyNumbersKate = EAVT()
      .withIndexTpe(DatomIndexType.EAVT)
      .withE(kate.id)
      .withA("services.scalable.index.test.DatomsSpec$Actor$1/luckyNumbers")
      .withValueTpe(DatomFieldType.TEXT)

    val kateLuckyNumbers = Await.result(index.all(index.prefix(luckyNumbersKate,
        rand.nextBoolean())(DatomComparators.datomOrdering)), Duration.Inf)
      .map{x => DatomPrinters.eavtToString(x._1)}

    println()
  }

}
