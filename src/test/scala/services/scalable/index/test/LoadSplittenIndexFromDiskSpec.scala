package services.scalable.index.test

import services.scalable.index.DefaultComparators.bytesOrd
import services.scalable.index.DefaultSerializers._
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl._
import services.scalable.index.{Bytes, Context, DefaultComparators, DefaultSerializers, IdGenerator, IndexBuilder, QueryableIndex}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import services.scalable.index.DefaultPrinters._

class LoadSplittenIndexFromDiskSpec extends Repeatable {

  override val times: Int = 1

  "operations" should " run successfully" in {

    val rand = ThreadLocalRandom.current()

    type K = Bytes
    type V = Bytes

    val NUM_LEAF_ENTRIES = 8
    val NUM_META_ENTRIES = 8

    //val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new MemoryStorage()
    implicit val storage = new CassandraStorage(TestConfig.session, false)

    val tctx = IndexContext("index", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    val ctx = Await.result(TestHelper.loadOrCreateIndex(tctx), Duration.Inf)

    val lctx = Await.result(storage.loadIndex("d4a53d67-6d4e-4d0f-8815-b7d388ea4daf"), Duration.Inf)
    val rctx = Await.result(storage.loadIndex("79b6eab6-5b5f-434f-a83e-9ac361003248"), Duration.Inf)

    val builder = IndexBuilder.create[K, V](DefaultComparators.bytesOrd)
      .storage(storage)
      .serializer(DefaultSerializers.grpcBytesBytesSerializer)

    val left = new QueryableIndex[K, V](lctx.get)(builder)
    val right = new QueryableIndex[K, V](rctx.get)(builder)

    val index = new QueryableIndex[K, V](ctx.get)(builder)

    val fullList = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)
      .map { case (k, v, _) => new String(k) }.toList

    val leftList = Await.result(TestHelper.all(left.inOrder()), Duration.Inf)
      .map { case (k, v, _) => new String(k) }.toList

    val rightList = Await.result(TestHelper.all(right.inOrder()), Duration.Inf)
      .map { case (k, v, _) => new String(k) }.toList

    println(s"left: ${Console.GREEN_B}${leftList}${Console.RESET}")
    println()
    println(s"right: ${Console.MAGENTA_B}${rightList}${Console.RESET}")

    assert(fullList == (leftList ++ rightList))

    Await.result(storage.close(), Duration.Inf)

  }

}
