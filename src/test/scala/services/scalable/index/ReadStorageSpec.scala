package services.scalable.index

import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.index.impl._

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class ReadStorageSpec extends AnyFlatSpec with Repeatable {

  override val times: Int = 1

  val logger = LoggerFactory.getLogger(this.getClass)
  val rand = ThreadLocalRandom.current()

  "random leaf greater than data " must "be equal to test data" in {

    implicit def xf(k: Bytes): String = new String(k)

    import DefaultComparators._
    import DefaultSerializers._

    val NUM_LEAF_ENTRIES = 8//rand.nextInt(4, 100)
    val NUM_META_ENTRIES = 8//rand.nextInt(4, NUM_LEAF_ENTRIES)

    //implicit val storage = new MemoryStorage[K, V]()

    val indexId = "demo_index"

    import DefaultSerializers._
    implicit val serializer = new GrpcByteSerializer[Bytes, Bytes]()

    implicit val cache = new DefaultCache[Bytes, Bytes]()
    implicit val storage = new CassandraStorage[Bytes, Bytes](TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, truncate = false)

    implicit val ctx = Await.result(storage.load(indexId), Duration.Inf)
    val index = new Index()

    var list = Seq.empty[Tuple2[Bytes, Bytes]]

    logger.debug(s"root: ${ctx.root}")

    var cur: Option[Leaf[Bytes, Bytes]] = Await.result(index.first(), Duration.Inf)

    /*var cur: Option[Leaf[S, K, V]] = Some(Await.result(ctx.getLeaf("d73ef724-2f59-46c7-90ca-62a1a1437b10"),
      Duration.Inf))*/

    //assert(cur.isDefined && cur.get.root.equals(ctx.root), s"cur root: ${cur.map(_.root)} ctx root: ${ctx.root}")

    /*var parent: Option[Block[S, K, V]] = cur

    while(parent.isDefined){
      logger.debug(s"parent: ${parent.get.parent}")

      if(parent.get.parent.isDefined){
        parent = Await.result(ctx.get(parent.get.parent.get), Duration.Inf)
      } else {
        parent = None
      }
    }*/

    println(ctx.root)
    println(cur.map(_.inOrder().map{case (k, v) => new String(k) -> new String(v)}))
    println(Await.result(index.get(cur.get.last), Duration.Inf).map{case (k, v) => new String(k) -> new String(v)})

    while(cur.isDefined){
      val block = cur.get
      list = list ++ block.inOrder()
      cur = Await.result(index.next(cur.map(_.unique_id)), Duration.Inf)
    }

    logger.debug(s"idata: ${list.map{case (k, v) => new String(k) -> new String(v)}}")

    /*def test(): Future[Try[Int]] = {
      //throw new RuntimeException(":(")
      //Future.successful(Success(3))
      Future.successful(Failure(new RuntimeException(":(")))
    }

    test().onComplete {
      case Success(value) => logger.debug(s"The result: ${value}")
      case Failure(ex) => logger.error(ex.getMessage)
    }*/

    Await.ready(storage.close(), Duration.Inf)

  }

}
