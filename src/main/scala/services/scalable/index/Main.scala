package services.scalable.index

import com.google.common.base.Charsets
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.impl.{DefaultCache, DefaultContext, MemoryStorage}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.ExecutionContext.Implicits.global
import services.scalable.index.DefaultComparators._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

object Main {

  val logger = LoggerFactory.getLogger(this.getClass)

  def all[K, V](it: AsyncIterator[Seq[Tuple[K, V]]]): Future[Seq[Tuple[K, V]]] = {
    it.hasNext().flatMap {
      case true => it.next().flatMap { list =>
        all(it).map{list ++ _}
      }
      case false => Future.successful(Seq.empty[Tuple[K, V]])
    }
  }

  def main(args: Array[String]): Unit = {

    val rand = ThreadLocalRandom.current()

    type K = Bytes
    type V = Bytes

    val NUM_LEAF_ENTRIES = 4
    val NUM_META_ENTRIES = 4

    val indexId = UUID.randomUUID().toString

    implicit val cache = new DefaultCache[K, V](MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new MemoryStorage[K, V](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    val ctx = new DefaultContext[K, V](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    val index = new QueryableIndex[K, V](ctx)

    var list = Seq.empty[(K, V)]

    for(i<-0 until 20){
      val k = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)
      val v = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

      if(!list.exists{case (k1, _) => ord.equiv(k, k1)}){
        list = list :+ k -> v
      }
    }

    val task = for {
      ok <- index.insert(list).map(_ == list.length)
      result <- if(ok) index.save().map(_ => true) else Future.successful(false)
    } yield {
      result
    }

    val result = Await.result(task, Duration.Inf)

    println(s"\n\nresult: ${result} \n\n")

    val idata = Await.result(all(index.inOrder()), Duration.Inf)

    println(s"${Console.GREEN_B}${idata.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v, Charsets.UTF_8)}}${Console.RESET}")
  }

}
