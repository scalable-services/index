package services.scalable.index.test

import com.datastax.oss.driver.api.core.CqlSession
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import services.scalable.index.grpc.{IndexContext, TemporalContext}
import services.scalable.index.test.TestConfig.{CQL_PWD, CQL_USER, KEYSPACE}
import services.scalable.index.{AsyncIterator, Storage, Tuple}

import scala.concurrent.{ExecutionContext, Future}

object TestHelper {

  def loadOrCreateTemporalIndex(tctx: TemporalContext)(implicit storage: Storage, ec: ExecutionContext): Future[Option[TemporalContext]] = {
    storage.loadTemporalIndex(tctx.id).flatMap {
      case None => storage.createTemporalIndex(tctx).map(_ => Some(tctx))
      case Some(t) => Future.successful(Some(t))
    }
  }

  def loadOrCreateIndex(tctx: IndexContext)(implicit storage: Storage, ec: ExecutionContext): Future[Option[IndexContext]] = {
    storage.loadIndex(tctx.id).flatMap {
      case None => storage.createIndex(tctx).map(_ => Some(tctx))
      case Some(t) => Future.successful(Some(t))
    }
  }

  def loadIndex(id: String)(implicit storage: Storage, ec: ExecutionContext): Future[Option[IndexContext]] = {
    storage.loadIndex(id)
  }

  def loadTemporalIndex(id: String)(implicit storage: Storage, ec: ExecutionContext): Future[Option[TemporalContext]] = {
    storage.loadTemporalIndex(id)
  }

  def all[K, V](it: AsyncIterator[Seq[Tuple[K, V]]])(implicit ec: ExecutionContext): Future[Seq[Tuple[K, V]]] = {
    it.hasNext().flatMap {
      case true => it.next().flatMap { list =>
        all(it).map {
          list ++ _
        }
      }
      case false => Future.successful(Seq.empty[Tuple[K, V]])
    }
  }

  def isColEqual[K, V](source: Seq[Tuple2[K, V]], target: Seq[Tuple2[K, V]])(implicit ordk: Ordering[K], ordv: Ordering[V]): Boolean = {
    if (target.length != source.length) return false
    for (i <- 0 until source.length) {
      val (ks, vs) = source(i)
      val (kt, vt) = target(i)

      if (!(ordk.equiv(kt, ks) && ordv.equiv(vt, vs))) {
        return false
      }
    }

    true
  }

  def createCassandraSession(): CqlSession = {
    CqlSession
      .builder()
      .withKeyspace(KEYSPACE)
      .withAuthCredentials(CQL_USER, CQL_PWD)
      .build()
  }

}
