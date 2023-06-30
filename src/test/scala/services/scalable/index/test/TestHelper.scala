package services.scalable.index.test

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import services.scalable.index.grpc.{IndexContext, TemporalContext}
import services.scalable.index.test.TestConfig.{CQL_PWD, CQL_USER, KEYSPACE}
import services.scalable.index.{AsyncIndexIterator, Storage, Tuple}

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

  def all[K, V](it: AsyncIndexIterator[Seq[Tuple[K, V]]])(implicit ec: ExecutionContext): Future[Seq[Tuple[K, V]]] = {
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
    val loader =
      DriverConfigLoader.programmaticBuilder()
        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(30))
        .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 31768)
        .withInt(DefaultDriverOption.SESSION_LEAK_THRESHOLD, 1000)
        .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
        .withString(DefaultDriverOption.RECONNECTION_POLICY_CLASS, "ExponentialReconnectionPolicy")
        .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, java.time.Duration.ofSeconds(1))
        .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, java.time.Duration.ofSeconds(10))
        /*.startProfile("slow")
        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
        .endProfile()*/
        .build()

    CqlSession
      .builder()
      .withKeyspace(KEYSPACE)
      .withAuthCredentials(CQL_USER, CQL_PWD)
      .withConfigLoader(loader)
      .build()
  }

}
