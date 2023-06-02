package services.scalable.index.test

import com.datastax.oss.driver.api.core.CqlSession
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper

object TestConfig {

  val KEYSPACE = "history"
  val CQL_USER = "cassandra"
  val CQL_PWD = "cassandra"
  val DATABASE = "myHistoryIndex"

  val NUM_LEAF_ENTRIES = 16
  val NUM_META_ENTRIES = 16

}
