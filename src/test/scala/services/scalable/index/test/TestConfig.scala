package services.scalable.index.test

import com.datastax.oss.driver.api.core.CqlSession

object TestConfig {

  val KEYSPACE = "history"
  val CQL_USER = "cassandra"
  val CQL_PWD = "cassandra"
  val DATABASE = "myHistoryIndex"

  val session = CqlSession
    .builder()
    .withKeyspace(KEYSPACE)
    .withAuthCredentials(CQL_USER, CQL_PWD)
    .build()

}
