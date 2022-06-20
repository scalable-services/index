package services.scalable.index

import scala.collection.concurrent.TrieMap

trait Cache {

  def put[K, V](block: Block[K,V]): Unit
  def get[K, V](id: (String, String)): Option[Block[K,V]]

  def put(id: (String, String), parent: Option[(String, String)], pos: Int): Unit
  def getParent(id: (String, String)): Option[(Option[(String, String)], Int)]

  val dbIndexes: TrieMap[String, Map[String, QueryableIndex[_, _]]]

  def putIndex[K, V](id: String, index: QueryableIndex[K, V]): Unit
  def getIndex[K, V](id: String, indexId: String): QueryableIndex[K, V]
  def getIndexes[K, V](id: String): Map[String, QueryableIndex[K, V]]
}
