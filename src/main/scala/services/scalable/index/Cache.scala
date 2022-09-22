package services.scalable.index

import scala.collection.concurrent.TrieMap

trait Cache {

  def put[K, V](block: Block[K,V]): Unit
  def get[K, V](id: (String, String)): Option[Block[K,V]]

  def put(id: (String, String), parent: Option[(String, String)], pos: Int): Unit
  def getParent(id: (String, String)): Option[(Option[(String, String)], Int)]

  val ctxBlocks: TrieMap[String, Map[(String, String), Block[_, _]]]

  def getNewBlock[K, V](ctxId: String, blockId: (String, String)): Option[Block[K, V]]
  def putNewBlock[K, V](ctxId: String, block: Block[K, V]): Unit

  val newBlocks = TrieMap.empty[(String, String), Block[_, _]]

}
