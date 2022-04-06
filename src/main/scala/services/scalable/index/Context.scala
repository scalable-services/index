package services.scalable.index

import scala.concurrent.Future

trait Context[K, V] {

  val indexId: String

  val NUM_LEAF_ENTRIES: Int
  val NUM_META_ENTRIES: Int

  var root: Option[(String, String)]
  var num_elements: Long
  var levels: Int

  def isNew(id: (String, String)): Boolean

  def get(id: (String, String)): Future[Block[K,V]]
  def getLeaf(id: (String, String)): Future[Leaf[K,V]]
  def getMeta(id: (String, String)): Future[Meta[K,V]]

  def createLeaf(): Leaf[K,V]
  def createMeta(): Meta[K,V]

  def setParent(id: (String, String), idx: Int, parent: Option[(String, String)])
  def getParent(id: (String, String)): Option[(Option[(String, String)], Int)]
  def isFromCurrentContext(b: Block[K,V]): Boolean

  //def save(): Future[Boolean]
  def duplicate(): Context[K, V]

}
