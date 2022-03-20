package services.scalable.index

import scala.concurrent.Future

trait Context[K, V] {

  val indexId: String

  val NUM_LEAF_ENTRIES: Int
  val NUM_META_ENTRIES: Int

  var root: Option[String]
  var num_elements = 0L
  var levels = 0

  def isNew(id: String): Boolean

  def get(id: String): Future[Block[K,V]]
  def getLeaf(id: String): Future[Leaf[K,V]]
  def getMeta(id: String): Future[Meta[K,V]]
  
  def createLeaf(): Leaf[K,V]
  def createMeta(): Meta[K,V]

  def setParent(id: String, idx: Int, parent: Option[String])
  def getParent(id: String): Option[(Option[String], Int)]
  def isFromCurrentContext(b: Block[K,V]): Boolean

  //def save(): Future[Boolean]
  def duplicate(): Context[K, V]

}
