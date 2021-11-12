package services.scalable.index

import scala.concurrent.Future

trait Context {

  val indexId: String

  val NUM_LEAF_ENTRIES: Int
  val NUM_META_ENTRIES: Int

  var root: Option[String]
  var num_elements = 0L
  var levels = 0

  def isNew(id: String): Boolean

  def get(id: String): Future[Block]
  def getLeaf(id: String): Future[Leaf]
  def getMeta(id: String): Future[Meta]

  def createLeaf(): Leaf
  def createMeta(): Meta

  def setParent(id: String, idx: Int, parent: Option[String])
  def getParent(id: String): Option[(Option[String], Int)]
  def isFromCurrentContext(b: Block): Boolean

  def save(): Future[Boolean]

}
