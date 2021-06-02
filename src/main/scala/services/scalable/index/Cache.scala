package services.scalable.index

trait Cache {

  def put(block: Block): Unit
  def get(id: String): Option[Block]

  def put(id: String, parent: Option[String], pos: Int): Unit
  def getParent(id: String): Option[(Option[String], Int)]

}
