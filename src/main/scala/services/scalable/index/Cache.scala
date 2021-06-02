package services.scalable.index

trait Cache[K,V] {

  def put(block: Block[K,V]): Unit
  def get(id: String): Option[Block[K,V]]

  def put(id: String, parent: Option[String], pos: Int): Unit
  def getParent(id: String): Option[(Option[String], Int)]
  
}
