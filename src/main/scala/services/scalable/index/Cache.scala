package services.scalable.index

trait Cache {

  def put[K, V](block: Block[K,V]): Unit
  def get[K, V](id: String): Option[Block[K,V]]

  def put(id: String, parent: Option[String], pos: Int): Unit
  def getParent(id: String): Option[(Option[String], Int)]
  
}
