package services.scalable.index

trait Cache {

  def put[K, V](block: Block[K,V]): Unit
  def get[K, V](id: (String, String)): Option[Block[K,V]]

  def put(id: (String, String), parent: Option[(String, String)], pos: Int): Unit
  def getParent(id: (String, String)): Option[(Option[(String, String)], Int)]

}
