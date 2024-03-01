package services.scalable.index

trait Cache {
  def put[K, V](block: Block[K,V]): Unit
  def get[K, V](id: (String, String)): Option[Block[K,V]]

  def invalidateAll(): Unit

  def invalidate[K, V](id: (String, String)): Unit
}
