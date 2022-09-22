package services.scalable.index

trait IdGenerator {

  def generateId[K,V](ctx: Context[K,V]): String
  def generatePartition[K,V](ctx: Context[K,V]): String

}
