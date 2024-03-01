package services.scalable.index

trait IdGenerator {

  def generateIndexId(): String

  def generateBlockId[K,V](ctx: Context[K,V]): String
  def generateBlockPartition[K,V](ctx: Context[K,V]): String

}
