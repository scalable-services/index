package services.scalable.index

trait IdGenerator {

  def generateId(ctx: Context): String
  def generatePartition(ctx: Context): String

}
