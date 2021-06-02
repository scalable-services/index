package services.scalable.index

trait Block {

  val id: String
  val partition: String

  val unique_id: String = partition.concat("#").concat(id)

  val size: Int

  val MIN: Int
  val MAX: Int

  // Tells from what root pointer this block was originally created
  var root: Option[String] = None

  def last: Bytes
  def first: Bytes
  def length: Int

  def borrowLeftTo(t: Block)(implicit ctx: Context): Block
  def borrowRightTo(t: Block)(implicit ctx: Context): Block
  def merge(r: Block)(implicit ctx: Context): Block

  def minNeeded(): Int = MIN - length
  def canBorrowTo(target: Block): Boolean = length - target.minNeeded() >= MIN

  def copy()(implicit ctx: Context): Block
  def split()(implicit ctx: Context): Block

  def isFull(): Boolean
  def isEmpty(): Boolean
  def hasMinimum(): Boolean

  def print()(implicit kf: Bytes => String, vf: Bytes => String): String

}
