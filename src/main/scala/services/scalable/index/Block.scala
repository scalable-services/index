package services.scalable.index

trait Block[K, V] {

  val id: String
  val partition: String

  var level = 0
  def nSubtree: Long
  val unique_id = (partition, id)

  val size: Int

  val MIN: Int
  val MAX: Int

  // Tells from what root pointer this block was originally created
  var root: Option[(String, String)] = None

  var isNew: Boolean = true

  def lastOption: Option[K]
  def firstOption: Option[K]

  def middleOption: Option[K]

  def last: K
  def first: K

  def middle: K

  def length: Int

  def binSearch(k: K, start: Int = 0, end: Int = length - 1)(implicit ord: Ordering[K]): (Boolean, Int)

  def findPosition(k: K)(implicit ord: Ordering[K]): Int

  //def borrowLeftTo(t: Block[K,V])(implicit ctx: Context[K,V]): Block[K,V]
  //def borrowRightTo(t: Block[K,V])(implicit ctx: Context[K,V]): Block[K,V]
  def borrow(t: Block[K,V])(implicit ctx: Context[K,V]): Block[K, V]
  def merge(r: Block[K,V], version: String)(implicit ctx: Context[K,V]): Block[K,V]

  def minNeeded(): Int = MIN - length
  def canBorrowTo(target: Block[K,V]): Boolean = length - target.minNeeded() >= MIN

  def copy()(implicit ctx: Context[K,V]): Block[K,V]
  def split()(implicit ctx: Context[K,V]): Block[K,V]

  def isFull(): Boolean
  def isEmpty(): Boolean
  def hasMinimum(): Boolean

  def hasEnough(): Boolean

  def print()(implicit ctx: Context[K, V]): String
}
