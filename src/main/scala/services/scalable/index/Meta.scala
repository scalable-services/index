package services.scalable.index

import org.slf4j.LoggerFactory
import scala.util.{Failure, Success, Try}

class Meta[K, V](override val id: String,
                          override val partition: String,
                          override val MIN: Int,
                          override val MAX: Int,
                          override val size: Int = 0) extends Block[K,V] {

  val logger = LoggerFactory.getLogger(this.getClass)

  var pointers = Array.empty[(K, Pointer)]

  override def last: K = pointers.last._1
  override def first: K = pointers.head._1

  override def middle: K  = pointers(pointers.length/2)._1

  override def lastOption: Option[K] = pointers.lastOption.map(_._1)
  override def firstOption: Option[K] = pointers.headOption.map(_._1)

  override def middleOption: Option[K] = pointers.length match {
    case 0 => None
    case _ => Some(pointers(pointers.length/2)._1)
  }

  override def nSubtree: Long = pointers.map(_._2.nElements).sum

  def setPointer(block: Block[K, V], pos: Int)(implicit ctx: Context[K, V]): Unit = {
    pointers(pos) = block.last -> Pointer(block.partition, block.id, block.nSubtree, block.level)
    ctx.setParent(block.unique_id, block.lastOption, pos, Some(unique_id))
  }

  def setPointers()(implicit ctx: Context[K,V]): Unit = {
    for(i<-0 until pointers.length){
      val (k, c) = pointers(i)
      ctx.setParent(c.unique_id, Some(k), i, Some(unique_id))
    }
  }

  override def binSearch(k: K, start: Int = 0, end: Int = pointers.length - 1)(implicit ord: Ordering[K]): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, pointers(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return binSearch(k, start, pos - 1)

    binSearch(k, pos + 1, end)
  }

  def findPath(k: K)(implicit ord: Ordering[K]): (String, String) = {
    val (_, pos) = binSearch(k)
    val idx = if(pos < pointers.length) pos else pos - 1
    val e = pointers(idx)._2.unique_id

    logger.debug(s"[meta search level ${level - 1} in ${id} at pos ${idx}] => ${e}")

    e
  }

  override def findPosition(k: K)(implicit ord: Ordering[K]): Int = {
    val (_, pos) = binSearch(k)
    if (pos < pointers.length) pos else pos - 1
  }

  def insert(data: Seq[(K, Pointer)])(implicit ctx: Context[K,V], ord: Ordering[K]): Try[Int] = {
    if(isFull()) return Failure(Errors.META_BLOCK_FULL)

    if(data.exists{case (k, _) => pointers.exists{case (k1, _) => ord.equiv(k, k1)}}){
      return Failure(Errors.META_DUPLICATE_KEY(data.map(_._1), ctx.builder.ks))
    }

    pointers = (pointers ++ data).sortBy(_._1)

    setPointers()

    Success(data.length)
  }

  def removeAt(pos: Int)(implicit ctx: Context[K,V]): (K, Pointer) = {
    val p = pointers(pos)

    var aux = Array.empty[(K, Pointer)]

    for(i<-0 until pos){
      aux = aux :+ pointers(i)
    }

    for(i<-pos+1 until pointers.length){
      aux = aux :+ pointers(i)
    }

    pointers = aux

    setPointers()
    p
  }

  def left(pos: Int): Option[(String, String)] = {
    val lpos = pos - 1
    if(lpos < 0) return None
    Some(pointers(lpos)._2.unique_id)
  }

  def right(pos: Int): Option[(String, String)] = {
    val rpos = pos + 1
    if(rpos == pointers.length) return None
    Some(pointers(rpos)._2.unique_id)
  }

  override def length: Int = pointers.length

  /*def borrowLeftTo(t: Block[K,V])(implicit ctx: Context[K,V]): Block[K,V] = {
    val target = t.asInstanceOf[Meta[K,V]]

    val len = pointers.length
    val start = len - target.minNeeded()

    target.pointers = pointers.slice(start, len) ++ target.pointers
    pointers = pointers.slice(0, start)

    target.setPointers()
    setPointers()

    target
  }

  def borrowRightTo(t: Block[K,V])(implicit ctx: Context[K,V]): Block[K,V] = {
    val target = t.asInstanceOf[Meta[K,V]]

    val n = target.minNeeded()
    target.pointers = target.pointers ++ pointers.slice(0, n)
    pointers = pointers.slice(n, pointers.length)

    target.setPointers()
    setPointers()

    target
  }*/

  override def borrow(t: Block[K,V])(implicit ctx: Context[K,V]): Block[K,V] = {
    val target = t.asInstanceOf[Meta[K,V]]
    val targetHead = target.pointers.head._1
    val thisHead = pointers.head._1
    val minKeys = target.minNeeded()

    // borrows left
    if(ctx.builder.ord.gteq(thisHead, targetHead)){

      target.pointers = target.pointers ++ pointers.slice(0, minKeys)
      pointers = pointers.slice(minKeys, pointers.length)

      setPointers()
      target.setPointers()

      return this
    }

    val start = pointers.length - minKeys

    target.pointers = pointers.slice(start, pointers.length) ++ target.pointers
    pointers = pointers.slice(0, start)

    setPointers()
    target.setPointers()

    this
  }

  override def merge(r: Block[K,V], version: String)(implicit ctx: Context[K,V]): Block[K,V] = {
    val right = r.asInstanceOf[Meta[K,V]]

    //pointers = pointers ++ right.pointers
    insert(right.pointers)(ctx, ctx.builder.ord)

    setPointers()

    this
  }

  override def isFull(): Boolean = pointers.length == MAX
  override def isEmpty(): Boolean = pointers.isEmpty

  override def hasMinimum(): Boolean = pointers.length >= MIN

  override def hasEnough(): Boolean = pointers.length > MIN

  override def copy()(implicit ctx: Context[K,V]): Meta[K,V] = {
    if(isNew) return this

    logger.debug(s"Creating leaf copy ${unique_id}...")

    val pinfo = ctx.getParent(unique_id).get

    val copy = ctx.createMeta()
    ctx.setParent(copy.unique_id, pinfo.key, pinfo.pos, pinfo.parent)

    //copy.pointers = pointers.clone()

    val len = pointers.length

    for(i<-0 until len){
      val (k, c) = pointers(i)
      copy.pointers = copy.pointers :+ k -> c
    }

    copy.setPointers()
    copy.level = level

    copy
  }

  override def split()(implicit ctx: Context[K,V]): Meta[K, V] = {
    val right = ctx.createMeta()

    val len = pointers.length
    val pos = len/2

    right.pointers = pointers.slice(pos, len)
    pointers = pointers.slice(0, pos)

    setPointers()
    right.setPointers()

    right.level = level

    right
  }

  override def print()(implicit ctx: Context[K, V]): String = {
    if(pointers.isEmpty) return "[]"

    val sb = new StringBuilder(s"id=[${id}, len=${length}]:")
    sb ++= Console.RED_B
    sb ++= "["
    sb ++= Console.RESET

    for(i<-0 until pointers.length - 1){
      val (k, _) = pointers(i)

      sb ++= s"""[${i}]${ctx.builder.ks(k)}"""
      sb ++= ","
    }

    sb ++= Console.RED_B

    val (k, _) = pointers(pointers.length - 1)
    sb ++= s"""[${pointers.length-1}]${ctx.builder.ks(k)}"""

    sb ++= Console.RESET

    sb ++= Console.RED_B
    sb ++= "]"
    sb ++= Console.RESET

    sb.toString()
  }

  def inOrder(): Seq[(K, Pointer)] = pointers

}
