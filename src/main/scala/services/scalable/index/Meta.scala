package services.scalable.index

import org.slf4j.LoggerFactory
import scala.util.{Failure, Success, Try}

class Meta (override val id: String,
            override val partition: String,
            override val MIN: Int,
            override val MAX: Int,
            override val size: Int = 0) extends Block {

  val logger = LoggerFactory.getLogger(this.getClass)

  var pointers = Array.empty[Pointer]

  override def last: Bytes = pointers.last._1
  override def first: Bytes = pointers.head._1

  def setPointer(block: Block, pos: Int)(implicit ctx: Context): Unit = {
    pointers(pos) = block.last -> block.unique_id
    ctx.setParent(block.unique_id, pos, Some(unique_id))
  }

  def setPointers()(implicit ctx: Context): Unit = {
    for(i<-0 until pointers.length){
      val (k, c) = pointers(i)
      ctx.setParent(c, i, Some(unique_id))
    }
  }

  def binSearch(k: Bytes, start: Int = 0, end: Int = pointers.length - 1)(implicit ord: Ordering[Bytes]): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, pointers(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return binSearch(k, start, pos - 1)

    binSearch(k, pos + 1, end)
  }

  def findPath(k: Bytes)(implicit ord: Ordering[Bytes]): String = {
    val (_, pos) = binSearch(k)
    pointers(if(pos < pointers.length) pos else pos - 1)._2
  }

  def insert(data: Seq[Pointer])(implicit ctx: Context, ord: Ordering[Bytes]): Try[Int] = {
    if(isFull()) return Failure(Errors.META_BLOCK_FULL)

    if(data.exists{case (k, _) => pointers.exists{case (k1, _) => ord.equiv(k, k1)}}){
      return Failure(Errors.META_DUPLICATE_KEY(inOrder(), data))
    }

    pointers = (pointers ++ data).sortBy(_._1)

    setPointers()

    Success(data.length)
  }

  def removeAt(pos: Int)(implicit ctx: Context): Pointer = {
    val p = pointers(pos)

    var aux = Array.empty[Pointer]

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

  def left(pos: Int): Option[String] = {
    val lpos = pos - 1
    if(lpos < 0) return None
    Some(pointers(lpos)._2)
  }

  def right(pos: Int): Option[String] = {
    val rpos = pos + 1
    if(rpos == pointers.length) return None
    Some(pointers(rpos)._2)
  }

  override def length: Int = pointers.length

  override def borrowLeftTo(t: Block)(implicit ctx: Context): Block = {
    val target = t.asInstanceOf[Meta]

    val len = pointers.length
    val start = len - target.minNeeded()

    target.pointers = pointers.slice(start, len) ++ target.pointers
    pointers = pointers.slice(0, start)

    target.setPointers()
    setPointers()

    target
  }

  override def borrowRightTo(t: Block)(implicit ctx: Context): Block = {
    val target = t.asInstanceOf[Meta]

    val n = target.minNeeded()
    target.pointers = target.pointers ++ pointers.slice(0, n)
    pointers = pointers.slice(n, pointers.length)

    target.setPointers()
    setPointers()

    target
  }

  override def merge(r: Block)(implicit ctx: Context): Block = {
    val right = r.asInstanceOf[Meta]

    pointers = pointers ++ right.pointers

    setPointers()

    this
  }

  override def isFull(): Boolean = pointers.length == MAX
  override def isEmpty(): Boolean = pointers.isEmpty

  override def hasMinimum(): Boolean = pointers.length >= MIN

  override def copy()(implicit ctx: Context): Meta = {
    if(ctx.isNew(unique_id)) return this

    val (p, pos) = ctx.getParent(unique_id).get

    val copy = ctx.createMeta()
    ctx.setParent(copy.unique_id, pos, p)

    //copy.pointers = pointers.clone()

    val len = pointers.length

    for(i<-0 until len){
      val (k, c) = pointers(i)
      copy.pointers = copy.pointers :+ k -> c
    }

    copy.setPointers()

    copy
  }

  override def split()(implicit ctx: Context): Meta = {
    val right = ctx.createMeta()

    val len = pointers.length
    val pos = len/2

    right.pointers = pointers.slice(pos, len)
    pointers = pointers.slice(0, pos)

    setPointers()
    right.setPointers()

    right
  }

  override def print()(implicit kf: Bytes => String, vf: Bytes => String): String = {
    if(pointers.isEmpty) return "[]"

    val sb = new StringBuilder()
    sb ++= Console.RED_B
    sb ++= "["
    sb ++= Console.RESET

    for(i<-0 until pointers.length - 1){
      val (k, _) = pointers(i)

      sb ++= kf(k)
      sb ++= ","
    }

    sb ++= Console.RED_B

    val (k, _) = pointers(pointers.length - 1)
    sb ++= kf(k)

    sb ++= Console.RESET

    sb ++= Console.RED_B
    sb ++= "]"
    sb ++= Console.RESET

    sb.toString()
  }

  def inOrder(): Seq[Pointer] = pointers

}
