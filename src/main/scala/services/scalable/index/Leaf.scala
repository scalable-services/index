package services.scalable.index

import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

class Leaf(override val id: String,
           override val partition: String,
           override val MIN: Int,
           override val MAX: Int,
           override val size: Int = 0) extends Block {

  val logger = LoggerFactory.getLogger(this.getClass)

  var tuples = Seq.empty[Tuple]

  override def last: Bytes = tuples.last._1
  override def first: Bytes = tuples.head._1
  
  def insert(data: Seq[Tuple], upsert: Boolean)(implicit ord: Ordering[Bytes]): Try[Int] = {

    if(isFull()) return Failure(Errors.LEAF_BLOCK_FULL)

    val n = Math.min(MAX - tuples.length, data.length)
    var slice = data.slice(0, n)

    val len = slice.length

    if(slice.exists{case (k, _) => tuples.exists{case (k1, _) => ord.equiv(k, k1)}}){
      if(!upsert){
        return Failure(Errors.LEAF_DUPLICATE_KEY(inOrder(), slice))
      }

      slice = slice.filterNot{case (k, _) => tuples.exists{case (k1, _) => ord.equiv(k, k1)}}
    }

    tuples = (tuples ++ slice).sortBy(_._1)

    Success(len)
  }

  def remove(keys: Seq[Bytes])(implicit ord: Ordering[Bytes]): Try[Int] = {
    if(keys.exists{ k => !tuples.exists{ case (k1, _) => ord.equiv(k1, k) }}){
      return Failure(Errors.LEAF_KEY_NOT_FOUND(keys))
    }

    tuples = tuples.filterNot{case (k, _) => keys.exists{ k1 => ord.equiv(k, k1)}}

    Success(keys.length)
  }

  def update(data: Seq[Tuple])(implicit ord: Ordering[Bytes]): Try[Int] = {

    if(data.exists{ case (k, _) => !tuples.exists{case (k1, _) => ord.equiv(k1, k) }}){
      return Failure(Errors.LEAF_KEY_NOT_FOUND(data.map(_._1)))
    }

    val notin = tuples.filterNot{case (k1, _) => data.exists{ case (k, _) => ord.equiv(k, k1)}}

    tuples = (notin ++ data).sortBy(_._1)

    Success(data.length)
  }

  override def length: Int = tuples.length

  override def borrowLeftTo(t: Block)(implicit ctx: Context): Leaf = {
    val target = t.asInstanceOf[Leaf]

    val len = tuples.length
    val start = len - target.minNeeded()

    target.tuples = tuples.slice(start, len) ++ target.tuples
    tuples = tuples.slice(0, start)

    target
  }

  override def borrowRightTo(t: Block)(implicit ctx: Context): Block = {
    val target = t.asInstanceOf[Leaf]

    val n = target.minNeeded()
    target.tuples = target.tuples ++ tuples.slice(0, n)
    tuples = tuples.slice(n, tuples.length)

    target
  }

  override def merge(r: Block)(implicit ctx: Context): Block = {
    val right = r.asInstanceOf[Leaf]

    tuples = tuples ++ right.tuples
    this
  }

  override def isFull(): Boolean = tuples.length == MAX
  override def isEmpty(): Boolean = tuples.isEmpty

  def inOrder(): Seq[Tuple] = tuples

  override def hasMinimum(): Boolean = tuples.length >= MIN

  override def copy()(implicit ctx: Context): Leaf = {
    if(ctx.isNew(unique_id)) return this

    val (p, pos) = ctx.getParent(unique_id).get

    val copy = ctx.createLeaf()
    ctx.setParent(copy.unique_id, pos, p)

    val len = tuples.length

    for(i<-0 until len){
      val (k, v) = tuples(i)
      copy.tuples = copy.tuples :+ k -> v
    }

    copy
  }
  
  override def split()(implicit ctx: Context): Leaf = {
    val right = ctx.createLeaf()

    val len = tuples.length
    val pos = len/2

    right.tuples = tuples.slice(pos, len)
    right.tuples = tuples.slice(pos, len)
    tuples = tuples.slice(0, pos)

    right
  }

  def binSearch(k: Bytes, start: Int = 0, end: Int = tuples.length - 1)(implicit ord: Ordering[Bytes]): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, tuples(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return binSearch(k, start, pos - 1)

    binSearch(k, pos + 1, end)
  }

  def find(k: Bytes)(implicit ord: Ordering[Bytes]): Option[Tuple] = {
    val (found, pos) = binSearch(k)

    if(!found) return None

    Some(tuples(pos))
  }

  def lt(k: Bytes, include: Boolean = false)(implicit ord: Ordering[Bytes]): Seq[Tuple] = {
    if(include) return tuples.filter{case (k1, _) => ord.lteq(k1, k)}.reverse
    tuples.filter{case (k1, _) => ord.lt(k1, k)}.reverse
  }

  def lte(k: Bytes)(implicit ord: Ordering[Bytes]): Seq[Tuple] = {
    lt(k, true)
  }

  def gt(k: Bytes, include: Boolean = false)(implicit ord: Ordering[Bytes]): Seq[Tuple] = {
    if(include) return tuples.filter{case (k1, _) => ord.gteq(k1, k)}
    tuples.filter{case (k1, _) => ord.gt(k1, k)}
  }

  def gte(k: Bytes)(implicit ord: Ordering[Bytes]): Seq[Tuple] = {
    gt(k, true)
  }

  def interval(lower: Bytes, upper: Bytes, includeLower: Boolean = false, includeUpper: Boolean = false)(implicit ord: Ordering[Bytes]): Seq[Tuple] = {
    tuples.filter{case (k, _) => (if(includeLower) ord.gteq(k, lower) else ord.gt(k, lower)) && (if(includeUpper) ord.lteq(k, upper) else ord.lt(k, upper))}
  }

  def min()(implicit ord: Ordering[Bytes]): Option[Tuple] = {
    if(tuples.isEmpty) return None
    Some(tuples.minBy(_._1))
  }

  def max()(implicit ord: Ordering[Bytes]): Option[Tuple] = {
    if(tuples.isEmpty) return None
    Some(tuples.maxBy(_._1))
  }

  override def print()(implicit kf: Bytes => String, vf: Bytes => String): String = {
    if(tuples.isEmpty) return "[]"

    val sb = new StringBuilder()
    sb ++= Console.GREEN_B
    sb ++= "["
    sb ++= Console.RESET

    for(i<-0 until tuples.length - 1){
      val (k, v) = tuples(i)
      sb ++= kf(k)

      sb ++= "->"

      sb ++= vf(v)
      sb ++= ","
    }

    sb ++= Console.RED_B
    val (k, v) = tuples(tuples.length - 1)
    sb ++= kf(k)
    sb ++= Console.RESET

    sb ++= "->"

    sb ++= vf(v)

    sb ++= Console.GREEN_B
    sb ++= "]"
    sb ++= Console.RESET

    sb.toString()
  }

}
