package services.scalable.index

import org.slf4j.LoggerFactory
import scala.util.{Failure, Success, Try}

class Leaf[K, V](override val id: String,
                          override val partition: String,
                          override val MIN: Int,
                          override val MAX: Int,
                          override val size: Int = 0) extends Block[K,V] {

  val logger = LoggerFactory.getLogger(this.getClass)

  var tuples = Seq.empty[Tuple[K, V]]

  override def last: K = tuples.last._1
  override def first: K = tuples.head._1

  override def nSubtree: Long = tuples.length.toLong

  def insert(data: Seq[Tuple[K, V]], upsert: Boolean)(implicit ord: Ordering[K]): Try[Int] = {
    
    if(isFull()) return Failure(Errors.LEAF_BLOCK_FULL)

    val n = Math.min(MAX - tuples.length, data.length)
    var slice = data.slice(0, n)

    val len = slice.length

    if(slice.exists{case (k, _, _) => tuples.exists{case (k1, _, _) => ord.equiv(k, k1)}}){
      if(!upsert){
        return Failure(Errors.LEAF_DUPLICATE_KEY(inOrder(), slice))
      }

      slice = slice.filterNot{case (k, _, _) => tuples.exists{case (k1, _, _) => ord.equiv(k, k1)}}
    }

    tuples = (tuples ++ slice).sortBy(_._1)

    Success(len)
  }

  def remove(keys: Seq[K])(implicit ord: Ordering[K]): Try[Int] = {
    if(keys.exists{ k => !tuples.exists{ case (k1, _, _) => ord.equiv(k1, k) }}){
      return Failure(Errors.LEAF_KEY_NOT_FOUND[K](keys))
    }

    tuples = tuples.filterNot{case (k, _, _) => keys.exists{ k1 => ord.equiv(k, k1)}}

    Success(keys.length)
  }

  def update(data: Seq[Tuple[K, V]], version: String)(implicit ord: Ordering[K]): Try[Int] = {

    if(data.exists{ case (k, _, _) => !tuples.exists{case (k1, _, _) => ord.equiv(k1, k) }}){
      return Failure(Errors.LEAF_KEY_NOT_FOUND(data.map(_._1)))
    }

    val versionsChanged = data.filter{case (k0, _, vs0) => tuples.exists{case (k1, _, vs1) => ord.equiv(k0, k1) && !vs0.equals(vs1)}}

    if (!versionsChanged.isEmpty) {
      return Failure(Errors.VERSION_CHANGED(versionsChanged))
    }

    val notin = tuples.filterNot{case (k1, _, _) => data.exists{ case (k, _, _) => ord.equiv(k, k1)}}

    tuples = (notin ++ data.map{case (k, v, _) => Tuple3(k, v, version)}).sortBy(_._1)

    Success(data.length)
  }

  override def length: Int = tuples.length

  override def borrowLeftTo(t: Block[K,V])(implicit ctx: Context[K,V]): Leaf[K,V] = {
    val target = t.asInstanceOf[Leaf[K,V]]

    val len = tuples.length
    val start = len - target.minNeeded()

    target.tuples = tuples.slice(start, len) ++ target.tuples
    tuples = tuples.slice(0, start)

    target
  }

  override def borrowRightTo(t: Block[K,V])(implicit ctx: Context[K,V]): Block[K,V] = {
    val target = t.asInstanceOf[Leaf[K,V]]

    val n = target.minNeeded()
    target.tuples = target.tuples ++ tuples.slice(0, n)
    tuples = tuples.slice(n, tuples.length)

    target
  }

  override def merge(r: Block[K,V])(implicit ctx: Context[K,V]): Block[K,V] = {
    val right = r.asInstanceOf[Leaf[K,V]]

    tuples = tuples ++ right.tuples
    this
  }

  override def isFull(): Boolean = tuples.length == MAX
  override def isEmpty(): Boolean = tuples.isEmpty

  def inOrder(): Seq[Tuple[K,V]] = tuples

  override def hasMinimum(): Boolean = tuples.length >= MIN

  override def copy()(implicit ctx: Context[K, V]): Leaf[K,V] = {
    //if(ctx.isNew(unique_id)) return this
    if(isNew) return this

    val (p, pos) = ctx.getParent(unique_id).get

    val copy = ctx.createLeaf()
    ctx.setParent(copy.unique_id, pos, p)

    val len = tuples.length

    for(i<-0 until len){
      val (k, v, vs) = tuples(i)
      copy.tuples = copy.tuples :+ Tuple3(k, v, vs)
    }

    copy.level = level

    copy
  }
  
  override def split()(implicit ctx: Context[K,V]): Leaf[K,V] = {
    val right = ctx.createLeaf()

    val len = tuples.length
    val pos = len/2

    right.tuples = tuples.slice(pos, len)
    tuples = tuples.slice(0, pos)

    right.level = level

    right
  }

  def binSearch(k: K, start: Int = 0, end: Int = tuples.length - 1)(implicit ord: Ordering[K]): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, tuples(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return binSearch(k, start, pos - 1)

    binSearch(k, pos + 1, end)
  }

  def find(k: K)(implicit ord: Ordering[K]): Option[Tuple[K,V]] = {
    val (found, pos) = binSearch(k)

    if(!found) return None

    Some(tuples(pos))
  }

  def findPath(k: K)(implicit ord: Ordering[K]): (Boolean, Tuple[K, V]) = {
    val (found, pos) = binSearch(k)
    found -> tuples(if(pos == tuples.length) pos - 1 else pos)
  }

  def lt(k: K, include: Boolean = false)(implicit ord: Ordering[K]): Seq[Tuple[K,V]] = {
    if(include) return tuples.filter{case (k1, _, _) => ord.lteq(k1, k)}.reverse
    tuples.filter{case (k1, _, _) => ord.lt(k1, k)}.reverse
  }

  def lte(k: K)(implicit ord: Ordering[K]): Seq[Tuple[K,V]] = {
    lt(k, true)
  }

  def gt(k: K, include: Boolean = false)(implicit ord: Ordering[K]): Seq[Tuple[K,V]] = {
    if(include) return tuples.filter{case (k1, _, _) => ord.gteq(k1, k)}
    tuples.filter{case (k1, _, _) => ord.gt(k1, k)}
  }

  def gte(k: K)(implicit ord: Ordering[K]): Seq[Tuple[K,V]] = {
    gt(k, true)
  }

  def interval(lower: K, upper: K, includeLower: Boolean = false, includeUpper: Boolean = false)(implicit ord: Ordering[K]): Seq[Tuple[K,V]] = {
    tuples.filter{case (k, _, _) => (if(includeLower) ord.gteq(k, lower) else ord.gt(k, lower)) && (if(includeUpper) ord.lteq(k, upper) else ord.lt(k, upper))}
  }

  def min()(implicit ord: Ordering[K]): Option[Tuple[K,V]] = {
    if(tuples.isEmpty) return None
    Some(tuples.minBy(_._1))
  }

  def max()(implicit ord: Ordering[K]): Option[Tuple[K,V]] = {
    if(tuples.isEmpty) return None
    Some(tuples.maxBy(_._1))
  }

  override def print()(implicit kf: K => String, vf: V => String): String = {
    if(tuples.isEmpty) return "[]"

    val sb = new StringBuilder(s"${id}:")
    sb ++= Console.GREEN_B
    sb ++= "["
    sb ++= Console.RESET

    for(i<-0 until tuples.length - 1){
      val (k, v, _) = tuples(i)
      sb ++= kf(k)

      sb ++= "->"

      sb ++= vf(v)
      sb ++= ","
    }

    sb ++= Console.RED_B
    val (k, v, _) = tuples(tuples.length - 1)
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
