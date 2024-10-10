package services.scalable.index

import org.slf4j.LoggerFactory

import java.util.UUID
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

  override def middle: K  = tuples(tuples.length/2)._1

  override def lastOption: Option[K] = tuples.lastOption.map(_._1)
  override def firstOption: Option[K] = tuples.headOption.map(_._1)

  override def middleOption: Option[K] = tuples.length match {
    case 0 => None
    case _ => Some(tuples(tuples.length/2)._1)
  }

  override def nSubtree: Long = tuples.length.toLong

  def insert(data: Seq[Tuple3[K, V, Boolean]], insertVersion: String)(implicit ctx: Context[K,V]): Try[Int] = {
    import ctx.builder.ord

    if(isFull()) return Failure(Errors.LEAF_BLOCK_FULL)

    val n = Math.min(MAX - tuples.length, data.length)
    val slice = data.slice(0, n)

    val len = slice.length

    if (slice.exists { case (k, _, upsert) => tuples.exists { case (k1, _, _) => !upsert && ord.equiv(k1, k) } }) {
      return Failure(Errors.LEAF_DUPLICATE_KEY(slice.map(_._1), ctx.builder.ks))
    }

    // Filter out upsert keys...
    val upserts = slice.filter(_._3)
    tuples = tuples.filterNot{case (k, v, _) => upserts.exists{case (k1, _, _) => ord.equiv(k, k1)}}

    // Add back the upsert keys and the new ones...
    tuples = (tuples ++ slice.map{case (k, v, _) => Tuple3(k, v, insertVersion)}).sortBy(_._1)

    Success(len)
  }

  def remove(keys: Seq[Tuple2[K, Option[String]]])(implicit ctx: Context[K, V]): Try[Int] = {
    import ctx.builder.ord

    if(keys.exists{ case (k, _) => !tuples.exists{ case (k1, _, _) => ord.equiv(k1, k) }}){
      return Failure(Errors.LEAF_KEY_NOT_FOUND[K](keys.map(_._1), ctx.builder.ks))
    }

    val versionsChanged = keys.filter(_._2.isDefined)
      .filter { case (k0, vs0) => tuples.exists { case (k1, _, vs1) => ord.equiv(k0, k1) && !vs0.get.equals(vs1) } }
      .map { case (k1, vs1) =>
        val (k, _, vs0) = tuples.find { case (k0, _, _) => ord.equiv(k0, k1) }.get

        (k, Some(vs0), vs1)
      }

    if (!versionsChanged.isEmpty) {
      return Failure(Errors.VERSION_CHANGED(versionsChanged, ctx.builder.ks))
    }

    tuples = tuples.filterNot{case (k, _, _) => keys.exists{ case (k1, _) => ord.equiv(k, k1)}}

    Success(keys.length)
  }

  def update(data: Seq[Tuple3[K, V, Option[String]]], updateVersion: String)(implicit ctx: Context[K, V]): Try[Int] = {
    import ctx.builder.ord

    if(data.exists{ case (k, _, _) => !tuples.exists{case (k1, _, _) => ord.equiv(k1, k) }}){
      return Failure(Errors.LEAF_KEY_NOT_FOUND(data.map(_._1), ctx.builder.ks))
    }

    val versionsChanged = data.filter(_._3.isDefined)
      .filter{case (k0, _, vs0) => tuples.exists{case (k1, _, vs1) => ord.equiv(k0, k1) && !vs0.get.equals(vs1)}}
      .map { case (k1, _, vs1) =>
        val (k, _, vs0) = tuples.find { case (k0, _, _) => ord.equiv(k0, k1) }.get

        (k, Some(vs0), vs1)
      }

    if (!versionsChanged.isEmpty) {
      return Failure(Errors.VERSION_CHANGED(versionsChanged, ctx.builder.ks))
    }

    val notin = tuples.filterNot{case (k1, _, _) => data.exists{ case (k, _, _) => ord.equiv(k, k1)}}

    tuples = (notin ++ data.map{case (k, v, _) => Tuple3(k, v, updateVersion)}).sortBy(_._1)

    Success(data.length)
  }

  override def length: Int = tuples.length

  override def borrow(t: Block[K,V])(implicit ctx: Context[K,V]): Block[K,V] = {
    val target = t.asInstanceOf[Leaf[K,V]]
    val targetHead = target.tuples.head._1
    val thisHead = tuples.head._1
    val minKeys = target.minNeeded()

    // borrows left
    if(ctx.builder.ord.gteq(thisHead, targetHead)){

      target.tuples = target.tuples ++ tuples.slice(0, minKeys)
      tuples = tuples.slice(minKeys, tuples.length)

      return this
    }

    val start = tuples.length - minKeys

    target.tuples = tuples.slice(start, tuples.length) ++ target.tuples
    tuples = tuples.slice(0, start)

    this
  }

  override def merge(r: Block[K, V], version: String)(implicit ctx: Context[K,V]): Block[K, V] = {
    val right = r.asInstanceOf[Leaf[K, V]]

    //tuples = tuples ++ right.tuples
    insert(right.tuples.map{case (k, v, _) => (k, v, false)}, version)

    this
  }

  override def isFull(): Boolean = tuples.length == MAX
  override def isEmpty(): Boolean = tuples.isEmpty

  def inOrder(): Seq[Tuple[K,V]] = tuples

  override def hasMinimum(): Boolean = tuples.length >= MIN

  override def hasEnough(): Boolean = tuples.length > MIN

  override def copy()(implicit ctx: Context[K, V]): Leaf[K, V] = {
    if(isNew) return this

    logger.debug(s"Creating leaf copy ${unique_id}...")

    val pinfo = ctx.getParent(unique_id).get

    val copy = ctx.createLeaf()
    ctx.setParent(copy.unique_id, pinfo.key, pinfo.pos, pinfo.parent)

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

  override def binSearch(k: K, start: Int = 0, end: Int = tuples.length - 1)(implicit ord: Ordering[K]): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, tuples(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return binSearch(k, start, pos - 1)

    binSearch(k, pos + 1, end)
  }

  override def findPosition(k: K)(implicit ord: Ordering[K]): Int = {
    val (_, pos) = binSearch(k)
    if (pos < tuples.length) pos else pos - 1
  }

  def find(k: K)(implicit ord: Ordering[K]): Option[Tuple[K,V]] = {
    val (found, pos) = binSearch(k)

    if(!found) return None

    val e = tuples(pos)

    logger.debug(s"[leaf search in ${id} at pos ${pos}] => ${e._1}")

    Some(e)
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

  def previousKey(k: K)(implicit ord: Ordering[K], ctx: Context[K, V]): Option[Tuple[K, V]] = {
    /*val idx = findPosition(k)(ord)
    val e = tuples(idx)._1

    logger.debug(s"pos: ${idx} e: ${ctx.builder.ks(e)} all keys: ${tuples.map(x => ctx.builder.ks(x._1))}")

    val isOk = ord.lt(e, k)

    if(isOk) Some(tuples(idx)) else Some(tuples(idx - 1))*/

    val reversed = tuples.reverse
    val idx = reversed.indexWhere{case (k1, _, _) => ord.lt(k1, k)}

    if(idx < 0) None else Some(reversed(idx))
  }

  def nextKey(k: K)(implicit ord: Ordering[K], ctx: Context[K, V]): Option[Tuple[K, V]] = {
    /*val idx = findPosition(k)(ord)
    val e = tuples(idx)._1

    logger.debug(s"pos: ${idx} e: ${ctx.builder.ks(e)} all keys: ${tuples.map(x => ctx.builder.ks(x._1))}")

    val isOk = ord.gt(e, k)

    if(isOk) Some(tuples(idx)) else Some(tuples(idx + 1))*/

    val idx = tuples.indexWhere{case (k1, _, _) => ord.gt(k1, k)}
    if(idx < 0) None else Some(tuples(idx))
  }

  override def print()(implicit ctx: Context[K, V]): String = {
    if(tuples.isEmpty) return "[]"

    val sb = new StringBuilder(s"id=[${id}, len=${length}]:")
    sb ++= Console.GREEN_B
    sb ++= "["
    sb ++= Console.RESET

    for(i<-0 until tuples.length - 1){
      val (k, v, _) = tuples(i)
      sb ++= s"""[${i}]${ctx.builder.ks(k)}"""

      sb ++= "->"

      sb ++= ctx.builder.vs(v)
      sb ++= ","
    }

    sb ++= Console.RED_B
    val (k, v, _) = tuples(tuples.length - 1)
    sb ++= s"""[${tuples.length-1}]${ctx.builder.ks(k)}"""
    sb ++= Console.RESET

    sb ++= "->"

    sb ++= ctx.builder.vs(v)

    sb ++= Console.GREEN_B
    sb ++= "]"
    sb ++= Console.RESET

    sb.toString()
  }
}
