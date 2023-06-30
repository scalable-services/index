package services.scalable.index.impl

import services.scalable.index.{AsyncIndexIterator, Block, Tuple}

/**
 * This is a single threaded iterator that can be copied.
 *
 * @param filter
 * @param limit
 * @tparam K
 * @tparam V
 */
abstract class RichAsyncIndexIterator[K, V](var filter: Tuple[K, V] => Boolean = (_: Tuple[K, V]) => true,
                                            var limit: Int = -1) extends AsyncIndexIterator[Seq[Tuple[K, V]]] {

  assert(limit != 0, "Limit must be < 0 (infinite) or > 0!")

  protected var counter = 0
  protected var cur: Option[Block[K, V]] = None

  protected var firstTime = false
  protected var stop = false

  def checkCounter(filtered: Seq[Tuple[K, V]]): Seq[Tuple[K, V]] = {
    if(limit < 0) {
      counter += filtered.length
      return filtered
    }

    if(counter + filtered.length >= limit){
      stop = true
    }

    val n = Math.min(filtered.length, limit - counter)

    if(n <= 0) return Seq.empty[Tuple[K, V]]

    counter += n

    filtered.slice(0, n)
  }

}
