package services.scalable.index.impl

import services.scalable.index.{AsyncIterator, Block, Tuple}

/**
 * This is a single threaded iterator that can be copied.
 * @param filter
 * @param limit
 * @tparam K
 * @tparam V
 */
abstract class RichAsyncIterator[K, V](var filter: Tuple[K, V] => Boolean = (_: Tuple[K, V]) => true,
                                       var limit: Int = Int.MaxValue) extends AsyncIterator[Seq[Tuple[K, V]]] {

  protected var counter = 0
  protected var cur: Option[Block[K, V]] = None

  protected var firstTime = false
  protected var stop = false

  def checkCounter(filtered: Seq[Tuple[K, V]]): Seq[Tuple[K, V]] = {
    val len = filtered.length

    if(counter + len >= limit){
      stop = true
    }

    val n = Math.min(len, limit - counter)

    counter += n

    filtered.slice(0, n)
  }

}
