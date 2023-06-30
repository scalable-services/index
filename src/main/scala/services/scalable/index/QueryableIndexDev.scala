package services.scalable.index

import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.RichAsyncIndexIterator
import scala.concurrent.Future

/**
 * Any iterator can be constructed using a searching function and a filter function
 */
class QueryableIndexDev[K, V](override val descriptor: IndexContext)(override val builder: IndexBuilder[K, V])
  extends Index[K, V](descriptor)(builder) {

  override val $this = this

  import builder._

  def find(k: K): Future[Option[Leaf[K, V]]] = {
    findPath(k)
  }

  def previousKey(k: K, orEqual: Boolean)(termComp: Ordering[K]): Future[Option[Leaf[K, V]]] = {
    findPath(k)(termComp).flatMap {
      case None => Future.successful(None)
      case Some(leaf) =>

        var (_, idx) = leaf.binSearch(k)
        idx = if(idx == leaf.length) leaf.length - 1 else idx
        val (key, _, _) = leaf.tuples(idx)

        val r = termComp.compare(key, k)

        if (r == 0) {
          if (orEqual || idx > 0) {
            Future.successful(Some(leaf))
          } else {
            prev(Some(leaf.unique_id))
          }
        } else {
          Future.successful(Some(leaf))
        }
    }
  }

  def nextKey(k: K, orEqual: Boolean)(termComp: Ordering[K]): Future[Option[Leaf[K, V]]] = {
    findPath(k)(termComp).flatMap {
      case None => Future.successful(None)
      case Some(leaf) =>

        var (_, idx) = leaf.binSearch(k)
        idx = if(idx == leaf.length) leaf.length - 1 else idx
        val (key, _, _) = leaf.tuples(idx)

        val r = termComp.compare(key, k)

        if (r == 0) {
          if (orEqual || idx < leaf.length - 1) {
            Future.successful(Some(leaf))
          } else {
            next(Some(leaf.unique_id))
          }
        } else {
          Future.successful(Some(leaf))
        }
    }
  }

  def asc(term: K, termInclusive: Boolean)(termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return nextKey(term, termInclusive)(termComp).map { block =>
          root = block.map(_.unique_id)
          first = false

          block.isDefined
        }

        $this.next(root)(termComp).map { block =>
          root = block.map(_.unique_id)
          block.isDefined
        }
      }

      private def next1(): Future[Seq[(K, V, String)]] = {
        if (root.isEmpty || stop) return Future.successful(Seq.empty[(K, V, String)])
        ctx.getLeaf(root.get).map { block =>
          val list = block.tuples.filter(filter)
          stop = list.isEmpty
          if(stop) list else checkCounter(list)
        }
      }

      override def next(): Future[Seq[(K, V, String)]] = {
        if (first) return hasNext().flatMap { _ =>
          next1()
        }

        next1()
      }
    }
  }

  def desc(term: K, termInclusive: Boolean)(termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return previousKey(term, termInclusive)(termComp).map { block =>
          root = block.map(_.unique_id)
          first = false

          block.isDefined
        }

        $this.prev(root)(termComp).map { block =>
          root = block.map(_.unique_id)
          block.isDefined
        }
      }

      private def prev1(): Future[Seq[(K, V, String)]] = {
        if (root.isEmpty || stop) return Future.successful(Seq.empty[(K, V, String)])
        ctx.getLeaf(root.get).map { block =>
          val list = block.tuples.reverse.filter(filter)
          stop = list.isEmpty
          if(stop) list else checkCounter(list)
        }
      }

      override def next(): Future[Seq[(K, V, String)]] = {
        if (first) return hasNext().flatMap { _ =>
          prev1()
        }

        prev1()
      }
    }
  }

  def head(): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return $this.first().map { block =>
          root = block.map(_.unique_id)
          first = false

          block.isDefined
        }

        $this.next(root)(builder.ord).map { block =>
          root = block.map(_.unique_id)
          block.isDefined
        }
      }

      private def next1(): Future[Seq[(K, V, String)]] = {
        if (root.isEmpty || stop) return Future.successful(Seq.empty[(K, V, String)])
        ctx.getLeaf(root.get).map { block =>
          val list = block.tuples.filter(filter)
          stop = list.isEmpty
          if(stop) list else checkCounter(list)
        }
      }

      override def next(): Future[Seq[(K, V, String)]] = {
        if (first) return hasNext().flatMap { _ =>
          next1()
        }

        next1()
      }
    }
  }

  def tail(): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return $this.last().map { block =>
          root = block.map(_.unique_id)
          first = false

          block.isDefined
        }

        $this.prev(root)(builder.ord).map { block =>
          root = block.map(_.unique_id)
          block.isDefined
        }
      }

      private def prev1(): Future[Seq[(K, V, String)]] = {
        if (root.isEmpty || stop) return Future.successful(Seq.empty[(K, V, String)])
        ctx.getLeaf(root.get).map { block =>
          val list = block.tuples.reverse.filter(filter)
          stop = list.isEmpty
          if(stop) list else checkCounter(list)
        }
      }

      override def next(): Future[Seq[(K, V, String)]] = {
        if (first) return hasNext().flatMap { _ =>
          prev1()
        }

        prev1()
      }
    }
  }

  def lt(term: K, termInclusive: Boolean, reverse: Boolean)(termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    val it = if(reverse) desc(term, termInclusive)(termComp) else head()

    it.filter = x => {
      (termInclusive && termComp.lteq(x._1, term)) || termComp.lt(x._1, term)
    }

    it
  }

  def lt(prefix: K, term: K, termInclusive: Boolean, reverse: Boolean)(prefixComp: Ordering[K], termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    val it = if (reverse) desc(term, termInclusive)(termComp) else
      asc(prefix, true)(termComp)

    it.filter = k => {
      prefixComp.equiv(k._1, prefix) && (termInclusive && termComp.lteq(k._1, term) || termComp.lt(k._1, term))
    }

    it
  }

  def gt(term: K, termInclusive: Boolean, reverse: Boolean)(termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    val it = if(reverse) tail() else asc(term, termInclusive)(termComp)

    it.filter = k => {
      (termInclusive && termComp.gteq(k._1, term)) || termComp.gt(k._1, term)
    }

    it
  }

  def desc2(term: K, termInclusive: Boolean)(termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return nextKey(term, termInclusive)(termComp).map { block =>
          root = block.map(_.unique_id)
          first = false

          block.isDefined
        }

        $this.prev(root)(termComp).map { block =>
          root = block.map(_.unique_id)
          block.isDefined
        }
      }

      private def prev1(): Future[Seq[(K, V, String)]] = {
        if (root.isEmpty || stop) return Future.successful(Seq.empty[(K, V, String)])
        ctx.getLeaf(root.get).map { block =>
          val list = block.tuples.reverse.filter(filter)
          stop = list.isEmpty
          if(stop) list else checkCounter(list)
        }
      }

      override def next(): Future[Seq[(K, V, String)]] = {
        if (first) return hasNext().flatMap { _ =>
          prev1()
        }

        prev1()
      }
    }
  }

  def prefix(term: K, reverse: Boolean)(prefixComp: Ordering[K], termComp: Ordering[K] = ord): RichAsyncIndexIterator[K, V] = {
    val it = if(reverse) desc2(term, true)(termComp)
      else asc(term, true)(termComp)

    it.filter = x => {
      prefixComp.equiv(x._1, term)
    }

    it
  }

  protected def gtReversePrefix(prefix: K, term: K, termInclusive: Boolean)(prefixComp: Ordering[K], termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return findPath(term, None, (k, m, ord) => {
          val reversed = m.pointers.reverse

          var idx = reversed.indexWhere{ case (k, p) =>
            prefixComp.gteq(k, prefix) && (termInclusive && termComp.gteq(k, term) || termComp.gt(k, term))
          }

          idx = if(idx < 0) 0 else idx

          val id = reversed(idx)._2.unique_id

          id
        })(termComp).map { block =>
          root = block.map(_.unique_id)
          first = false

          block.isDefined
        }

        $this.prev(root)(termComp).map { block =>
          root = block.map(_.unique_id)
          block.isDefined
        }
      }

      private def prev1(): Future[Seq[(K, V, String)]] = {
        if (root.isEmpty || stop) return Future.successful(Seq.empty[(K, V, String)])
        ctx.getLeaf(root.get).map { block =>
          val list = block.tuples.reverse.filter(filter)
          if(stop) list else checkCounter(list)
        }
      }

      override def next(): Future[Seq[(K, V, String)]] = {
        if (first) return hasNext().flatMap { _ =>
          prev1()
        }

        prev1()
      }
    }
  }

  def gt(prefix: K, term: K, termInclusive: Boolean, reverse: Boolean)(prefixComp: Ordering[K], termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    val it = if(reverse) gtReversePrefix(prefix, term, termInclusive)(prefixComp, termComp)
      else asc(term, termInclusive)(termComp)

    it.filter = k => {
      prefixComp.equiv(k._1, prefix) && (termInclusive && termComp.gteq(k._1, term) || termComp.gt(k._1, term))
    }

    it
  }

  def range(from: K, to: K, fromInclusive: Boolean, toInclusive: Boolean, reverse: Boolean)(termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    val it = if(reverse) desc(to, toInclusive)(termComp) else asc(from, fromInclusive)(termComp)

    it.filter = k => {
      ((fromInclusive && termComp.gteq(k._1, from)) || termComp.gt(k._1, from)) &&
        ((toInclusive && termComp.lteq(k._1, to)) || termComp.lt(k._1, to))
    }

    it
  }
}
