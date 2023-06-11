package services.scalable.index

import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.RichAsyncIndexIterator

import scala.concurrent.Future

/**
 * Any search function can be constructed using any of basic iterators: find(), previousKey(), nextKey(),
 * head() and tail()
 */
class QueryableIndexTest[K, V](override val descriptor: IndexContext)(override val builder: IndexBuilder[K, V])
  extends Index[K, V](descriptor)(builder) {

  override val $this = this

  import builder._

  def find(k: K): Future[Option[Leaf[K, V]]] = {
    findPath(k)
  }

  def previousKey(k: K, orEqual: Boolean): Future[Option[Leaf[K, V]]] = {
    findPath(k)(builder.ord).flatMap {
      case None => Future.successful(None)
      case Some(leaf) =>

        val (_, idx) = leaf.binSearch(k)
        val (key, _, _) = leaf.tuples(idx)

        val r = builder.ord.compare(key, k)

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

  def nextKey(k: K, orEqual: Boolean): Future[Option[Leaf[K, V]]] = {
    findPath(k)(builder.ord).flatMap {
      case None => Future.successful(None)
      case Some(leaf) =>

        val (_, idx) = leaf.binSearch(k)
        val (key, _, _) = leaf.tuples(idx)

        val r = builder.ord.compare(key, k)

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

  def asc(term: K, termInclusive: Boolean): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return nextKey(term, termInclusive).map { block =>
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

          /*val pos = block.tuples.indexWhere { case (k1, _, _) => if (termInclusive) builder.ord.gteq(k1, term)
            else builder.ord.gt(k1, term)
          }
          val list = if (pos >= 0) block.tuples.slice(pos, block.tuples.length).takeWhile { case (k1, _, _) =>
            if (termInclusive) builder.ord.gteq(k1, term) else builder.ord.gt(k1, term)
          }
          else Seq.empty[(K, V, String)]*/

          stop = list.isEmpty

          //list.filter(filter)
          list
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

  def desc(term: K, termInclusive: Boolean): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return previousKey(term, termInclusive).map { block =>
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

          list
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

          list
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

          list
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

  def lt(term: K, termInclusive: Boolean): RichAsyncIndexIterator[K, V] = {
    val it = head()

    it.filter = x => {
      if (termInclusive) {
        builder.ord.lteq(x._1, term)
      } else {
        builder.ord.lt(x._1, term)
      }
    }

    it
  }

  def gt(term: K, termInclusive: Boolean): RichAsyncIndexIterator[K, V] = {
    val it = asc(term, termInclusive)

    it.filter = x => {
      if(termInclusive){
        builder.ord.gteq(x._1, term)
      } else {
        builder.ord.gt(x._1, term)
      }
    }

    it
  }

  def ltr(term: K, termInclusive: Boolean): RichAsyncIndexIterator[K, V] = {
    val it = desc(term, termInclusive)

    it.filter = x => {
      if (termInclusive) {
        builder.ord.lteq(x._1, term)
      } else {
        builder.ord.lt(x._1, term)
      }
    }

    it
  }

  def gtr(term: K, termInclusive: Boolean): RichAsyncIndexIterator[K, V] = {
    val it = tail()

    it.filter = x => {
      if (termInclusive) {
        builder.ord.gteq(x._1, term)
      } else {
        builder.ord.gt(x._1, term)
      }
    }

    it
  }

}
