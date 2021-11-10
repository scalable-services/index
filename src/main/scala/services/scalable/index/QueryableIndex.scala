package services.scalable.index

import scala.concurrent.{ExecutionContext, Future}

class QueryableIndex[K, V]()(override implicit val ec: ExecutionContext, override val ctx: Context[K, V],
                             implicit val order: Ordering[K]) extends Index[K, V]()(ec, ctx) {

  override val $this = this

  protected def ltr(prefix: Option[K], term: K, inclusive: Boolean)(prefixOrd: Option[Ordering[K]], termOrd: Ordering[K]): RichAsyncIterator[K, V] = {
    new RichAsyncIterator[K, V] {

      // Find the first prefix...
      val sord = if(prefix.isDefined) new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          val r = prefixOrd.get.compare(prefix.get, y)

          if(r != 0) return r

          termOrd.compare(term, y)
        }
      } else new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          termOrd.compare(term, y)
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && (inclusive && termOrd.lteq(k, term) || termOrd.lt(k, term))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(term)(sord).flatMap {
            case None =>
              cur = None
              Future.successful(Seq.empty[Tuple[K, V]])

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }.reverse
              //stop = filtered.isEmpty

              // Maybe we got into next block...
              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
              }
          }
        }

        $this.prev(cur.map(_.unique_id))(order).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _) => check(k) }.reverse
            stop = filtered.isEmpty

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  def lt(prefix: Option[K], term: K, inclusive: Boolean, reverse: Boolean)(prefixOrd: Option[Ordering[K]], termOrd: Ordering[K]): RichAsyncIterator[K, V] = {

    if(reverse){
      return ltr(prefix, term, inclusive)(prefixOrd, termOrd)
    }

    val sord = if(prefix.isEmpty) new Ordering[K] {
      override def compare(x: K, y: K): Int = -1
    } else new Ordering[K] {
      override def compare(x: K, y: K): Int = {
        val r = prefixOrd.get.compare(prefix.get, y)

        if(r != 0) return r

        -1
      }
    }

    new RichAsyncIterator[K, V] {

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && (inclusive && termOrd.lteq(k, term) || termOrd.lt(k, term))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(term)(sord).flatMap {
            case None =>
              cur = None
              Future.successful(Seq.empty[Tuple[K, V]])

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              //stop = filtered.isEmpty

              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
              }

          }
        }

        $this.next(cur.map(_.unique_id))(order).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _) => check(k) }
            stop = filtered.isEmpty

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  protected def gtr(prefix: Option[K], term: K, inclusive: Boolean)(prefixOrd: Option[Ordering[K]], termOrd: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

      // Find the last prefix...
      val sord = if(prefix.isDefined) new Ordering[K] {
        override def compare(x: K, y: K): Int = {

          val r = prefixOrd.get.compare(prefix.get, y)

          if(r != 0) return r

          1
        }
      }
      // Iterate from the last block
      else new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          1
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && (inclusive && termOrd.gteq(k, term) || termOrd.gt(k, term))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(term)(sord).flatMap {
            case None =>
              cur = None
              Future.successful(Seq.empty[Tuple[K, V]])

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }.reverse
              //stop = filtered.isEmpty

              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
              }
          }
        }

        $this.prev(cur.map(_.unique_id))(order).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _) => check(k) }.reverse
            stop = filtered.isEmpty

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  def gt(prefix: Option[K], term: K, inclusive: Boolean, reverse: Boolean)(prefixOrd: Option[Ordering[K]], termOrd: Ordering[K]): RichAsyncIterator[K, V] = {

    if(reverse){
      return gtr(prefix, term, inclusive)(prefixOrd, termOrd)
    }

    val sord = if(prefix.isDefined) new Ordering[K] {
      override def compare(x: K, y: K): Int = {
        val r = prefixOrd.get.compare(prefix.get, y)

        if(r != 0) return r

        order.compare(term, y)
      }
    } else new Ordering[K] {
      override def compare(x: K, y: K): Int = {
        termOrd.compare(x, y)
      }
    }

    new RichAsyncIterator[K, V] {

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && (inclusive && termOrd.gteq(k, term) || termOrd.gt(k, term))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(term)(sord).flatMap {
            case None =>
              cur = None
              Future.successful(Seq.empty[Tuple[K, V]])

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              //stop = filtered.isEmpty

              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
              }
          }
        }

        $this.next(cur.map(_.unique_id))(order).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _) => check(k) }
            stop = filtered.isEmpty

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  protected def ranger(lowerPrefix: Option[K], upperPrefix: Option[K], lowerTerm: K, upperTerm: K, inclusiveLower: Boolean, inclusiveUpper: Boolean)
  (lowerPrefixOrder: Option[Ordering[K]], upperPrefixOrder: Option[Ordering[K]], lowerOrder: Ordering[K], upperOrder: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

      // Find the last prefix...
      val sord = if(upperPrefix.isDefined) new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          val r = upperPrefixOrder.get.compare(upperPrefix.get, y)

          if(r != 0) return r

          upperOrder.compare(upperTerm, y)
        }
      }
      // Find the last element
      else new Ordering[K] {
        override def compare(x: K, y: K): Int = {

          /*val r = upperOrder.compare(upperTerm, y)
          if(r != 0) return r
          if(inclusiveUpper) 1 else -1*/

          upperOrder.compare(upperTerm, y)
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        ((lowerPrefix.isEmpty || lowerPrefixOrder.get.equiv(k, lowerPrefix.get)) && (inclusiveLower && lowerOrder.gteq(k, lowerTerm) || lowerOrder.gt(k, lowerTerm))) &&
          ((upperPrefix.isEmpty || upperPrefixOrder.get.equiv(k, upperPrefix.get)) && (inclusiveUpper && upperOrder.lteq(k, upperTerm) || upperOrder.lt(k, upperTerm)))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(lowerTerm)(sord).flatMap {
            case None =>
              cur = None
              Future.successful(Seq.empty[Tuple[K, V]])

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }.reverse
              //stop = filtered.isEmpty

              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
              }

          }
        }

        $this.prev(cur.map(_.unique_id))(order).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _) => check(k) }.reverse
            stop = filtered.isEmpty

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  def range(lowerPrefix: Option[K], upperPrefix: Option[K], lowerTerm: K, upperTerm: K, inclusiveLower: Boolean, inclusiveUpper: Boolean, reverse: Boolean)
           (lowerPrefixOrder: Option[Ordering[K]], upperPrefixOrder: Option[Ordering[K]], lowerOrder: Ordering[K], upperOrder: Ordering[K]): RichAsyncIterator[K, V] = {

    /*// Check if interval is valid
    assert((lowerPrefix.isDefined && upperPrefix.isDefined && lowerPrefixOrder.isDefined && upperPrefixOrder.isDefined &&
      upperPrefixOrder.get.gteq(upperPrefix.get, lowerPrefix.get) && upperOrder.gteq(upperTerm, lowerTerm)) || (lowerPrefixOrder.isEmpty && upperPrefixOrder.isEmpty && upperOrder.gteq(upperTerm, lowerTerm)))*/

    if(reverse){
      return ranger(lowerPrefix, upperPrefix, lowerTerm, upperTerm, inclusiveLower, inclusiveUpper)(lowerPrefixOrder, upperPrefixOrder, lowerOrder, upperOrder)
    }

    new RichAsyncIterator[K, V] {

      // Find the first prefix...
      val sord = if(lowerPrefix.isDefined) new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          val r = lowerPrefixOrder.get.compare(lowerPrefix.get, y)

          if(r != 0) return r

          lowerOrder.compare(lowerTerm, y)
        }
      } else new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          lowerOrder.compare(lowerTerm, y)
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        ((lowerPrefix.isEmpty || lowerPrefixOrder.get.equiv(k, lowerPrefix.get)) && (inclusiveLower && lowerOrder.gteq(k, lowerTerm) || lowerOrder.gt(k, lowerTerm))) &&
          ((upperPrefix.isEmpty || upperPrefixOrder.get.equiv(k, upperPrefix.get)) && (inclusiveUpper && upperOrder.lteq(k, upperTerm) || upperOrder.lt(k, upperTerm)))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(lowerTerm)(sord).flatMap {
            case None =>
              cur = None
              Future.successful(Seq.empty[Tuple[K, V]])

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              //stop = filtered.isEmpty

              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
              }

          }
        }

        $this.next(cur.map(_.unique_id))(order).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _) => check(k) }
            stop = filtered.isEmpty

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  def lt(term: K, inclusive: Boolean, reverse: Boolean)(implicit ord: Ordering[K]): RichAsyncIterator[K, V] = {
    lt(None, term, inclusive, reverse)(None, ord)
  }

  def lt(prefix: K, term: K, inclusive: Boolean, reverse: Boolean)(prefixOrd: Ordering[K], termOrd: Ordering[K]): RichAsyncIterator[K, V] = {
    lt(Some(prefix), term, inclusive, reverse)(Some(prefixOrd), termOrd)
  }

  def gt(term: K, inclusive: Boolean, reverse: Boolean)(implicit ord: Ordering[K]): RichAsyncIterator[K, V] = {
    gt(None, term, inclusive, reverse)(None, ord)
  }

  def gt(prefix: K, term: K, inclusive: Boolean, reverse: Boolean)(prefixOrd: Ordering[K], termOrd: Ordering[K]): RichAsyncIterator[K, V] = {
    gt(Some(prefix), term, inclusive, reverse)(Some(prefixOrd), termOrd)
  }

  def range(lowerPrefix: K, lowerTerm: K, upperPrefix: K, upperTerm: K, includeLower: Boolean, includeUpper: Boolean, reverse: Boolean)(lowerPrefixOrd: Ordering[K],
                              upperPrefixOrd: Ordering[K],
                              lowerTermOrd: Ordering[K],
                              upperTermOrd: Ordering[K]): RichAsyncIterator[K, V] = {
    range(Some(lowerPrefix), Some(upperPrefix), lowerTerm, upperTerm, includeLower, includeUpper, reverse)(Some(lowerPrefixOrd),
      Some(upperPrefixOrd), lowerTermOrd, upperTermOrd)
  }

  def range(lowerTerm: K, upperTerm: K, includeLower: Boolean, includeUpper: Boolean, reverse: Boolean)(lowerTermOrd: Ordering[K], upperTermOrd: Ordering[K]): RichAsyncIterator[K, V] = {
    range(None, None, lowerTerm, upperTerm, includeLower, includeUpper, reverse)(None, None, lowerTermOrd, upperTermOrd)
  }

}
