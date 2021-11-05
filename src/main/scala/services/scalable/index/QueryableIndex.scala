package services.scalable.index

import services.scalable.index.grpc.Datom
import scala.concurrent.{ExecutionContext, Future}

class QueryableIndex[K, V]()(override implicit val ec: ExecutionContext, override val ctx: Context[K, V],
                           implicit val order: Ordering[K]) extends Index()(ec, ctx) {

  override val $this = this

  def gtr(term: K, prefix:Option[K], inclusive: Boolean, prefixOrder: Option[Ordering[K]], termOrd: Ordering[K]): RichAsyncIterator[K, V] = {
    new RichAsyncIterator[K, V] {

      // Find the last prefix...
      val gtOrd = if(prefix.isDefined) new Ordering[K] {
        override def compare(x: K, y: K): Int = {

          val r = prefixOrder.get.compare(prefix.get, y)

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
        (prefix.isEmpty || prefixOrder.get.equiv(k, prefix.get)) && (inclusive && termOrd.gteq(k, term) || termOrd.gt(k, term))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          // From the first block with e >= term to the last block.
          return findPath(term)(gtOrd).map {
            case None =>
              cur = None
              Seq.empty[Tuple[K, V]]

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }.reverse
              //stop = filtered.isEmpty

              checkCounter(filtered.filter{case (k, v) => filter(k, v)})
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

  def gt(term: K, prefix:Option[K], inclusive: Boolean, prefixOrder: Option[Ordering[K]], termOrd: Ordering[K], reverse: Boolean): RichAsyncIterator[K, V] = {

    if(reverse){
      return gtr(term, prefix, inclusive, prefixOrder, termOrd)
    }

    new RichAsyncIterator[K, V] {

      val gtOrd = if(prefix.isDefined) new Ordering[K] {
        override def compare(x: K, y: K): Int = {

          val r = prefixOrder.get.compare(prefix.get, y)

          if(r != 0) return r

          termOrd.compare(x, y)
        }
      } else new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          termOrd.compare(x, y)
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (prefix.isEmpty || prefixOrder.get.equiv(k, prefix.get)) && (inclusive && termOrd.gteq(k, term) || termOrd.gt(k, term))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          // From the first block with e >= term to the last block.
          return findPath(term)(gtOrd).map {
            case None =>
              cur = None
              Seq.empty[Tuple[K, V]]

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              //stop = filtered.isEmpty

              checkCounter(filtered.filter{case (k, v) => filter(k, v)})
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

  def printDatom(d: Datom): String = {
    d.getA match {
      case "users/:name" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]"
      case "users/:age" => s"[${d.a},${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()},${d.e},${d.t}]"
      case "users/:color" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]"
      case "users/:height" => s"[${d.a},${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()},${d.e},${d.t}]"
      case _ => ""
    }
  }

  protected def ltr(term: K, prefix:Option[K], inclusive: Boolean, prefixOrder: Option[Ordering[K]], termOrd: Ordering[K]): RichAsyncIterator[K, V] = {
    new RichAsyncIterator[K, V] {

      // Find the first prefix...
      val ltOrd = if(prefix.isDefined) new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          val r = prefixOrder.get.compare(prefix.get, y)

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

      protected def check(k: K): Boolean = {
        (prefix.isEmpty || prefixOrder.get.equiv(k, prefix.get)) && (inclusive && termOrd.lteq(k, term) || termOrd.lt(k, term))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(term)(ltOrd).map {
            case None =>
              cur = None
              Seq.empty[Tuple[K, V]]

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }.reverse
              //stop = filtered.isEmpty

              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => printDatom(k.asInstanceOf[Datom])}} filtered: ${filtered.length}${Console.RESET}\n")

              checkCounter(filtered.filter{case (k, v) => filter(k, v)})
          }
        }

        $this.prev(cur.map(_.unique_id))(order).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, v) => check(k)}.reverse
            stop = filtered.isEmpty

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  protected def lt(term: K, prefix:Option[K], inclusive: Boolean, prefixOrder: Option[Ordering[K]], termOrd: Ordering[K], reverse: Boolean): RichAsyncIterator[K, V] = {

    if(reverse){
      return ltr(term, prefix, inclusive, prefixOrder, termOrd)
    }

    new RichAsyncIterator[K, V] {

      // Find the first prefix and iterate from there...
      val ltOrd = if(prefix.isDefined) new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          val r = prefixOrder.get.compare(prefix.get, y)

          if(r != 0) return r

          -1
        }
      }
      // Iterate from the first block...
      else new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          -1
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      protected def check(k: K): Boolean = {
        (prefix.isEmpty || prefixOrder.get.equiv(k, prefix.get)) && (inclusive && termOrd.lteq(k, term) || termOrd.lt(k, term))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(term)(ltOrd).map {
            case None =>
              cur = None
              Seq.empty[Tuple[K, V]]

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              //stop = filtered.isEmpty

              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => printDatom(k.asInstanceOf[Datom])}} filtered: ${filtered.length}${Console.RESET}\n")

              checkCounter(filtered.filter{case (k, v) => filter(k, v)})
          }
        }

        $this.next(cur.map(_.unique_id))(order).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, v) => check(k)}
            stop = filtered.isEmpty

            //println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => printDatom(k.asInstanceOf[Datom])}} filtered: ${filtered.length}${Console.RESET}\n")

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  def intervalr(lowerTerm: K, upperTerm: K, lowerPrefix: Option[K], upperPrefix: Option[K],
               inclusiveLower: Boolean, inclusiveUpper: Boolean, lowerPrefixOrder: Option[Ordering[K]], upperPrefixOrder: Option[Ordering[K]],
               lowerOrder: Ordering[K], upperOrder: Ordering[K]): RichAsyncIterator[K, V] = {
    new RichAsyncIterator[K, V] {

      // Find the last prefix...
      val upperGtOrd = if(upperPrefix.isDefined) new Ordering[K] {
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

          // From the first block with e >= term to the last block.
          return findPath(lowerTerm)(upperGtOrd).map {
            case None =>
              cur = None
              Seq.empty[Tuple[K, V]]

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }.reverse
              //stop = filtered.isEmpty

              println(s"${printDatom(lowerTerm.asInstanceOf[Datom])} - ${printDatom(upperTerm.asInstanceOf[Datom])}")
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => printDatom(k.asInstanceOf[Datom])}} filtered: ${filtered.length}${Console.RESET}\n")

              checkCounter(filtered.filter{case (k, v) => filter(k, v)})
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

  def interval(lowerTerm: K, upperTerm: K, lowerPrefix: Option[K], upperPrefix: Option[K],
                         inclusiveLower: Boolean, inclusiveUpper: Boolean, lowerPrefixOrder: Option[Ordering[K]], upperPrefixOrder: Option[Ordering[K]],
                         lowerOrder: Ordering[K], upperOrder: Ordering[K], reverse: Boolean): RichAsyncIterator[K, V] = {

    if(reverse){
      return intervalr(lowerTerm, upperTerm, lowerPrefix, upperPrefix, inclusiveLower, inclusiveUpper, lowerPrefixOrder,
        upperPrefixOrder, lowerOrder, upperOrder)
    }

    new RichAsyncIterator[K, V] {

      // Find the first prefix...
      val lowerGtOrd = if(lowerPrefix.isDefined) new Ordering[K] {
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

          // From the first block with e >= term to the last block.
          return findPath(lowerTerm)(lowerGtOrd).map {
            case None =>
              cur = None
              Seq.empty[Tuple[K, V]]

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              //stop = filtered.isEmpty

              checkCounter(filtered.filter{case (k, v) => filter(k, v)})
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

  def lt(prefix: K, term: K, inclusive: Boolean, reverse: Boolean)(implicit prefixOrder: Ordering[K], termOrd: Ordering[K]): RichAsyncIterator[K, V] = {
    lt(term, Some(prefix), inclusive, Some(prefixOrder), termOrd, reverse)
  }

  def lt(term: K, inclusive: Boolean, reverse: Boolean)(implicit termOrd: Ordering[K]): RichAsyncIterator[K, V] = {
    lt(term, None, inclusive, None, termOrd, reverse)
  }

  def gt(prefix: K, term: K, inclusive: Boolean, reverse: Boolean)(implicit prefixOrder: Ordering[K], termOrd: Ordering[K]): RichAsyncIterator[K, V] = {
    gt(term, Some(prefix), inclusive, Some(prefixOrder), termOrd, reverse)
  }

  def gt(term: K, inclusive: Boolean, reverse: Boolean)(implicit termOrd: Ordering[K]): RichAsyncIterator[K, V] = {
    gt(term, None, inclusive, None, termOrd, reverse)
  }

}
