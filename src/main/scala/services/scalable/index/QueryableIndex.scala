package services.scalable.index

import services.scalable.index.grpc.Datom

import scala.concurrent.{ExecutionContext, Future}

class QueryableIndex[K, V]()(override implicit val ec: ExecutionContext, override val ctx: Context[K, V],
                             implicit val order: Ordering[K]) extends Index[K, V]()(ec, ctx) {

  override val $this = this

  def printDatom(d: Datom): String = {
    d.getA match {
      case "users/:name" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]"
      case "users/:age" => s"[${d.a},${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()},${d.e},${d.t}]"
      case "users/:color" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]"
      case "users/:height" => s"[${d.a},${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()},${d.e},${d.t}]"
      case _ => ""
    }
  }

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

              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => printDatom(k.asInstanceOf[Datom])}} filtered: ${filtered.length}${Console.RESET}\n")

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

              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => printDatom(k.asInstanceOf[Datom])}} filtered: ${filtered.length}${Console.RESET}\n")

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

              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => printDatom(k.asInstanceOf[Datom])}} filtered: ${filtered.length}${Console.RESET}\n")

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

}
