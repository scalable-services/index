package services.scalable.index

import services.scalable.index.grpc.Datom
import scala.concurrent.{ExecutionContext, Future}

/**
 * For all the methods provide the whole word with the prefix!!!
 */
class QueryableIndex[K, V]()(override implicit val ec: ExecutionContext, override val ctx: Context[K, V],
                             implicit val order: Ordering[K]) extends Index[K, V]()(ec, ctx) {

  override val $this = this

  protected def gtr(word: K, inclusive: Boolean, prefix: Option[K], prefixOrd: Option[Ordering[K]], order: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

      val sord = if(prefix.isDefined) new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          val r = -prefixOrd.get.compare(y, prefix.get)

          if(r != 0) return r

          1
        }
      } else new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          1
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && (inclusive && order.gteq(k, word) || !inclusive && order.gt(k, word))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(word)(sord).flatMap {
            case None =>
              cur = None
              Future.successful(Seq.empty[Tuple[K, V]])

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }.reverse
              //stop = filtered.isEmpty

              // println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")

              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
              }
          }
        }

        $this.prev(cur.map(_.unique_id))(sord).flatMap {
          case None =>
            cur = None
            Future.successful(Seq.empty[Tuple[K, V]])

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _) => check(k) }.reverse
            //stop = filtered.isEmpty

            //println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            println(s"${Console.BLUE_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")

            if(filtered.isEmpty){
              next()
            } else {
              Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v) }))
            }
        }
      }
    }
  }

  def gt(word: K, inclusive: Boolean, reverse: Boolean, prefix: Option[K], prefixOrd: Option[Ordering[K]], order: Ordering[K]): RichAsyncIterator[K, V] = {

    if(reverse){
      return gtr(word, inclusive, prefix, prefixOrd, order)
    }

    new RichAsyncIterator[K, V] {

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && (inclusive && order.gteq(k, word) || !inclusive && order.gt(k, word))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(word)(order).flatMap {
            case None =>
              cur = None
              Future.successful(Seq.empty[Tuple[K, V]])

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              //stop = filtered.isEmpty

              // println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")

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

            //println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            println(s"${Console.BLUE_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  protected def ltr(word: K, inclusive: Boolean, prefix: Option[K], prefixOrd: Option[Ordering[K]], order: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && (inclusive && order.lteq(k, word) || !inclusive && order.lt(k, word))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(word)(order).flatMap {
            case None =>
              cur = None
              Future.successful(Seq.empty[Tuple[K, V]])

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }.reverse
              //stop = filtered.isEmpty

              // println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")

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

            //println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            println(s"${Console.BLUE_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  def lt(word: K, inclusive: Boolean, reverse: Boolean, prefix: Option[K], prefixOrd: Option[Ordering[K]], order: Ordering[K]): RichAsyncIterator[K, V] = {

    if(reverse){
      return ltr(word, inclusive, prefix, prefixOrd, order)
    }

    new RichAsyncIterator[K, V] {

      val sord = if(prefix.isDefined) new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          val r = -prefixOrd.get.compare(y, prefix.get)

          if(r != 0) return r

          -1
        }
      } else new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          -1
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && (inclusive && order.lteq(k, word) || !inclusive && order.lt(k, word))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(word)(sord).map {
            case None =>
              cur = None
              Seq.empty[Tuple[K, V]]

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              stop = filtered.isEmpty

              // println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")

              checkCounter(filtered.filter{case (k, v) => filter(k, v)})
          }
        }

        $this.next(cur.map(_.unique_id))(sord).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _) => check(k) }
            stop = filtered.isEmpty

            //println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            println(s"${Console.BLUE_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  def ranger(fromWord: K, toWord: K, inclusiveFrom: Boolean, inclusiveTo: Boolean,
             fromPrefix: Option[K], toPrefix: Option[K], prefixOrd: Option[Ordering[K]], order: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

      val sord = if(toPrefix.isDefined) new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          order.compare(toWord, y)
        }
      } else new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          order.compare(toWord, y)
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        ((fromPrefix.isEmpty || prefixOrd.get.equiv(k, fromPrefix.get)) && (inclusiveFrom && order.gteq(k, fromWord) || !inclusiveFrom && order.gt(k, fromWord))) &&
          ((toPrefix.isEmpty || prefixOrd.get.equiv(k, toPrefix.get)) && (inclusiveTo && order.lteq(k, toWord) || !inclusiveTo && order.lt(k, toWord)))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(fromWord)(sord).flatMap {
            case None =>
              cur = None
              Future.successful(Seq.empty[Tuple[K, V]])

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }.reverse
              //stop = filtered.isEmpty

              // println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")

              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
              }
          }
        }

        $this.prev(cur.map(_.unique_id))(sord).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _) => check(k) }.reverse
            stop = filtered.isEmpty

            //println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            println(s"${Console.BLUE_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  def range(fromWord: K, toWord: K, inclusiveFrom: Boolean, inclusiveTo: Boolean, reverse: Boolean,
            fromPrefix: Option[K], toPrefix: Option[K], prefixOrd: Option[Ordering[K]], order: Ordering[K]): RichAsyncIterator[K, V] = {

    if(reverse){
      return ranger(fromWord, toWord, inclusiveFrom, inclusiveTo, fromPrefix, toPrefix, prefixOrd, order)
    }

    new RichAsyncIterator[K, V] {

      /*val sord = new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          order.compare(fromWord, y)
        }
      }*/

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        ((fromPrefix.isEmpty || prefixOrd.get.equiv(k, fromPrefix.get)) && (inclusiveFrom && order.gteq(k, fromWord) || !inclusiveFrom && order.gt(k, fromWord))) &&
          ((toPrefix.isEmpty || prefixOrd.get.equiv(k, toPrefix.get)) && (inclusiveTo && order.lteq(k, toWord) || !inclusiveTo && order.lt(k, toWord)))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(fromWord)(order).flatMap {
            case None =>
              cur = None
              Future.successful(Seq.empty[Tuple[K, V]])

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              //stop = filtered.isEmpty

              // println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")

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

            //println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            println(s"${Console.BLUE_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  def gt(word: K, inclusive: Boolean, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {
    gt(word, inclusive, reverse, None, None, order)
  }

  def gt(prefix: K, word: K, inclusive: Boolean, reverse: Boolean)(prefixOrd: Ordering[K], order: Ordering[K]): RichAsyncIterator[K, V] = {
    gt(word, inclusive, reverse, Some(prefix), Some(prefixOrd), order)
  }

  def lt(word: K, inclusive: Boolean, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {
    lt(word, inclusive, reverse, None, None, order)
  }

  def lt(prefix: K, word: K, inclusive: Boolean, reverse: Boolean)(prefixOrd: Ordering[K], order: Ordering[K]): RichAsyncIterator[K, V] = {
    lt(word, inclusive, reverse, Some(prefix), Some(prefixOrd), order)
  }

  def range(fromWord: K, toWord: K, inclusiveFrom: Boolean, inclusiveTo: Boolean, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {
    range(fromWord, toWord, inclusiveFrom, inclusiveTo, reverse, None, None, None, order)
  }

  def range(fromPrefix: K, fromWord: K, toPrefix: K, toWord: K, inclusiveFrom: Boolean, inclusiveTo: Boolean, reverse: Boolean)(prefixOrd: Ordering[K], order: Ordering[K]): RichAsyncIterator[K, V] = {
    range(fromWord, toWord, inclusiveFrom, inclusiveTo, reverse, Some(fromPrefix), Some(toPrefix), Some(prefixOrd), order)
  }

  def printDatom(d: Datom, p: String): String = {
    p match {
      case "users/:name" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]"
      case "users/:age" => s"[${d.a},${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()},${d.e},${d.t}]"
      case "users/:color" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]"
      case "users/:height" => s"[${d.a},${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()},${d.e},${d.t}]"
      //case "users/:height" => s"[${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()}]"
      case _ => ""
    }
  }

}
