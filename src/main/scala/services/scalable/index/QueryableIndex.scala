package services.scalable.index

import services.scalable.index.grpc.Datom
import scala.concurrent.{ExecutionContext, Future}

/**
 * All 'term' parameters in the functions should be provided with the prefix.
 * If the prefix is important, it should be explicitly provided with the optional parameter, i.e.,
 * fromPrefix.
 */
class QueryableIndex[K, V]()(override implicit val ec: ExecutionContext, override val ctx: Context[K, V],
                             implicit val order: Ordering[K]) extends Index[K, V]()(ec, ctx) {

  override val $this = this

  protected def ltr(fromPrefix: Option[K], fromWord: K, inclusiveFrom: Boolean, order: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

      val sord: Ordering[K] = if(inclusiveFrom){
        new Ordering[K]{
          override def compare(x: K, y: K): Int = {
            val r = -order.compare(y, fromWord)

            if(r != 0) return r

            1
          }
        }
      } else {
        new Ordering[K]{
          override def compare(x: K, y: K): Int = {
            val r = -order.compare(y, fromWord)

            if(r != 0) return r

            //-1
            0
          }
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (fromPrefix.isEmpty || order.equiv(k, fromPrefix.get)) && (inclusiveFrom && order.lteq(k, fromWord) || !inclusiveFrom && order.lt(k, fromWord))
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
              stop = filtered.isEmpty

              /*if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
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

            /*if(fromWord.isInstanceOf[Datom])
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  def lt(fromPrefix: Option[K], fromWord: K, inclusiveFrom: Boolean, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {

    if(reverse){
      return ltr(fromPrefix, fromWord, inclusiveFrom, order)
    }

    new RichAsyncIterator[K, V] {

      val sord: Ordering[K] = if(fromPrefix.isDefined){
        new Ordering[K]{
          override def compare(term: K, y: K): Int = {
            val r = -order.compare(y, fromPrefix.get)

            if(r != 0) return r

            -1
          }
        }
      } else {
        new Ordering[K]{
          override def compare(term: K, y: K): Int = {
            -1
          }
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (fromPrefix.isEmpty || order.equiv(k, fromPrefix.get)) && (inclusiveFrom && order.lteq(k, fromWord) || !inclusiveFrom && order.lt(k, fromWord))
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

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              stop = filtered.isEmpty

              /*if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
              /*if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
              }*/

              Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
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

            /* if(fromWord.isInstanceOf[Datom])
               println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
             else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
 */
            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  protected def gtr(fromWord: K, inclusiveFrom: Boolean, fromPrefix: Option[K], order: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

      val sord: Ordering[K] = if(fromPrefix.isEmpty){
        new Ordering[K]{
          override def compare(x: K, y: K): Int = {
            1
          }
        }
      } else {
        new Ordering[K]{
          override def compare(x: K, y: K): Int = {
            val r = -order.compare(y, fromPrefix.get)

            if(r != 0) return r

            1
          }
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (fromPrefix.isEmpty || order.equiv(k, fromPrefix.get)) && (inclusiveFrom && order.gteq(k, fromWord) || !inclusiveFrom && order.gt(k, fromWord))
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
              stop = filtered.isEmpty

              /*if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
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

            /*if(fromWord.isInstanceOf[Datom])
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  def gt(fromPrefix: Option[K], fromWord: K, inclusiveFrom: Boolean, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {

    if(reverse){
      return gtr(fromWord, inclusiveFrom, fromPrefix, order)
    }

    new RichAsyncIterator[K, V] {

      val sord = if(inclusiveFrom) new Ordering[K]{
        override def compare(term: K, y: K): Int = {
          val r = -order.compare(y, term)

          if(r != 0) return r

          //-1
          0
        }
      } else
        new Ordering[K]{
        override def compare(term: K, y: K): Int = {
          val r = -order.compare(y, term)

          if(r != 0) return r

          1
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (fromPrefix.isEmpty || order.equiv(k, fromPrefix.get)) && (inclusiveFrom && order.gteq(k, fromWord) || !inclusiveFrom && order.gt(k, fromWord))
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

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              stop = filtered.isEmpty

              if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")

              /*if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
              }*/

              Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
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

            /*if(fromWord.isInstanceOf[Datom])
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  protected def ranger(fromWord: K, toWord: K, inclusiveFrom: Boolean, inclusiveTo: Boolean, order: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

      val sord: Ordering[K] = if(inclusiveTo){
        new Ordering[K]{
          override def compare(x: K, y: K): Int = {
            val r = -order.compare(y, toWord)

            if(r != 0) return r

            1
          }
        }
      } else new Ordering[K]{
        override def compare(x: K, y: K): Int = {
          val r = -order.compare(y, toWord)

          if(r != 0) return r

          //-1
          0
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (inclusiveFrom && order.gteq(k, fromWord) || !inclusiveFrom && order.gt(k, fromWord)) &&
          (inclusiveTo && order.lteq(k, toWord) || !inclusiveTo && order.lt(k, toWord))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(toWord)(sord).flatMap {
            case None =>
              cur = None
              Future.successful(Seq.empty[Tuple[K, V]])

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }.reverse
              stop = filtered.isEmpty

              /*if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
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

            /*if(fromWord.isInstanceOf[Datom])
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  def range(fromWord: K, toWord: K, inclusiveFrom: Boolean, inclusiveTo: Boolean, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {

    if(reverse){
      return ranger(fromWord, toWord, inclusiveFrom, inclusiveTo, order)
    }

    new RichAsyncIterator[K, V] {

      val sord = if(inclusiveFrom) new Ordering[K]{
        override def compare(term: K, y: K): Int = {
          val r = -order.compare(y, fromWord)

          if(r != 0) return r

          //-1
          0
        }
      } else
        new Ordering[K]{
          override def compare(term: K, y: K): Int = {
            val r = -order.compare(y, fromWord)

            if(r != 0) return r

            1
          }
        }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (inclusiveFrom && order.gteq(k, fromWord) || !inclusiveFrom && order.gt(k, fromWord)) &&
          (inclusiveTo && order.lteq(k, toWord) || !inclusiveTo && order.lt(k, toWord))
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

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              stop = filtered.isEmpty

              /*if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
              }
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

            /*if(fromWord.isInstanceOf[Datom])
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  protected def findr(word: K)(order: Ordering[K]): RichAsyncIterator[K, V] = {

    val sord = new Ordering[K]{
      override def compare(word: K, y: K): Int = {
        val r = -order.compare(y, word)

        if(r != 0) return r

        1
      }
    }

    new RichAsyncIterator[K, V] {

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        order.equiv(k, word)
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

              val filtered = b.tuples.filter{case (k, _) => check(k)}.reverse
              stop = filtered.isEmpty

              /*if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
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

            /*if(fromWord.isInstanceOf[Datom])
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  def find(word: K, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {

    if(reverse){
      return findr(word)(order)
    }

    val sord = order

    new RichAsyncIterator[K, V] {
      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        order.equiv(k, word)
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

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              stop = filtered.isEmpty

              /*if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
              }
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

            /*if(fromWord.isInstanceOf[Datom])
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  def lt(prefix: K, word: K, inclusive: Boolean, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {
    lt(Some(prefix), word, inclusive, reverse)(order)
  }

  def lt(word: K, inclusive: Boolean, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {
    lt(None, word, inclusive, reverse)(order)
  }

  def gt(prefix: K, word: K, inclusive: Boolean, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {
    gt(Some(prefix), word, inclusive, reverse)(order)
  }

  def gt(word: K, inclusive: Boolean, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {
    gt(None, word, inclusive, reverse)(order)
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
