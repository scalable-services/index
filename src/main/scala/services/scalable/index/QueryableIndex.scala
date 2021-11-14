package services.scalable.index

import services.scalable.index.grpc.Datom
import scala.concurrent.{ExecutionContext, Future}

class QueryableIndex[K, V]()(override implicit val ec: ExecutionContext, override val ctx: Context[K, V],
                             implicit val order: Ordering[K]) extends Index[K, V]()(ec, ctx) {

  override val $this = this

  def gt(prefix: K, term: K, inclusive: Boolean, reverse: Boolean)(prefixOrd: Ordering[K], order: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

      val sord = new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          var r = prefixOrd.compare(prefix, y)

          if(r != 0) return r

          r = order.compare(term, y)

          if(r != 0) return r

          if(inclusive) 0 else 1
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        prefixOrd.equiv(k, prefix) && (inclusive && order.gteq(k, term) || !inclusive && order.gt(k, term))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(term)(sord).map {
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

  def gt(term: K, inclusive: Boolean, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

      val sord = new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          val r = order.compare(x, y)

          if(r != 0) return r

          if(inclusive) 0 else 1
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        inclusive && order.gteq(k, term) || !inclusive && order.gt(k, term)
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(term)(sord).map {
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

  def lt(prefix: K, term: K, inclusive: Boolean, reverse: Boolean)(prefixOrd: Ordering[K], order: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

      val sord = new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          val r = prefixOrd.compare(prefix, y)

          if(r != 0) return r

          -1
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        prefixOrd.equiv(k, prefix) && (inclusive && order.lteq(k, term) || !inclusive && order.lt(k, term))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(term)(sord).map {
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

  def lt(term: K, inclusive: Boolean, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

      val sord = new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          -1
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        inclusive && order.lteq(k, term) || !inclusive && order.lt(k, term)
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(term)(sord).map {
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

  def range(fromPrefix: K, fromTerm: K, toPrefix: K, toTerm: K, inclusiveFrom: Boolean, inclusiveTo: Boolean, reverse: Boolean)(prefixOrd: Ordering[K], order: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

      val sord = new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          var r = prefixOrd.compare(fromPrefix, y)

          if(r != 0) return r

          r = order.compare(fromTerm, y)

          if(r != 0) return r

          if(inclusiveFrom) 0 else 1
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (prefixOrd.equiv(k, fromPrefix) && (inclusiveFrom && order.gteq(k, fromTerm) || !inclusiveFrom && order.gt(k, fromTerm))) &&
          (prefixOrd.equiv(k, toPrefix) && (inclusiveTo && order.lteq(k, toTerm) || !inclusiveTo && order.lt(k, toTerm)))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(fromTerm)(sord).map {
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

  def range(fromTerm: K, toTerm: K, inclusiveFrom: Boolean, inclusiveTo: Boolean, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

      val sord = new Ordering[K] {
        override def compare(x: K, y: K): Int = {
         val r = order.compare(fromTerm, y)

          if(r != 0) return r

          if(inclusiveFrom) 0 else 1
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (inclusiveFrom && order.gteq(k, fromTerm) || !inclusiveFrom && order.gt(k, fromTerm)) &&
          (inclusiveTo && order.lteq(k, toTerm) || !inclusiveTo && order.lt(k, toTerm))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(fromTerm)(sord).map {
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
