package services.scalable.index

import services.scalable.index.grpc.Datom

import scala.concurrent.{ExecutionContext, Future}

class QueryableIndex[K, V]()(override implicit val ec: ExecutionContext, override val ctx: Context[K, V],
                           implicit val order: Ordering[K]) extends Index()(ec, ctx) {

  override val $this = this

  def gt(term: K, inclusive: Boolean = false)(implicit ord: Ordering[K]): RichAsyncIterator[K, V] = {
    new RichAsyncIterator[K, V] {

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (inclusive && ord.gteq(k, term)) || ord.gt(k, term)
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          // From the first block with e >= term to the last block.
          return findPath(term)(ord).map {
            case None =>
              cur = None
              Seq.empty[Tuple[K, V]]

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              stop = filtered.isEmpty

              checkCounter(filtered.filter{case (k, v) => filter(k, v)})
          }
        }

        $this.next(cur.map(_.unique_id))(ord).map {
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

  def lt(prefix: K, term: K, inclusive: Boolean = false)(implicit prefixOrd: Ordering[K], termOrd: Ordering[K]): RichAsyncIterator[K, V] = {
    new RichAsyncIterator[K, V] {

      val sOrd = new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          val r = prefixOrd.compare(x, prefix)

          if(r != 0) return r

          -1
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        prefixOrd.equiv(k, prefix) && ((inclusive && termOrd.lteq(k, term)) || termOrd.lt(k, term))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          // From the first block with e >= term to the last element >= term.
          return findPath(term)(sOrd).map {
            case None =>
              cur = None
              Seq.empty[Tuple[K, V]]

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              stop = filtered.isEmpty

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

            val filtered = b.tuples.filter{case (k, _) => check(k) }
            stop = filtered.isEmpty

            println(s"${Console.GREEN_B}${filtered.map{case (k, _) => printDatom(k.asInstanceOf[Datom])}}${Console.RESET}\n")

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

}
