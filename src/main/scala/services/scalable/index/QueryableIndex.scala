package services.scalable.index

import services.scalable.index.grpc.Datom

import scala.concurrent.{ExecutionContext, Future}

class QueryableIndex[K, V]()(override implicit val ec: ExecutionContext, override val ctx: Context[K, V],
                             implicit val order: Ordering[K]) extends Index[K, V]()(ec, ctx) {

  override val $this = this

  def gt(prefix: Option[K], term: K, inclusive: Boolean, reverse: Boolean)(prefixOrd: Option[Ordering[K]], termOrd: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

       val sord = if(prefix.isEmpty) new Ordering[K] {
         override def compare(x: K, y: K): Int = {
           val r = termOrd.compare(x, y)

           if(r != 0) return r

           if(inclusive) 0 else 1
         }
       } else new Ordering[K]{
         override def compare(x: K, y: K): Int = {
           val r = prefixOrd.get.compare(x, y)

           if(r != 0) return r

           termOrd.compare(x, y)
         }
       }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (prefix.isEmpty || prefixOrd.get.equiv(k, prefix.get)) && ((inclusive && termOrd.gteq(k, term)) || (!inclusive && termOrd.gt(k, term)))
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
