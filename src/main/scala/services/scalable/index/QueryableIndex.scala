package services.scalable.index

import services.scalable.index.grpc.Datom
import scala.concurrent.{ExecutionContext, Future}

/**
 * For all the methods provide the whole word with the prefix!!!
 */
class QueryableIndex[K, V]()(override implicit val ec: ExecutionContext, override val ctx: Context[K, V],
                             implicit val order: Ordering[K]) extends Index[K, V]()(ec, ctx) {

  override val $this = this

  def findPathGt(k: K, start: Block[K,V], limit: Option[Block[K,V]], inclusive: Boolean)(implicit order: Ordering[K]): Future[Option[Leaf[K,V]]] = {

    if(limit.isDefined && limit.get.unique_id.equals(start.unique_id)){
      logger.debug(s"reached limit!")
      return Future.successful(None)
    }

    start match {
      case leaf: Leaf[K,V] => Future.successful(Some(leaf))
      case meta: Meta[K,V] =>

        meta.setPointers()(ctx)

        val bid = meta.findPathGt(k, inclusive)

        ctx.get(bid).flatMap { block =>
          findPathGt(k, block, limit, inclusive)
        }
    }
  }

  def findPathGt(k: K, inclusive: Boolean, limit: Option[Block[K,V]] = None)(implicit order: Ordering[K]): Future[Option[Leaf[K,V]]] = {
    if(ctx.root.isEmpty) {
      return Future.successful(None)
    }

    val bid = ctx.root.get

    ctx.get(ctx.root.get).flatMap { start =>
      ctx.setParent(bid, 0, None)
      findPathGt(k, start, limit, inclusive)
    }
  }

  def gt(fromWord: K, inclusiveFrom: Boolean, reverse: Boolean,
            fromPrefix: Option[K], prefixOrd: Option[Ordering[K]], order: Ordering[K])(sord: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

      /*val sord: Ordering[K] = if(fromPrefix.isDefined){
        if(inclusiveFrom){
          new Ordering[K] {
            override def compare(x: K, y: K): Int = {
              val r = -prefixOrd.get.compare(y, fromPrefix.get)

              if(r != 0) return r

              -order.compare(y, fromWord)
            }
          }
        } else {
          new Ordering[K] {
            override def compare(x: K, y: K): Int = {
              var r = -prefixOrd.get.compare(y, fromPrefix.get)

              if(r != 0) return r

              r = -order.compare(y, fromWord)

              if(r < 0) return r

              1
            }
          }
        }
      } else {
        if(inclusiveFrom) new Ordering[K] {
          override def compare(x: K, y: K): Int = {
            -order.compare(y, fromWord)
          }
        } else new Ordering[K] {

          // We want the first element that is greater than the element being searched. So we want the comparison to return 1
          override def compare(x: K, y: K): Int = {
            val r = order.compare(fromWord, y)

            if(r < 0) return r

            1
          }
        }
      }*/

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (fromPrefix.isEmpty || prefixOrd.get.equiv(k, fromPrefix.get)) && (inclusiveFrom && order.gteq(k, fromWord) || !inclusiveFrom && order.gt(k, fromWord))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(fromWord)(sord).map {
            case None =>
              cur = None
              Seq.empty[Tuple[K, V]]

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              stop = filtered.isEmpty

              if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")

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

            if(fromWord.isInstanceOf[Datom])
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")


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
