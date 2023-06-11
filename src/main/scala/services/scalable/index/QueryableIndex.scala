package services.scalable.index

import services.scalable.index.grpc.{IndexContext, RootRef}
import services.scalable.index.impl.RichAsyncIndexIterator

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/**
 * All 'term' parameters in the functions should be provided along the prefix.
 * If the prefix is important in the searching, it should be explicitly provided with the optional parameter, i.e.,
 * fromPrefix.
 * order[T].compare(k, term): the first parameter of the compare function is the key being compared. The second one is
 * the pattern to be compared to.
 */
class QueryableIndex[K, V](override val descriptor: IndexContext)(override val builder: IndexBuilder[K, V])
  extends Index[K, V](descriptor)(builder) {

  override val $this = this

  import builder._

  protected def ltr(fromPrefix: Option[K], fromWord: K, inclusiveFrom: Boolean,
                    prefixOrd: Option[Ordering[K]], order: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V] {

      val sord = if(inclusiveFrom) new Ordering[K]{
        override def compare(term: K, y: K): Int = {
          val r = order.compare(term, y)

          if(r != 0) return r

          -1
        }
      } else
        new Ordering[K]{
          override def compare(term: K, y: K): Int = {
            val r = order.compare(term, y)

            if(r != 0) return r

            1
          }
        }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (fromPrefix.isEmpty || prefixOrd.get.equiv(k, fromPrefix.get)) && (inclusiveFrom && order.lteq(k, fromWord) || !inclusiveFrom && order.lt(k, fromWord))
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

              val filtered = b.tuples.filter{case (k, _, _) => check(k) }.reverse
              stop = filtered.isEmpty

              /*if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter(filter)))
              }
          }
        }

        $this.prev(cur.map(_.unique_id))(sord).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _, _) => check(k) }.reverse
            stop = filtered.isEmpty

            checkCounter(filtered.filter(filter))
        }
      }
    }
  }

  def lt(fromPrefix: Option[K], fromWord: K, inclusiveFrom: Boolean, reverse: Boolean)
        (prefixOrd: Option[Ordering[K]], order: Ordering[K]): RichAsyncIndexIterator[K, V] = {

    if(reverse){
      return ltr(fromPrefix, fromWord, inclusiveFrom, prefixOrd, order)
    }

    new RichAsyncIndexIterator[K, V] {

      val sord: Ordering[K] = if(fromPrefix.isDefined){
        new Ordering[K]{
          override def compare(term: K, y: K): Int = {
            val r = -prefixOrd.get.compare(y, fromPrefix.get)

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
        (fromPrefix.isEmpty || prefixOrd.get.equiv(k, fromPrefix.get)) && (inclusiveFrom && order.lteq(k, fromWord) || !inclusiveFrom && order.lt(k, fromWord))
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

              val filtered = b.tuples.filter{case (k, _, _) => check(k) }
              stop = filtered.isEmpty

              Future.successful(checkCounter(filtered.filter(filter)))
          }
        }

        $this.next(cur.map(_.unique_id))(sord).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _, _) => check(k) }
            stop = filtered.isEmpty

            checkCounter(filtered.filter(filter))
        }
      }
    }
  }

  protected def gtr(fromWord: K, inclusiveFrom: Boolean, fromPrefix: Option[K],
                    prefixOrd: Option[Ordering[K]], order: Ordering[K]): RichAsyncIndexIterator[K, V] = {

    new RichAsyncIndexIterator[K, V] {

      val sord: Ordering[K] = if(fromPrefix.isEmpty){
        new Ordering[K]{
          override def compare(x: K, y: K): Int = {
            1
          }
        }
      } else {
        new Ordering[K]{
          override def compare(x: K, y: K): Int = {
            val r = -prefixOrd.get.compare(y, fromPrefix.get)

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
        (fromPrefix.isEmpty || prefixOrd.get.equiv(k, fromPrefix.get)) && (inclusiveFrom && order.gteq(k, fromWord) || !inclusiveFrom && order.gt(k, fromWord))
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

              val filtered = b.tuples.filter{case (k, _, _) => check(k) }.reverse
              stop = filtered.isEmpty

              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter(filter)))
              }
          }
        }

        $this.prev(cur.map(_.unique_id))(sord).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _, _) => check(k) }.reverse
            stop = filtered.isEmpty

            checkCounter(filtered.filter(filter))
        }
      }
    }
  }

  def gt(fromPrefix: Option[K], fromWord: K, inclusiveFrom: Boolean, reverse: Boolean)
        (prefixOrd: Option[Ordering[K]], order: Ordering[K]): RichAsyncIndexIterator[K, V] = {

    if(reverse){
      return gtr(fromWord, inclusiveFrom, fromPrefix, prefixOrd, order)
    }

    new RichAsyncIndexIterator[K, V] {

      val sord = if(inclusiveFrom) new Ordering[K]{
        override def compare(term: K, y: K): Int = {
          val r = order.compare(term, y)

          if(r != 0) return r

          -1
        }
      } else
        new Ordering[K]{
        override def compare(term: K, y: K): Int = {
          val r = order.compare(term, y)

          if(r != 0) return r

          1
        }
      }

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

          return findPath(fromWord)(sord).flatMap {
            case None =>
              cur = None
              Future.successful(Seq.empty[Tuple[K, V]])

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _, _) => check(k) }
              stop = filtered.isEmpty

              /*if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
              }*/

              Future.successful(checkCounter(filtered.filter(filter)))
          }
        }

        $this.next(cur.map(_.unique_id))(sord).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _, _) => check(k) }
            stop = filtered.isEmpty

            /*if(fromWord.isInstanceOf[Datom])
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
            checkCounter(filtered.filter(filter))
        }
      }
    }
  }

  protected def ranger(fromWord: K, toWord: K, inclusiveFrom: Boolean, inclusiveTo: Boolean, order: Ordering[K]): RichAsyncIndexIterator[K, V] = {

    new RichAsyncIndexIterator[K, V] {

      val sord = if(inclusiveFrom) new Ordering[K]{
        override def compare(term: K, y: K): Int = {
          val r = order.compare(term, y)

          if(r != 0) return r

          -1
        }
      } else
        new Ordering[K]{
          override def compare(term: K, y: K): Int = {
            val r = order.compare(term, y)

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

          return findPath(toWord)(sord).flatMap {
            case None =>
              cur = None
              Future.successful(Seq.empty[Tuple[K, V]])

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _, _) => check(k) }.reverse
              stop = filtered.isEmpty

              /*if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter(filter)))
              }
          }
        }

        $this.prev(cur.map(_.unique_id))(sord).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _, _) => check(k) }.reverse
            stop = filtered.isEmpty

            /*if(fromWord.isInstanceOf[Datom])
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
            checkCounter(filtered.filter(filter))
        }
      }
    }
  }

  def range(fromWord: K, toWord: K, inclusiveFrom: Boolean, inclusiveTo: Boolean, reverse: Boolean)(implicit order: Ordering[K]): RichAsyncIndexIterator[K, V] = {

    if(reverse){
      return ranger(fromWord, toWord, inclusiveFrom, inclusiveTo, order)
    }

    new RichAsyncIndexIterator[K, V] {

      val sord = if(inclusiveFrom) new Ordering[K]{
        override def compare(term: K, y: K): Int = {
          val r = order.compare(term, y)

          if(r != 0) return r

          -1
        }
      } else
        new Ordering[K]{
          override def compare(term: K, y: K): Int = {
            val r = order.compare(term, y)

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

              val filtered = b.tuples.filter{case (k, _, _) => check(k) }
              stop = filtered.isEmpty

              /*if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter(filter)))
              }
          }
        }

        $this.next(cur.map(_.unique_id))(sord).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _, _) => check(k) }
            stop = filtered.isEmpty

            /*if(fromWord.isInstanceOf[Datom])
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
            checkCounter(filtered.filter(filter))
        }
      }
    }
  }

  protected def findr(word: K)(implicit order: Ordering[K]): RichAsyncIndexIterator[K, V] = {

    val sord = new Ordering[K]{
      override def compare(word: K, y: K): Int = {
        val r = -order.compare(y, word)

        if(r != 0) return r

        1
      }
    }

    new RichAsyncIndexIterator[K, V] {

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

              val filtered = b.tuples.filter{case (k, _, _) => check(k)}.reverse
              stop = filtered.isEmpty

              /*if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter(filter)))
              }
          }
        }

        $this.prev(cur.map(_.unique_id))(sord).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _, _) => check(k) }.reverse
            stop = filtered.isEmpty

            /*if(fromWord.isInstanceOf[Datom])
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
            checkCounter(filtered.filter(filter))
        }
      }
    }
  }

  def find(word: K, reverse: Boolean)(implicit order: Ordering[K]): RichAsyncIndexIterator[K, V] = {

    if(reverse){
      return findr(word)(order)
    }

    new RichAsyncIndexIterator[K, V] {
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

          return findPath(word)(order).flatMap {
            case None =>
              cur = None
              Future.successful(Seq.empty[Tuple[K, V]])

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _, _) => check(k) }
              stop = filtered.isEmpty

              /*if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter(filter)))
              }
          }
        }

        $this.next(cur.map(_.unique_id))(order).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _, _) => check(k) }
            stop = filtered.isEmpty

            /*if(fromWord.isInstanceOf[Datom])
              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
            else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
            checkCounter(filtered.filter(filter))
        }
      }
    }
  }

  def gt(term: K, inclusive: Boolean, reverse: Boolean)(implicit order: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    gt(None, term, inclusive, reverse)(None, order)
  }

  def gt(prefix: K, term: K, inclusive: Boolean, reverse: Boolean)(prefixOrd: Ordering[K], order: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    gt(Some(prefix), term, inclusive, reverse)(Some(prefixOrd), order)
  }

  def lt(term: K, inclusive: Boolean, reverse: Boolean)(implicit order: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    lt(None, term, inclusive, reverse)(None, order)
  }

  def lt(prefix: K, term: K, inclusive: Boolean, reverse: Boolean)(prefixOrd: Ordering[K], order: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    lt(Some(prefix), term, inclusive, reverse)(Some(prefixOrd), order)
  }

  def isFull(): Boolean = {
    ctx.num_elements >= descriptor.maxNItems
  }

  def hasMinimum(): Boolean = {
    ctx.num_elements >= descriptor.maxNItems/2
  }

  /*def split(): Future[QueryableIndex[K, V]] = {

    for {
      leftR <- tmpCtx.getMeta(tmpCtx.root.get).flatMap {
        case block if block.length == 1 => tmpCtx.getMeta(block.pointers(0)._2.unique_id)
        case block => Future.successful(block)
      }
    } yield {

      val leftN = leftR.pointers.slice(0, leftR.length / 2).map { case (_, ptr) =>
        ptr.nElements
      }.sum

      val rightN = leftR.pointers.slice(leftR.length / 2, leftR.length).map { case (_, ptr) =>
        ptr.nElements
      }.sum

      val leftICtx = descriptor
        .withId(tmpCtx.id)
        .withMaxNItems(descriptor.maxNItems)
        .withNumElements(leftN)
        .withLevels(leftR.level)
        .withNumLeafItems(descriptor.numLeafItems)
        .withNumMetaItems(descriptor.numMetaItems)

      val rightICtx = descriptor
        .withId(UUID.randomUUID().toString)
        .withMaxNItems(descriptor.maxNItems)
        .withNumElements(rightN)
        .withLevels(leftR.level)
        .withNumLeafItems(descriptor.numLeafItems)
        .withNumMetaItems(descriptor.numMetaItems)

      val refs = tmpCtx.blockReferences

      tmpCtx = Context.fromIndexContext(leftICtx)(this.builder)

      tmpCtx.blockReferences ++= refs

      val rindex = new QueryableIndex[K, V](rightICtx)(this.builder)

      val leftRoot = leftR.copy()(tmpCtx)
      tmpCtx.root = Some(leftRoot.unique_id)

      val rightRoot = leftRoot.split()(rindex.tmpCtx)
      rindex.tmpCtx.root = Some(rightRoot.unique_id)

      /*val ileft = Await.result(TestHelper.all(left.inOrder()), Duration.Inf).map { case (k, v, _) => k -> v }
        logger.debug(s"${Console.BLUE_B}idata data: ${ileft.map { case (k, v) => new String(k, Charsets.UTF_8) -> new String(v) }}${Console.RESET}\n")

        val idataL = Await.result(TestHelper.all(lindex.inOrder()), Duration.Inf).map { case (k, v, _) => k -> v }
        logger.debug(s"${Console.GREEN_B}idataL data: ${idataL.map { case (k, v) => new String(k, Charsets.UTF_8) -> new String(v) }}${Console.RESET}\n")

        val idataR = Await.result(TestHelper.all(rindex.inOrder()), Duration.Inf).map { case (k, v, _) => k -> v }
        logger.debug(s"${Console.GREEN_B}idataR data: ${idataR.map { case (k, v) => new String(k, Charsets.UTF_8) -> new String(v) }}${Console.RESET}\n")

        assert(idataR.slice(0, idataL.length) != idataL)
        assert(ileft == (idataL ++ idataR))*/

      rindex
    }
  }

  def copy(): QueryableIndex[K, V] = {
    val context = IndexContext(UUID.randomUUID.toString, descriptor.numLeafItems, descriptor.numMetaItems,
      tmpCtx.root.map { r => RootRef(r._1, r._2) }, levels, tmpCtx.num_elements, descriptor.maxNItems)

    val copy = new QueryableIndex[K, V](context)(this.builder)

    tmpCtx.blockReferences.foreach { case (id, _) =>
      copy.tmpCtx.blockReferences += id -> id
    }

    copy
  }*/

}
