package services.scalable.index

import services.scalable.index.grpc.{IndexContext, RootRef}

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * All 'term' parameters in the functions should be provided along the prefix.
 * If the prefix is important in the searching, it should be explicitly provided with the optional parameter, i.e.,
 * fromPrefix.
 * order[T].compare(k, term): the first parameter of the compare function is the key being compared. The second one is
 * the pattern to be compared to.
 */
class QueryableIndex[K, V](val c: IndexContext)(override implicit val ec: ExecutionContext,
                                                  override val storage: Storage,
                                                  override val serializer: Serializer[Block[K, V]],
                                                  override val cache: Cache,
                                                  override val ord: Ordering[K],
                                                  override val idGenerator: IdGenerator)
  extends Index[K, V](c)(ec, storage, serializer, cache, ord, idGenerator) {

  override val $this = this

  protected def ltr(fromPrefix: Option[K], fromWord: K, inclusiveFrom: Boolean,
                    prefixOrd: Option[Ordering[K]], order: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

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
                Future.successful(checkCounter(filtered.filter{case (k, v, _) => filter(k, v)}))
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
            checkCounter(filtered.filter{case (k, v, _) => filter(k, v) })
        }
      }
    }
  }

  def lt(fromPrefix: Option[K], fromWord: K, inclusiveFrom: Boolean, reverse: Boolean)
        (prefixOrd: Option[Ordering[K]], order: Ordering[K]): RichAsyncIterator[K, V] = {

    if(reverse){
      return ltr(fromPrefix, fromWord, inclusiveFrom, prefixOrd, order)
    }

    new RichAsyncIterator[K, V] {

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

              /*if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
              /*if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
              }*/

              Future.successful(checkCounter(filtered.filter{case (k, v, _) => filter(k, v)}))
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

            /* if(fromWord.isInstanceOf[Datom])
               println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
             else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
 */
            checkCounter(filtered.filter{case (k, v, _) => filter(k, v) })
        }
      }
    }
  }

  protected def gtr(fromWord: K, inclusiveFrom: Boolean, fromPrefix: Option[K],
                    prefixOrd: Option[Ordering[K]], order: Ordering[K]): RichAsyncIterator[K, V] = {

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

              /*if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter{case (k, v, _) => filter(k, v)}))
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
            checkCounter(filtered.filter{case (k, v, _) => filter(k, v) })
        }
      }
    }
  }

  def gt(fromPrefix: Option[K], fromWord: K, inclusiveFrom: Boolean, reverse: Boolean)
        (prefixOrd: Option[Ordering[K]], order: Ordering[K]): RichAsyncIterator[K, V] = {

    if(reverse){
      return gtr(fromWord, inclusiveFrom, fromPrefix, prefixOrd, order)
    }

    new RichAsyncIterator[K, V] {

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

              Future.successful(checkCounter(filtered.filter{case (k, v, _) => filter(k, v)}))
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
            checkCounter(filtered.filter{case (k, v, _) => filter(k, v) })
        }
      }
    }
  }

  protected def ranger(fromWord: K, toWord: K, inclusiveFrom: Boolean, inclusiveTo: Boolean, order: Ordering[K]): RichAsyncIterator[K, V] = {

    new RichAsyncIterator[K, V] {

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
                Future.successful(checkCounter(filtered.filter{case (k, v, _) => filter(k, v)}))
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
            checkCounter(filtered.filter{case (k, v, _) => filter(k, v) })
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
                Future.successful(checkCounter(filtered.filter{case (k, v, _) => filter(k, v)}))
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
            checkCounter(filtered.filter{case (k, v, _) => filter(k, v) })
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

              val filtered = b.tuples.filter{case (k, _, _) => check(k)}.reverse
              stop = filtered.isEmpty

              /*if(fromWord.isInstanceOf[Datom])
                println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => k.asInstanceOf[Datom]}.map(d => printDatom(d, d.getA))} filtered: ${filtered.length}${Console.RESET}\n")
              else println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k.asInstanceOf[Bytes])}} filtered: ${filtered.length}${Console.RESET}\n")
*/
              if(filtered.isEmpty){
                next()
              } else {
                Future.successful(checkCounter(filtered.filter{case (k, v, _) => filter(k, v)}))
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
            checkCounter(filtered.filter{case (k, v, _) => filter(k, v) })
        }
      }
    }
  }

  def find(word: K, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {

    if(reverse){
      return findr(word)(order)
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
                Future.successful(checkCounter(filtered.filter{case (k, v, _) => filter(k, v)}))
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
            checkCounter(filtered.filter{case (k, v, _) => filter(k, v) })
        }
      }
    }
  }

  def gt(term: K, inclusive: Boolean, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {
    gt(None, term, inclusive, reverse)(None, order)
  }

  def gt(prefix: K, term: K, inclusive: Boolean, reverse: Boolean)(prefixOrd: Ordering[K], order: Ordering[K]): RichAsyncIterator[K, V] = {
    gt(Some(prefix), term, inclusive, reverse)(Some(prefixOrd), order)
  }

  def lt(term: K, inclusive: Boolean, reverse: Boolean)(order: Ordering[K]): RichAsyncIterator[K, V] = {
    lt(None, term, inclusive, reverse)(None, order)
  }

  def lt(prefix: K, term: K, inclusive: Boolean, reverse: Boolean)(prefixOrd: Ordering[K], order: Ordering[K]): RichAsyncIterator[K, V] = {
    lt(Some(prefix), term, inclusive, reverse)(Some(prefixOrd), order)
  }

  def isFull(): Boolean = {
    ctx.num_elements >= c.maxNItems
  }

  def hasMinimum(): Boolean = {
    ctx.num_elements >= c.maxNItems/2
  }

  def split(): Future[QueryableIndex[K, V]] = {

    println(s"original index root: ", ctx.root)

    val originalRoot = ctx.root

    (for {
      leftRoot <- ctx.get(ctx.root.get).map(_.copy())
      rightRoot = leftRoot.split()

      leftMeta <- leftRoot match {
        case leaf: Leaf[K, V] => Future.successful(leaf)
        case meta: Meta[K, V] => if (meta.length == 1) ctx.get(meta.pointers(0)._2.unique_id) else
          Future.successful(leftRoot)
      }

      rightMeta <- rightRoot match {
        case leaf: Leaf[K, V] => Future.successful(leaf)
        case meta: Meta[K, V] => if (meta.length == 1) ctx.get(meta.pointers(0)._2.unique_id) else
          Future.successful(rightRoot)
      }

      leftIndexCtx = IndexContext(ctx.id)
        .withNumLeafItems(ctx.NUM_LEAF_ENTRIES)
        .withNumMetaItems(ctx.NUM_META_ENTRIES)
        .withNumElements(leftMeta.nSubtree)
        .withLevels(leftMeta.level)
        .withMaxNItems(c.maxNItems)
        .withRoot(RootRef(leftMeta.unique_id._1, leftMeta.unique_id._2))

      rightIndexCtx = IndexContext(UUID.randomUUID.toString)
        .withNumLeafItems(ctx.NUM_LEAF_ENTRIES)
        .withNumMetaItems(ctx.NUM_META_ENTRIES)
        .withNumElements(rightMeta.nSubtree)
        .withLevels(rightMeta.level)
        .withMaxNItems(c.maxNItems)
        .withRoot(RootRef(rightMeta.unique_id._1, rightMeta.unique_id._2))

    } yield {
      (leftIndexCtx, rightIndexCtx)
    }).flatMap { case (lctx, rctx) =>

      println(s"left root: ${lctx.root.get} right root: ${rctx.root.get}")

      // This fixes it .... IDKW...
      //Await.result(storage.save(ctx.getBlocks().map{case (id, block) => id -> serializer.serialize(block)}.toMap), Duration.Inf)

      val lr = lctx.root.get
      val rr = rctx.root.get

      val oldCtx = this.ctx

      var leftParents = Await.result(ctx.getMeta(lr.partition -> lr.id), Duration.Inf)
        .pointers.map(_._2).map(_.unique_id)

      if(lctx.root.isDefined){
        leftParents = leftParents :+ lctx.root.get.partition -> lctx.root.get.id
      }

      var rightParents = Await.result(ctx.getMeta(rr.partition -> rr.id), Duration.Inf)
        .pointers.map(_._2).map(_.unique_id)

      if (rctx.root.isDefined) {
        rightParents = rightParents :+ rctx.root.get.partition -> rctx.root.get.id
      }

      var refs = Seq.empty[(String, String)]

      this.ctx.blockReferences.foreach { id =>
        refs :+= id
      }

      def isParentOf(id: (String, String), parents: Seq[(String, String)]): Boolean = {

        if(parents.exists(p => id == p)) return true

        val par = oldCtx.getParent(id)

        if(par.isEmpty) return false

        if(par.get._1.isEmpty) return false

        isParentOf(par.get._1.get, parents)
      }

      this.ctx = Context.fromIndexContext[K, V](lctx)(this.ec, this.storage, this.serializer,
        this.cache, this.ord, this.idGenerator)

      /*refs.foreach { id =>
        this.ctx.blockReferences :+= id
      }*/

      val rightIndex = new QueryableIndex[K, V](rctx)(this.ec, this.storage, this.serializer,
        this.cache, this.ord, this.idGenerator)

      //refs = refs.filterNot{x => !oldCtx.parents.isDefinedAt(x)}

      refs = refs.filter { id =>
        val isLeft = isParentOf(id, leftParents)
        val isRight = isParentOf(id, rightParents)

        // Removing orphan blocks...
        if(isLeft || isRight){
          println("id is from parent ", id, " left: ", isLeft, "right: ", isRight,
            oldCtx.parents.get(id), this.ctx.parents.get(id), rightIndex.ctx.parents.get(id))

          assert((isLeft && !isRight) || (isRight && !isLeft))

          if (isLeft) {
            this.ctx.blockReferences :+= id
          } else {
            rightIndex.ctx.blockReferences :+= id
          }

          true
        } else {
          false
        }
      }

      if(this.ctx.root.isDefined){
        this.ctx.blockReferences :+= this.ctx.root.get
      }

      if(rightIndex.ctx.root.isDefined){
        rightIndex.ctx.blockReferences :+= rightIndex.ctx.root.get
      }

      Future.successful(rightIndex)
    }
  }

  def copy(): QueryableIndex[K, V] = {

    val copy = new QueryableIndex[K, V](snapshot().withId(UUID.randomUUID.toString))(this.ec, this.storage, this.serializer,
      this.cache, this.ord, this.idGenerator)

    if(!ctx.blockReferences.isEmpty){
      ctx.blockReferences.foreach { id =>
        copy.ctx.blockReferences :+= id
      }
    }

    copy
  }

}
