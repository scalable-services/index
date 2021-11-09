package services.scalable.index

import scala.concurrent.{ExecutionContext, Future}

class QueryableIndex()(override implicit val ec: ExecutionContext, override val ctx: Context[Bytes, Bytes],
                           implicit val order: Ordering[Bytes]) extends Index()(ec, ctx) {

  override val $this = this

  type K = Bytes
  type V = Bytes

  protected def ltr(prefix: Option[K], term: K, inclusive: Boolean): RichAsyncIterator[K, V] = {
    new RichAsyncIterator[K, V] {

      val word = if(prefix.isDefined) prefix.get ++ term else term

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        if(prefix.isEmpty || k.length < prefix.get.length){
          return (inclusive && order.lteq(k, word)) || order.lt(k, word)
        }

        val p = k.slice(0, prefix.get.length)
        val t = k.slice(prefix.get.length, k.length)

        order.equiv(p, prefix.get) && (inclusive && order.lteq(t, term) || order.lt(t, term))
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

              println(s"${Console.RED_B}${b.tuples.map{case (k, _) => new String(k)}} filtered: ${filtered.length}${Console.RESET}\n")

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

  def lt(prefix: Option[K], term: Bytes, inclusive: Boolean, reverse: Boolean): RichAsyncIterator[K, V] = {

    val word = if(prefix.isDefined) prefix.get ++ term else term

    if(reverse){
      return ltr(prefix, term, inclusive)
    }

    val sord = if(prefix.isEmpty) new Ordering[K] {
      override def compare(x: K, y: K): Int = -1
    } else new Ordering[K] {
      override def compare(x: K, y: K): Int = {
        order.compare(prefix.get, y)
      }
    }

    new RichAsyncIterator[K, V] {

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        if(prefix.isEmpty || k.length < prefix.get.length){
          return (inclusive && order.lteq(k, word)) || order.lt(k, word)
        }

        val p = k.slice(0, prefix.get.length)
        val t = k.slice(prefix.get.length, k.length)

        order.equiv(p, prefix.get) && (inclusive && order.lteq(t, term) || order.lt(t, term))
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
              //stop = filtered.isEmpty

              println(s"${Console.RED_B}${b.tuples.map{case (k, _) => new String(k)}} filtered: ${filtered.length}${Console.RESET}\n")

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

  protected def gtr(prefix: Option[K], term: K, inclusive: Boolean): RichAsyncIterator[K, V] = {

    val word = if(prefix.isDefined) prefix.get ++ term else term

    new RichAsyncIterator[K, V] {

      val sord = if(prefix.isEmpty) new Ordering[K] {
        override def compare(x: K, y: K): Int = 1
      } else new Ordering[K] {
        override def compare(x: K, y: K): Int = {
          val r = order.compare(prefix.get, y.slice(0, prefix.get.length))

          if(r != 0) return r

          1
        }
      }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        if(prefix.isEmpty || k.length < prefix.get.length){
          return (inclusive && order.gteq(k, word)) || order.gt(k, word)
        }

        val p = k.slice(0, prefix.get.length)
        val t = k.slice(prefix.get.length, k.length)

        order.equiv(p, prefix.get) && (inclusive && order.gteq(t, term) || order.gt(t, term))
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

              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k)}} filtered: ${filtered.length}${Console.RESET}\n")

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

            println(s"${Console.RED_B}${b.tuples.map{case (k, _) => new String(k)}} filtered: ${filtered.length}${Console.RESET}\n")

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  def gt(prefix: Option[K], term: K, inclusive: Boolean, reverse: Boolean): RichAsyncIterator[K, V] = {

    if(reverse){
      return gtr(prefix, term, inclusive)
    }

    val word = if(prefix.isDefined) prefix.get ++ term else term

    new RichAsyncIterator[K, V] {

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        if(prefix.isEmpty || k.length < prefix.get.length){
          return (inclusive && order.gteq(k, word)) || order.gt(k, word)
        }

        val p = k.slice(0, prefix.get.length)
        val t = k.slice(prefix.get.length, k.length)

        order.equiv(p, prefix.get) && (inclusive && order.gteq(t, term) || order.gt(t, term))
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

              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k)}} filtered: ${filtered.length}${Console.RESET}\n")

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

  protected def ranger(fromPrefix: Option[K], toPrefix: Option[K], from: K, to: K, includeFrom: Boolean, includeTo: Boolean): RichAsyncIterator[K, V] = {

    val fromWord = if(fromPrefix.isDefined) fromPrefix.get ++ from else from
    val toWord = if(toPrefix.isDefined) toPrefix.get ++ to else to

    val sord = if(toPrefix.isEmpty) new Ordering[K] {
      override def compare(x: K, y: K): Int = 1
    } else new Ordering[K] {
      override def compare(x: K, y: K): Int = {
        val r = order.compare(toPrefix.get, y.slice(0, toPrefix.get.length))

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
        ((includeFrom && order.gteq(k, fromWord) || order.gt(k, fromWord)) && (fromPrefix.isEmpty || order.gteq(k.slice(0, fromPrefix.get.length), fromPrefix.get))) &&
          ((includeTo && order.lteq(k, toWord) || order.lt(k, toWord)) && (toPrefix.isEmpty || order.lteq(k.slice(0, toPrefix.get.length), toPrefix.get)))
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

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              //stop = filtered.isEmpty

              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k)}} filtered: ${filtered.length}${Console.RESET}\n")

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

            val filtered = b.tuples.filter{case (k, _) => check(k) }
            stop = filtered.isEmpty

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }
    }
  }

  def range(fromPrefix: Option[K], toPrefix: Option[K], from: K, to: K, includeFrom: Boolean, includeTo: Boolean, reverse: Boolean): RichAsyncIterator[K, V] = {

    if(reverse){
      return ranger(fromPrefix, toPrefix, from, to, includeFrom, includeTo)
    }

    val fromWord = if(fromPrefix.isDefined) fromPrefix.get ++ from else from
    val toWord = if(toPrefix.isDefined) toPrefix.get ++ to else to

    new RichAsyncIterator[K, V] {

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        ((includeFrom && order.gteq(k, fromWord) || order.gt(k, fromWord)) && (fromPrefix.isEmpty || order.gteq(k.slice(0, fromPrefix.get.length), fromPrefix.get))) &&
          ((includeTo && order.lteq(k, toWord) || order.lt(k, toWord)) && (toPrefix.isEmpty || order.lteq(k.slice(0, toPrefix.get.length), toPrefix.get)))
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

              println(s"${Console.GREEN_B}${b.tuples.map{case (k, _) => new String(k)}} filtered: ${filtered.length}${Console.RESET}\n")

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
