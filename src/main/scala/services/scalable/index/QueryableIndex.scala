package services.scalable.index

import services.scalable.index.grpc.{IndexContext, RootRef}
import services.scalable.index.impl.RichAsyncIndexIterator

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

/**
 * Any iterator can be constructed using a searching function and a filter function
 */
class QueryableIndex[K, V](override protected val descriptor: IndexContext)(override val builder: IndexBuilt[K, V])
  extends Index[K, V](descriptor)(builder) {

  override val $this = this

  import builder._

  def find(k: K): Future[Option[Leaf[K, V]]] = {
    findPath(k)
  }

  /**
   * Returns the block containing the preceding key for k
   * @param k
   * @param orEqual
   * @param termComp
   * @return
   */
  def previousKeyBlock(k: K, orEqual: Boolean, findPathFn: (K, Meta[K, V], Ordering[K]) => (String, String) = (k, m, ord) => m.findPath(k)(ord))(termComp: Ordering[K]): Future[Option[Leaf[K, V]]] = {
    findPath(k, None, findPathFn)(termComp).flatMap {
      case None => Future.successful(None)
      case Some(leaf) =>

        val found = leaf.tuples.indexWhere {case (k1, _, _) => (orEqual && termComp.lteq(k1, k)) || termComp.lt(k1, k)}

        logger.debug(s"${Console.GREEN_B}Arrived at leaf: ${leaf.print()} found: ${found}${Console.RESET}")

        if(found >= 0){
          Future.successful(Some(leaf))
        } else {
          prev(Some(leaf.unique_id))
        }
    }
  }

  /**
   * Returns the block containing the successor key for k
   * @param k
   * @param orEqual
   * @param termComp
   * @return
   */
  def nextKeyBlock(k: K, orEqual: Boolean, findPathFn: (K, Meta[K, V], Ordering[K]) => (String, String) = (k, m, ord) => m.findPath(k)(ord))(termComp: Ordering[K]): Future[Option[Leaf[K, V]]] = {
    findPath(k, None, findPathFn)(termComp).flatMap {
      case None => Future.successful(None)
      case Some(leaf) =>
        val found = leaf.tuples.indexWhere {case (k1, _, _) => (orEqual && termComp.gteq(k1, k)) || termComp.gt(k1, k)}

        logger.debug(s"${Console.CYAN_B}Arrived at leaf: ${leaf.print()}${Console.RESET}")

        if(found >= 0){
          Future.successful(Some(leaf))
        } else {
          next(Some(leaf.unique_id))
        }
    }
  }

  def previousKey(k: K, findPathFn: (K, Meta[K, V], Ordering[K]) => (String, String) = (k, m, ord) => m.findPath(k)(ord))(termComp: Ordering[K]): Future[Option[Tuple[K, V]]] = {
    previousKeyBlock(k, false, findPathFn)(termComp).map {
      case None => None
      case Some(leaf) => leaf.previousKey(k)(termComp, ctx)
    }
  }

  def nextKey(k: K, findPathFn: (K, Meta[K, V], Ordering[K]) => (String, String) = (k, m, ord) => m.findPath(k)(ord))(termComp: Ordering[K]): Future[Option[Tuple[K, V]]] = {
    nextKeyBlock(k, false, findPathFn)(termComp).map {
      case None => None
      case Some(leaf) => leaf.nextKey(k)(termComp, ctx)
    }
  }

  def head(): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return $this.first().map { block =>
          root = block.map(_.unique_id)
          first = false

          block.isDefined
        }

        $this.next(root)(builder.ord).map { block =>
          root = block.map(_.unique_id)
          block.isDefined
        }
      }

      private def next1(): Future[Seq[(K, V, String)]] = {
        if (root.isEmpty || stop) return Future.successful(Seq.empty[(K, V, String)])
        ctx.getLeaf(root.get).map { block =>
          val list = block.tuples.filter(filter)
          stop = list.isEmpty
          if(stop) list else checkCounter(list)
        }
      }

      override def next(): Future[Seq[(K, V, String)]] = {
        if (first) return hasNext().flatMap { _ =>
          next1()
        }

        next1()
      }
    }
  }

  def tail(): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return $this.last().map { block =>
          root = block.map(_.unique_id)
          first = false

          block.isDefined
        }

        $this.prev(root)(builder.ord).map { block =>
          root = block.map(_.unique_id)
          block.isDefined
        }
      }

      private def prev1(): Future[Seq[(K, V, String)]] = {
        if (root.isEmpty || stop) return Future.successful(Seq.empty[(K, V, String)])
        ctx.getLeaf(root.get).map { block =>
          val list = block.tuples.reverse.filter(filter)
          stop = list.isEmpty
          if(stop) list else checkCounter(list)
        }
      }

      override def next(): Future[Seq[(K, V, String)]] = {
        if (first) return hasNext().flatMap { _ =>
          prev1()
        }

        prev1()
      }
    }
  }

  def asc(term: K, termInclusive: Boolean)(termComp: Ordering[K], findPathFn: (K, Meta[K, V], Ordering[K]) => (String, String) = (k, m, ord) => m.findPath(k)(ord)): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return nextKeyBlock(term, termInclusive, findPathFn)(termComp).map { block =>
          root = block.map(_.unique_id)
          first = false

          logger.debug(s"${Console.CYAN_B}First leaf search of ${ctx.builder.ks(term)}: ${block.map(_.print())}${Console.RESET}")

          block.isDefined
        }

        $this.next(root)(termComp).map { block =>
          root = block.map(_.unique_id)
          block.isDefined
        }
      }

      private def next1(): Future[Seq[(K, V, String)]] = {
        if (root.isEmpty || stop) return Future.successful(Seq.empty[(K, V, String)])
        ctx.getLeaf(root.get).map { block =>
          val list = block.tuples.filter(filter)
          stop = list.isEmpty
          if(stop) list else checkCounter(list)
        }
      }

      override def next(): Future[Seq[(K, V, String)]] = {
        if (first) return hasNext().flatMap { _ =>
          next1()
        }

        next1()
      }
    }
  }

  def desc(term: K, termInclusive: Boolean)(termComp: Ordering[K], findPathFn: (K, Meta[K, V], Ordering[K]) => (String, String) = (k, m, ord) => m.findPath(k)(ord)): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return previousKeyBlock(term, termInclusive, findPathFn)(termComp).map { block =>
          root = block.map(_.unique_id)
          first = false

          logger.debug(s"${Console.YELLOW_B}First leaf search of ${ctx.builder.ks(term)}: ${block.map(_.print())}${Console.RESET}")

          block.isDefined
        }

        $this.prev(root)(termComp).map { block =>
          root = block.map(_.unique_id)
          block.isDefined
        }
      }

      private def prev1(): Future[Seq[(K, V, String)]] = {
        if (root.isEmpty || stop) return Future.successful(Seq.empty[(K, V, String)])
        ctx.getLeaf(root.get).map { block =>
          val list = block.tuples.reverse.filter(filter)
          stop = list.isEmpty
          if(stop) list else checkCounter(list)
        }
      }

      override def next(): Future[Seq[(K, V, String)]] = {
        if (first) return hasNext().flatMap { _ =>
          prev1()
        }

        prev1()
      }
    }
  }

  def lt(term: K, termInclusive: Boolean, reverse: Boolean)(termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    val it = if(reverse) desc(term, termInclusive)(termComp) else head()

    it.filter = x => {
      (termInclusive && termComp.lteq(x._1, term)) || termComp.lt(x._1, term)
    }

    it
  }

  def gt(term: K, termInclusive: Boolean, reverse: Boolean)(termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    val it = if(reverse) tail() else asc(term, termInclusive)(termComp)

    it.filter = k => {
      (termInclusive && termComp.gteq(k._1, term)) || termComp.gt(k._1, term)
    }

    it
  }

  protected def gtReversePrefix(prefix: K, term: K, termInclusive: Boolean)(prefixComp: Ordering[K], termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return findPath(term, None, (k, m, ord) => {
          val reversed = m.pointers.reverse

          var idx = reversed.indexWhere{ case (k, _) =>
            prefixComp.gteq(k, prefix) && (termInclusive && termComp.gteq(k, term) || termComp.gt(k, term))
          }

          idx = if(idx < 0) 0 else idx

          val id = reversed(idx)._2.unique_id

          id
        })(termComp).map { block =>
          root = block.map(_.unique_id)
          first = false

          block.isDefined
        }

        $this.prev(root)(termComp).map { block =>
          root = block.map(_.unique_id)
          block.isDefined
        }
      }

      private def prev1(): Future[Seq[(K, V, String)]] = {
        if (root.isEmpty || stop) return Future.successful(Seq.empty[(K, V, String)])
        ctx.getLeaf(root.get).map { block =>
          val list = block.tuples.reverse.filter(filter)
          if(stop) list else checkCounter(list)
        }
      }

      override def next(): Future[Seq[(K, V, String)]] = {
        if (first) return hasNext().flatMap { _ =>
          prev1()
        }

        prev1()
      }
    }
  }

  /**
   *
   * @param prefix
   * @param term Must include the prefix!!!
   * @param inclusive
   * @param reverse
   * @param prefixComp
   * @param termComp
   * @return
   */
  def gt(prefix: K, term: K, inclusive: Boolean, reverse: Boolean)(prefixComp: Ordering[K],
                                                                   termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    assert(prefixComp.equiv(term, prefix) || termComp.equiv(prefix, term), s"Term ${ctx.builder.ks(term)} must include the prefix ${ctx.builder.ks(prefix)}!")

    val it = if(reverse) gtReversePrefix(prefix, term, inclusive)(prefixComp, termComp)
      else asc(term, inclusive)(termComp)

    it.filter = k => {
      prefixComp.equiv(k._1, prefix) && (inclusive && termComp.gteq(k._1, term) || termComp.gt(k._1, term))
    }

    it
  }

  /**
   * @param prefix Must include the prefix!
   * @param term
   * @param termInclusive
   * @param reverse
   * @param prefixComp
   * @param termComp
   * @return
   */
  def lt(prefix: K, term: K, termInclusive: Boolean, reverse: Boolean)(prefixComp: Ordering[K], termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    assert(prefixComp.equiv(term, prefix) || termComp.equiv(prefix, term), s"Term ${ctx.builder.ks(term)} must include the prefix ${ctx.builder.ks(prefix)}!")

    val it = if (reverse) desc(term, termInclusive)(termComp) else asc(prefix, true)(termComp)

    it.filter = k => {
      prefixComp.equiv(k._1, prefix) && (termInclusive && termComp.lteq(k._1, term) || termComp.lt(k._1, term))
    }

    it
  }

  protected def prefixReverse(term: K)(comp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return findPath(term, None, (k, m, ord) => {
          val reversed = m.pointers.reverse

          var idx = reversed.indexWhere { case (k, p) =>
            comp.gteq(k, term)
          }

          idx = if (idx < 0) 0 else idx

          val id = reversed(idx)._2.unique_id

          id
        })(comp).map { block =>
          root = block.map(_.unique_id)
          first = false

          block.isDefined
        }

        $this.prev(root)(comp).map { block =>
          root = block.map(_.unique_id)
          block.isDefined
        }
      }

      private def prev1(): Future[Seq[(K, V, String)]] = {
        if (root.isEmpty || stop) return Future.successful(Seq.empty[(K, V, String)])
        ctx.getLeaf(root.get).map { block =>
          val list = block.tuples.reverse.filter(filter)
          if (stop) list else checkCounter(list)
        }
      }

      override def next(): Future[Seq[(K, V, String)]] = {
        if (first) return hasNext().flatMap { _ =>
          prev1()
        }

        prev1()
      }
    }
  }

  def prefix(prefix: K, reverse: Boolean)(prefixComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    val it = if(reverse) prefixReverse(prefix)(prefixComp) else asc(prefix, true)(prefixComp)

    it.filter = x => {
      prefixComp.equiv(x._1, prefix)
    }

    it
  }

  def range(from: K, to: K, fromInclusive: Boolean, toInclusive: Boolean, reverse: Boolean)(termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {

    assert(termComp.gteq(to, from), s"to term ${ctx.builder.ks(to)} must be greater than from term ${ctx.builder.ks(from)}!")

    val it = if(reverse) desc(to, toInclusive)(termComp, (k, m, ord) => {
      val reversed = m.pointers.reverse

      var idx = reversed.lastIndexWhere { case (k, _) =>
        (toInclusive && termComp.gteq(k, to)) || termComp.gt(k, to)
      }

      idx = if(idx < 0) 0 else idx

      reversed(idx)._2.unique_id
    }) else asc(from, fromInclusive)(termComp)

    it.filter = k => {
      ((fromInclusive && termComp.gteq(k._1, from)) || termComp.gt(k._1, from)) &&
        ((toInclusive && termComp.lteq(k._1, to)) || termComp.lt(k._1, to))
    }

    it
  }

  /*protected def prefixForward(term: K)(comp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return findPath(term, None, (k, m, ord) => {
          val pointers = m.pointers

          var idx = pointers.indexWhere { case (k, p) =>
            comp.gteq(k, term)
          }

          idx = if (idx < 0) 0 else idx

          val id = pointers(idx)._2.unique_id

          id
        })(comp).map { block =>
          root = block.map(_.unique_id)
          first = false

          block.isDefined
        }

        $this.next(root)(comp).map { block =>
          root = block.map(_.unique_id)
          block.isDefined
        }
      }

      private def next1(): Future[Seq[(K, V, String)]] = {
        if (root.isEmpty || stop) return Future.successful(Seq.empty[(K, V, String)])
        ctx.getLeaf(root.get).map { block =>
          val list = block.tuples.filter(filter)
          if (stop) list else checkCounter(list)
        }
      }

      override def next(): Future[Seq[(K, V, String)]] = {
        if (first) return hasNext().flatMap { _ =>
          next1()
        }

        next1()
      }
    }
  }*/

  /*def prefixRange(prefixFrom: K, prefixTo: K, fromInclusive: Boolean, toInclusive: Boolean, reverse: Boolean)(prefixComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    assert(prefixComp.gt(prefixTo, prefixFrom), s"prefixTo ${ctx.builder.ks(prefixTo)} must be greater than prefixFrom ${ctx.builder.ks(prefixFrom)}!")

    val it = if(reverse) prefixReverse(prefixTo)(prefixComp) else prefixForward(prefixFrom)(prefixComp)

    it.filter = k => {
      ((fromInclusive && prefixComp.gteq(k._1, prefixFrom)) || prefixComp.gt(k._1, prefixFrom)) &&
        ((toInclusive && prefixComp.lteq(k._1, prefixTo)) || prefixComp.lt(k._1, prefixTo))
    }

    it
  }*/

  def isFull(): Boolean = {
    if(builder.MAX_N_ITEMS < 0) return false
    ctx.num_elements >= builder.MAX_N_ITEMS
  }

  def isEmpty(): Boolean = {
    ctx.root.isEmpty
  }

  def hasMinimum(): Boolean = {
    if(builder.MAX_N_ITEMS < 0) return true
    ctx.num_elements >= builder.MAX_N_ITEMS / 2
  }

  def hasEnough(): Boolean = {
    if(builder.MAX_N_ITEMS < 0) return true
    ctx.num_elements > builder.MAX_N_ITEMS / 2
  }

  def copy(sameId: Boolean = false): QueryableIndex[K, V] = {
    // Copies current snapshot
    var descriptor = ctx.snapshot()

    if(!sameId){
      descriptor = descriptor.withId(builder.idGenerator.generateIndexId())
    }

    val copy = new QueryableIndex[K, V](descriptor)(builder)

    ctx.newBlocksReferences.foreach { e =>
      copy.ctx.newBlocksReferences += e
    }

    ctx.parents.foreach { e =>
      copy.ctx.parents += e
    }

    copy
  }

  def merge(right: QueryableIndex[K, V], version: String = ctx.id): Future[QueryableIndex[K, V]] = {

    // This is allowed because at the moment of a splitting the number of elements could not be exactly half for both indexes.
    assert(ctx.num_elements + right.ctx.num_elements <= MAX_N_ITEMS,
      s"""The sum of number of elements from left and
         |right indexes must be less or equal to MAX_N_ITEMS, which is ${MAX_N_ITEMS}""".stripMargin)
    //assert(!right.hasMinimum(), s"The right index must be less than half of elements to be merged!")
    //assert(!hasMinimum(), s"The left index must be less than half of elements to be merged!")

    for {
      leftBlock <- ctx.get(ctx.root.get).map(_.copy())
      rightBlock <- right.ctx.get(right.ctx.root.get)
    } yield {
      leftBlock.merge(rightBlock, version)
      ctx.root = Some(leftBlock.unique_id)

      ctx.num_elements = ctx.num_elements + right.ctx.num_elements
      ctx.parents ++= right.ctx.parents
      ctx.newBlocksReferences ++= right.ctx.newBlocksReferences
      ctx.lastChangeVersion = UUID.randomUUID().toString

      this
    }
  }

  def split(): Future[QueryableIndex[K, V]] = {
    assert(isFull(), s"The index must be full to be split!")

   for {
     rootBlock <- ctx.get(ctx.root.get)
     leftBlock <- ctx.get(ctx.root.get).map(_.copy())
   } yield {

     val halfPos = rootBlock.length / 2

     val leftRefs = TrieMap.empty[(String, String), Block[K, V]]
     val rightRefs = TrieMap.empty[(String, String), Block[K, V]]

     ctx.newBlocksReferences.foreach { case (id, block) =>
       val pos = rootBlock.findPosition(block.last)

       if(pos < halfPos){
         logger.debug(s"New block ${id} goes left...")
         leftRefs += id -> block
       } else {
         logger.debug(s"New block ${id} goes right...")
         rightRefs += id -> block
       }
     }

     val leftParents = TrieMap.empty[(String, String), ParentInfo[K]]
     val rightParents = TrieMap.empty[(String, String), ParentInfo[K]]

     val allRefs = ctx.parents.iterator.toSeq

     allRefs.filterNot{case (id, _) => id == rootBlock.unique_id}.foreach { case (id, pinfo) =>
       val isLeft = ord.lteq(pinfo.key.get, rootBlock.middle)

       if(isLeft){
         logger.debug(s"${id} is on the left!")
         leftParents.put(id, pinfo)
       } else {
         logger.debug(s"${id} is on the right!")
         rightParents.put(id, pinfo)
       }
     }

     val rightBlock = leftBlock.split()

     if(leftBlock.isInstanceOf[Meta[K, V]]){
       leftBlock.asInstanceOf[Meta[K, V]].pointers.foreach { case (_, p) =>
         leftParents.put(p.unique_id, ctx.parents(p.unique_id))
       }
     }

     if(rightBlock.isInstanceOf[Meta[K, V]]){
       rightBlock.asInstanceOf[Meta[K, V]].pointers.foreach { case (_, p) =>
         rightParents.put(p.unique_id, ctx.parents(p.unique_id))
       }
     }

     ctx.parents.clear()

     val leftN = leftBlock.nSubtree
     val rightN = rightBlock.nSubtree

     logger.debug(s"number of elements: ${ctx.num_elements} leftN: ${leftN} rightN: ${rightN}...")
     assert(ctx.num_elements == leftN + rightN)

     ctx.num_elements = leftN
     ctx.lastChangeVersion = UUID.randomUUID.toString
     ctx.root = Some(leftBlock.unique_id)

     val rdescriptor = IndexContext()
       .withId(builder.idGenerator.generateIndexId())
       .withRoot(RootRef(rightBlock.unique_id._1, rightBlock.unique_id._2))
       .withLastChangeVersion(UUID.randomUUID.toString)
       .withMaxNItems(builder.MAX_N_ITEMS)
       .withNumElements(rightN)
       .withLevels(rightBlock.level)
       .withNumLeafItems(builder.MAX_LEAF_ITEMS)
       .withNumMetaItems(builder.MAX_META_ITEMS)

     val right = new QueryableIndex[K, V](rdescriptor)(builder)

     ctx.newBlocksReferences = leftRefs
     ctx.parents.addAll(leftParents)

     right.ctx.num_elements = rightN
     right.ctx.root = Some(rightBlock.unique_id)
     right.ctx.newBlocksReferences = rightRefs
     right.ctx.parents.addAll(rightParents)

     ctx.parents += leftBlock.unique_id -> ParentInfo(None, leftBlock.lastOption, 0)
     right.ctx.parents += rightBlock.unique_id -> ParentInfo(None, rightBlock.lastOption, 0)

     right
   }
  }
}
