package services.scalable.index

import services.scalable.index.grpc.{IndexContext, RootRef}
import services.scalable.index.impl.RichAsyncIndexIterator

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
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

  def previousKey(k: K, orEqual: Boolean)(termComp: Ordering[K],
                                          findPathFn: (K, Meta[K, V], Ordering[K]) => (String, String)): Future[Option[Leaf[K, V]]] = {
    findPath(k, None, findPathFn)(termComp).flatMap {
      case None => Future.successful(None)
      case Some(leaf) =>

        var (_, idx) = leaf.binSearch(k)
        idx = if(idx == leaf.length) leaf.length - 1 else idx
        val (key, _, _) = leaf.tuples(idx)

        val r = termComp.compare(key, k)

        if (r == 0) {
          if (orEqual || idx > 0) {
            Future.successful(Some(leaf))
          } else {
            prev(Some(leaf.unique_id))
          }
        } else {
          Future.successful(Some(leaf))
        }
    }
  }

  def nextKey(k: K, orEqual: Boolean)(termComp: Ordering[K], findPathFn: (K, Meta[K, V], Ordering[K]) => (String, String)): Future[Option[Leaf[K, V]]] = {
    findPath(k, None, findPathFn)(termComp).flatMap {
      case None => Future.successful(None)
      case Some(leaf) =>

        var (_, idx) = leaf.binSearch(k)
        idx = if(idx == leaf.length) leaf.length - 1 else idx
        val (key, _, _) = leaf.tuples(idx)

        val r = termComp.compare(key, k)

        if (r == 0) {
          if (orEqual || idx < leaf.length - 1) {
            Future.successful(Some(leaf))
          } else {
            next(Some(leaf.unique_id))
          }
        } else {
          Future.successful(Some(leaf))
        }
    }
  }

  def asc(term: K, termInclusive: Boolean)(termComp: Ordering[K], findPathFn: (K, Meta[K, V], Ordering[K]) => (String, String)): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return nextKey(term, termInclusive)(termComp, findPathFn).map { block =>
          root = block.map(_.unique_id)
          first = false

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

  def desc(term: K, termInclusive: Boolean)(termComp: Ordering[K], findPathFn: (K, Meta[K, V], Ordering[K]) => (String, String)): RichAsyncIndexIterator[K, V] = {
    new RichAsyncIndexIterator[K, V]() {

      var root: Option[(String, String)] = None
      var first = true

      override def hasNext(): Future[Boolean] = {
        if (stop) return Future.successful(false)

        if (first) return previousKey(term, termInclusive)(termComp, findPathFn).map { block =>
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

  def lt(term: K, termInclusive: Boolean, reverse: Boolean)(termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    val it = if(reverse) desc(term, termInclusive)(termComp, (_, meta: Meta[K, V], _) => {
     val idx = meta.pointers.indexWhere(x => termInclusive && termComp.lteq(x._1, term) || termComp.lt(x._1, term))
      meta.pointers(if(idx < 0) 0 else idx)._2.unique_id
    }) else head()

    it.filter = x => {
      (termInclusive && termComp.lteq(x._1, term)) || termComp.lt(x._1, term)
    }

    it
  }

  def lt(prefix: K, term: K, termInclusive: Boolean, reverse: Boolean)(prefixComp: Ordering[K],
                                                                       termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    val fpfn = (k: K, meta: Meta[K, V], ordering: Ordering[K]) => {
      val idx = meta.pointers.indexWhere(x => prefixComp.equiv(x._1, prefix) &&
          (termInclusive && termComp.lteq(x._1, term) || termComp.lt(x._1, term)))
      meta.pointers(if(idx < 0) 0 else idx)._2.unique_id
    }

    val it = if (reverse) desc(term, termInclusive)(termComp, fpfn) else
      asc(prefix, true)(termComp, fpfn)

    it.filter = k => {
      prefixComp.equiv(k._1, prefix) && (termInclusive && termComp.lteq(k._1, term) || termComp.lt(k._1, term))
    }

    it
  }

  def gt(prefix: K, term: K, termInclusive: Boolean, reverse: Boolean)(prefixComp: Ordering[K],
                                                                       termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    val fpfn = (k: K, meta: Meta[K, V], ordering: Ordering[K]) => {
      val idx = meta.pointers.indexWhere(x => prefixComp.equiv(x._1, prefix) &&
          (termInclusive && termComp.gteq(x._1, term) || termComp.gt(x._1, term)))
      meta.pointers(if(idx < 0) 0 else idx)._2.unique_id
    }

    val it = if(reverse) gtReversePrefix(prefix, term, termInclusive)(prefixComp, termComp)
      else asc(term, termInclusive)(termComp, fpfn)

    it.filter = k => {
      prefixComp.equiv(k._1, prefix) && (termInclusive && termComp.gteq(k._1, term) || termComp.gt(k._1, term))
    }

    it
  }

  def gt(term: K, termInclusive: Boolean, reverse: Boolean)(termComp: Ordering[K]): RichAsyncIndexIterator[K, V] = {
    val it = if(reverse) tail() else asc(term, termInclusive)(termComp, (_, meta, _) => {
      val idx = meta.pointers.indexWhere(x => termInclusive && termComp.gteq(x._1, term) || termComp.gt(x._1, term))
      meta.pointers(if(idx < 0) 0 else idx)._2.unique_id
    })

    it.filter = k => {
      (termInclusive && termComp.gteq(k._1, term)) || termComp.gt(k._1, term)
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

  def prefix(prefix: K, reverse: Boolean)(prefixComp: Ordering[K], findPathFn: (K, Meta[K, V], Ordering[K]) => (String, String)): RichAsyncIndexIterator[K, V] = {
    val it = if(reverse) prefixReverse(prefix)(prefixComp)
      else asc(prefix, true)(prefixComp, findPathFn)

    it.filter = x => {
      prefixComp.equiv(x._1, prefix)
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

          var idx = reversed.indexWhere{ case (k, p) =>
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

  def range(from: K, to: K, fromInclusive: Boolean, toInclusive: Boolean, reverse: Boolean)(termComp: Ordering[K],
                                                                                            findPathFn: (K, Meta[K, V], Ordering[K]) => (String, String)): RichAsyncIndexIterator[K, V] = {
    val it = if(reverse) desc(to, toInclusive)(termComp, findPathFn) else asc(from, fromInclusive)(termComp, findPathFn)

    it.filter = k => {
      ((fromInclusive && termComp.gteq(k._1, from)) || termComp.gt(k._1, from)) &&
        ((toInclusive && termComp.lteq(k._1, to)) || termComp.lt(k._1, to))
    }

    it
  }

  def isFull(): Boolean = {
    if(builder.MAX_N_ITEMS < 0) return false
    ctx.num_elements >= builder.MAX_N_ITEMS
  }

  def isEmpty(): Boolean = {
    ctx.root.isEmpty
  }

  def hasMinimum(): Boolean = {
    ctx.num_elements >= builder.MAX_N_ITEMS / 2
  }

  def copy(sameId: Boolean = false): QueryableIndex[K, V] = {
    // Copies current snapshot
    var descriptor = ctx.snapshot()

    if(!sameId){
      descriptor = descriptor.withId(builder.idGenerator.generateIndexId())
    }

    val copy = new QueryableIndex[K, V](descriptor)(builder)

    ctx.newBlocksReferences.foreach { case (id, b) =>
      copy.ctx.newBlocksReferences += id -> b
    }

    ctx.parents.foreach { case (k, v) =>
      copy.ctx.parents += k -> v
    }

    copy
  }

  def split(): Future[QueryableIndex[K, V]] = {
   assert(isFull(), s"The index must be full to be splitten!")

   for {
     leftBlock <- ctx.get(ctx.root.get)
     rightBlock = leftBlock.split()

   } yield {
     val halfPos = leftBlock.length / 2

     val leftN = leftBlock.nSubtree
     val rightN = rightBlock.nSubtree

     assert(ctx.num_elements == leftN + rightN)

     val leftRefs = mutable.WeakHashMap.empty[(String, String), (String, String)]
     val rightRefs = mutable.WeakHashMap.empty[(String, String), (String, String)]

     val leftParents = TrieMap.empty[(String, String), (Option[(String, String)], Int)]
     val rightParents = TrieMap.empty[(String, String), (Option[(String, String)], Int)]

     ctx.newBlocksReferences.foreach { case (id, block) =>
       val pos = ctx.getRootPosition(id)

       if(pos < halfPos){
          leftRefs += id -> block
          leftParents += id -> ctx.parents(id)
       } else {
         rightRefs += id -> block
         rightParents += id -> ctx.parents(id)
       }
     }

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

     ctx.parents.clear()

     leftParents.foreach { e =>
       ctx.parents += e
     }

     right.ctx.num_elements = rightN
     right.ctx.root = Some(rightBlock.unique_id)
     right.ctx.newBlocksReferences = rightRefs

     rightParents.foreach { e =>
       right.ctx.parents += e
     }

     right
   }
  }
}
