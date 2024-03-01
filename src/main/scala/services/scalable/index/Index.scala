package services.scalable.index

import org.slf4j.LoggerFactory
import services.scalable.index.Commands._
import services.scalable.index.Errors.{IndexError, KEY_NOT_FOUND}
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.RichAsyncIndexIterator

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * The index class is meant to be executed on a single threaded context!
 * SerializeOpsSpec.scala contains an example demonstrating how to perform batch operations on the index!
 *
 * This class does not implement query operations like <, > and <=, interval(x,y). To implement
 * such operations, you can use a custom filter to find the first block containing the element being searched. From that
 * you need to use prev(block) and next(block) to iterate!
 *
 * This class is meant to be used like a multi operation container. To do a transaction, you create an instance of the class passing
 * the context. Once you done, the resulting context can be saved and passed to future instances of the class representing another set of
 * operations.
 */
class Index[K, V](protected val descriptor: IndexContext)(val builder: IndexBuilt[K, V]){

  import builder._

  assert(builder.MAX_LEAF_ITEMS == descriptor.numLeafItems,
    s"Builder and index maxLeafItems are not equal! Corrupted IndexContext or wrong configured builder!")
  assert(builder.MAX_META_ITEMS == descriptor.numMetaItems,
    s"Builder and index maxMetaItems are not equal! Corrupted IndexContext or wrong configured builder!")
  assert(builder.MAX_N_ITEMS == descriptor.maxNItems,
    s"Builder and index maxNItems are not equal! Corrupted IndexContext or wrong configured builder!")
  assert(descriptor.numLeafItems >= 4 && descriptor.numMetaItems >= 4,
    s"Number of leaf and meta elements must be greater or equal to 4!")

  val logger = LoggerFactory.getLogger(this.getClass)

  implicit var ctx = Context.fromIndexContext(descriptor)(builder)

  val $this = this

  def currentSnapshot(): IndexContext = {
    ctx.currentSnapshot()
  }

  /**
   * Creates a snapshot of the tree that could be saved and accessed later
   * @return
   */
  def snapshot(): IndexContext = {
    ctx.snapshot()
  }

  def save(): Future[IndexContext] = {
    ctx.save()
  }

  def findPath(k: K, start: Block[K,V], limit: Option[Block[K,V]],
               findPathFn: (K, Meta[K, V], Ordering[K]) => (String, String))
              (implicit ord: Ordering[K]): Future[Option[Leaf[K,V]]] = {

    if(limit.isDefined && limit.get.unique_id.equals(start.unique_id)){
      logger.debug(s"reached limit!")
      return Future.successful(None)
    }

    start match {
      case leaf: Leaf[K,V] => Future.successful(Some(leaf))
      case meta: Meta[K,V] =>

        meta.setPointers()

        val bid = findPathFn(k, meta, ord)

        ctx.get(bid).flatMap { block =>
          findPath(k, block, limit, findPathFn)
        }
    }
  }

  def findPath(k: K, limit: Option[Block[K,V]] = None,
               findPathFn: (K, Meta[K, V], Ordering[K]) => (String, String) =
               (k, m, ord) => m.findPath(k)(ord))
              (implicit ord: Ordering[K]): Future[Option[Leaf[K,V]]] = {
    if(ctx.root.isEmpty) {
      return Future.successful(None)
    }

    val bid = ctx.root.get

    ctx.get(ctx.root.get).flatMap { start =>
      ctx.setParent(bid, start.lastOption, 0, None)
      findPath(k, start, limit, findPathFn)
    }
  }

  protected def fixRoot(p: Block[K, V]): Future[Boolean] = {
    p match {
      case p: Meta[K,V] =>

        if(p.length == 1){
          val c = p.pointers(0)._2
          //ctx.root = Some(c.unique_id)

          ctx.levels -= 1

          ctx.get(c.unique_id).map { block =>
            block.level = ctx.levels

            val copy = block.copy()
            ctx.put(copy)

            ctx.root = Some(copy.unique_id)
            ctx.setParent(copy.unique_id, copy.lastOption, 0, None)

            true
          }
        } else {
          ctx.root = Some(p.unique_id)
          ctx.setParent(p.unique_id, p.lastOption, 0, None)

          Future.successful(true)
        }

      case p: Leaf[K,V] =>
        ctx.root = Some(p.unique_id)
        ctx.setParent(p.unique_id, p.lastOption, 0, None)

        Future.successful(true)
    }
  }

  protected def recursiveCopy(block: Block[K,V])(implicit ord: Ordering[K]): Future[Boolean] = {

    val opt = ctx.getParent(block.unique_id)

    if(opt.isEmpty){
      return setPath(block).flatMap(_ => recursiveCopy(block))
    }

    val pinfo = opt.get

    pinfo.parent match {
      case None => fixRoot(block)
      case Some(pid) => ctx.getMeta(pid).flatMap { p =>
        val parent = p.copy()

        parent.pointers(pinfo.pos) = block.last -> Pointer(block.partition, block.id, block.nSubtree, block.level)
        ctx.setParent(block.unique_id, block.lastOption, pinfo.pos, Some(parent.unique_id))

        parent.setPointers()

        recursiveCopy(parent)
      }
    }
  }

  protected def insertEmpty(data: Seq[Tuple3[K, V, Boolean]], insertVersion: String): Future[Int] = {
    val leaf = ctx.createLeaf()

    leaf.insert(data, insertVersion) match {
      case Success(n) =>

        ctx.levels += 1

        recursiveCopy(leaf).map(_ => n)
      case Failure(ex) => Future.failed(ex)
    }
  }

  protected def insertParent(left: Meta[K, V], prev: Block[K, V]): Future[Boolean] = {
    if(left.isFull()){
      val right = left.split()

      if(ord.gt(prev.last, left.last)){
        right.insert(Seq(prev.last -> Pointer(prev.partition, prev.id, prev.nSubtree, prev.level)))
      } else {
        left.insert(Seq(prev.last -> Pointer(prev.partition, prev.id, prev.nSubtree, prev.level)))
      }

      return handleParent(left, right)
    }

    left.insert(Seq(prev.last -> Pointer(prev.partition, prev.id, prev.nSubtree, prev.level))) match {
      case Success(n) => recursiveCopy(left).map(_ => true)
      case Failure(ex) => Future.failed(ex)
    }
  }

  protected def handleParent(left: Block[K,V], right: Block[K,V]): Future[Boolean] = {

    val opt = ctx.getParent(left.unique_id)

    if(opt.isEmpty) {
      return setPath(left).flatMap(_ => handleParent(left, right))
    }

    val pinfo = opt.get

    pinfo.parent match {
      case None =>

        logger.debug(s"${Console.BLUE_B}NEW LEVEL!${Console.RESET}")

        val meta = ctx.createMeta()

        ctx.levels += 1
        meta.level = ctx.levels

        meta.insert(Seq(
          left.last -> Pointer(left.partition, left.id, left.nSubtree, left.level),
          right.last -> Pointer(right.partition, right.id, right.nSubtree, right.level)
        ))

        meta.setPointers()

        recursiveCopy(meta)

      case Some(pid) => ctx.getMeta(pid).flatMap { p =>
        val parent = p.copy()

        parent.pointers(pinfo.pos) = left.last -> Pointer(left.partition, left.id, left.nSubtree, left.level)
        ctx.setParent(left.unique_id, left.lastOption, pinfo.pos, Some(parent.unique_id))

        insertParent(parent, right)
      }
    }
  }

  protected def splitLeaf(left: Leaf[K, V], data: Seq[Tuple3[K, V, Boolean]], insertVersion: String): Future[Int] = {
    val right = left.split()

    val (k, _, _) = data(0)
    val leftLast = left.last
    val rightLast = right.last

    var list = data

    // Avoids searching for the path again! :)
    if(ord.gt(k, leftLast)){

      if(!ord.gt(k, rightLast)){
        list = list.takeWhile{case (k, _, _) => ord.lt(k, rightLast)}
      }

      val rn = right.insert(list, insertVersion)

      return handleParent(left, right).map(_ => rn.get)
    }

    val ln = left.insert(list.takeWhile{case (k, _, _) => ord.lt(k, leftLast)}, insertVersion)

    handleParent(left, right).map{_ => ln.get}
  }

  protected def insertLeaf(left: Leaf[K, V], data: Seq[Tuple3[K, V, Boolean]], insertVersion: String): Future[Int] = {
    if(left.isFull()){
      logger.debug(s"${Console.RED_B}LEAF FULL...${Console.RESET}")

      /*val right = left.split()
      return handleParent(left, right).map{_ => 0}*/

      return splitLeaf(left, data, insertVersion)
    }

    left.insert(data, insertVersion) match {
      case Success(n) => recursiveCopy(left).map{_ => n}
      case Failure(ex) => Future.failed(ex)
    }
  }

  /**
   * @param data (key, value, upsert)
   * @param ord
   * @return
   */
  def insert(data: Seq[Tuple3[K, V, Boolean]], insertVersion: String): Future[InsertionResult] = {

    val sorted = data.sortBy(_._1)

    if(sorted.exists{case (k, _, _) => sorted.count{case (k1, _, _) => ord.equiv(k, k1)} > 1}){
      return Future.successful(InsertionResult(false, 0,
        Some(Errors.DUPLICATED_KEYS(data.map(_._1), ctx.builder.ks))))
    }

    val len = sorted.length
    var pos = 0

    def insert(): Future[Int] = {
      if(pos == len) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _, _) = list(0)

      findPath(k).flatMap {
        case None => insertEmpty(list, insertVersion)
        case Some(leaf) =>

          val idx = list.indexWhere{case (k, _, _) => ord.gt(k, leaf.last)}
          if(idx > 0) list = list.slice(0, idx)

          insertLeaf(leaf.copy(), list, insertVersion)
      }.flatMap { n =>
        pos += n
        ctx.num_elements += n
        insert()
      }
    }

    insert().map { n =>
      InsertionResult(true, n)
    }.recover {
      case t: IndexError => InsertionResult(false, 0, Some(t))
      case t: Throwable => throw t
    }
  }

  protected def merge(left: Block[K, V], lpos: Int, right: Block[K, V], rpos: Int, parent: Meta[K,V]): Future[Boolean] = {

    //ctx.levels -= 1

    left.merge(right)

    parent.setPointer(left, lpos)
    parent.removeAt(rpos)

    logger.debug(s"${Console.BLUE_B}merging ${if(left.isInstanceOf[Meta[K,V]]) "meta" else "leaf"} blocks...${Console.RESET}\n")

    if(parent.hasMinimum()){
      return recursiveCopy(parent)
    }

    val opt = ctx.getParent(parent.unique_id)

    if(opt.isEmpty){
      return setPath(parent).flatMap(_ => merge(left, lpos, right, rpos, parent))
    }

    val ginfo = opt.get

    if(ginfo.parent.isEmpty){
      return recursiveCopy(left)
    }

    ctx.getMeta(ginfo.parent.get).flatMap { gp =>
      borrow(parent, gp.copy(), ginfo.pos)
    }
  }

  protected def borrowRight(target: Block[K, V], left: Option[Block[K, V]], right: Option[(String, String)], parent: Meta[K,V], pos: Int): Future[Boolean] = {
    right match {
      case Some(id) => ctx.get(id).flatMap { r =>
        val right = r.copy()

        if(right.canBorrowTo(target)){

          right.borrowRightTo(target)

          parent.setPointer(target, pos)
          parent.setPointer(right, pos + 1)

          logger.debug(s"${Console.RED_B}borrowing from right ${if(target.isInstanceOf[Meta[K,V]]) "meta" else "leaf"}...${Console.RESET}\n")

          recursiveCopy(parent)

        } else {
          merge(target, pos, right, pos + 1, parent)
        }
      }

      case None => merge(left.get, pos - 1, target, pos, parent)
    }
  }

  protected def borrowLeft(target: Block[K, V], left: Option[(String, String)], right: Option[(String, String)], parent: Meta[K,V], pos: Int): Future[Boolean] = {
    left match {
      case Some(id) => ctx.get(id).flatMap { l =>
        val left = l.copy()

        if(left.canBorrowTo(target)){

          left.borrowLeftTo(target)

          parent.setPointer(left, pos - 1)
          parent.setPointer(target, pos)

          logger.debug(s"${Console.GREEN_B}borrowing from left ${if(target.isInstanceOf[Meta[K,V]]) "meta" else "leaf"}...${Console.RESET}\n")

          recursiveCopy(parent)

        } else {
          borrowRight(target, Some(left), right, parent, pos)
        }
      }

      case None => borrowRight(target, None, right, parent, pos)
    }
  }

  protected def borrow(target: Block[K, V], parent: Meta[K,V], pos: Int): Future[Boolean] = {

    val left = parent.left(pos)
    val right = parent.right(pos)

    // One parent with one child node
    if(left.isEmpty && right.isEmpty){

     // ctx.levels -= 1

      logger.debug(s"[remove] ONE LEVEL LESS...")
      return recursiveCopy(target)
    }

    borrowLeft(target, left, right, parent, pos)
  }

  protected def removeFromLeaf(target: Leaf[K, V], keys: Seq[Tuple2[K, Option[String]]]): Future[Int] = {
    val result = target.remove(keys)

    if(result.isFailure) return Future.failed(result.failed.get)

    if(target.hasMinimum()){
      logger.debug(s"${Console.YELLOW_B}removing from leaf...${Console.RESET}\n")
      return recursiveCopy(target).map(_ => result.get)
    }

    val opt = ctx.getParent(target.unique_id)

    if(opt.isEmpty){
      return setPath(target).flatMap(_ => removeFromLeaf(target, keys))
    }

    val pinfo = opt.get

    if(pinfo.parent.isEmpty){

      if(target.isEmpty()){

        ctx.levels -= 1

        logger.debug(s"${Console.RED_B}[remove] TREE IS EMPTY${Console.RESET}")

        ctx.root = None
        return Future.successful(result.get)
      }

      return recursiveCopy(target).map(_ => result.get)
    }

    ctx.getMeta(pinfo.parent.get).flatMap { p =>
      borrow(target, p.copy(), pinfo.pos).map(_ => result.get)
    }
  }

  /**
   * @param keys (key, compare_version_change?)
   * @param ord
   * @return
   */
  def remove(keys: Seq[Tuple2[K, Option[String]]]): Future[RemovalResult] = {
    val sorted = keys.distinct.sortBy(_._1)

    val len = sorted.length
    var pos = 0

    def remove(): Future[Int] = {
      if(pos == len) {
        ctx.num_elements -= sorted.length
        return Future.successful(sorted.length)
      }

      var list = sorted.slice(pos, len)
      val (k, _) = list(0)

      findPath(k).flatMap {
        case None => Future.failed(Errors.KEY_NOT_FOUND[K](k, ctx.builder.ks))
        case Some(leaf) =>

          val idx = list.indexWhere { case (k, _) => ord.gt(k, leaf.last)}
          list = if(idx > 0) list.slice(0, idx) else list

          removeFromLeaf(leaf.copy(), list)
      }.flatMap { n =>
        pos += n
        remove()
      }
    }

    remove().map { n =>
      RemovalResult(true, n)
    }.recover {
      case t: IndexError => RemovalResult(false, 0, Some(t))
      case t: Throwable => throw t
    }
  }

  protected def updateLeaf(left: Leaf[K, V], data: Seq[Tuple3[K, V, Option[String]]], updateVersion: String): Future[Int] = {
    val result = left.update(data, updateVersion)

    if (result.isFailure) return Future.failed(result.failed.get)

    recursiveCopy(left).map(_ => result.get)
  }

  /**
   * @param data (key, value, compare_version_change?)
   * @param mappingF
   * @param ord
   * @return
   */
  def update(data: Seq[Tuple3[K, V, Option[String]]], updateVersion: String): Future[UpdateResult] = {

    val sorted = data.sortBy(_._1)

    if(sorted.exists{case (k, _, _) => sorted.count{case (k1, _, _) => ord.equiv(k, k1)} > 1}){
      return Future.successful(UpdateResult(false, 0, Some(Errors.DUPLICATED_KEYS(sorted.map(_._1), ctx.builder.ks))))
    }

    val len = sorted.length
    var pos = 0

    def update(): Future[Int] = {
      if(len == pos) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _, _) = list(0)

      findPath(k).flatMap {
        case None => Future.failed(Errors.KEY_NOT_FOUND(k, ctx.builder.ks))
        case Some(leaf) =>

          val idx = list.indexWhere{case (k, _, _) => ord.gt(k, leaf.last)}
          if(idx > 0) list = list.slice(0, idx)

          updateLeaf(leaf.copy(), list, updateVersion)
      }.flatMap { n =>
        pos += n
        update()
      }
    }

    update().map { n =>
      UpdateResult(true, n)
    }.recover {
      case t: IndexError => UpdateResult(false, 0, Some(t))
      case t: Throwable => throw t
    }
  }

  def inOrder(f: Tuple[K, V] => Boolean = _ => true)(implicit ord: Ordering[K]): AsyncIndexIterator[Seq[Tuple[K, V]]] = new RichAsyncIndexIterator[K, V](f) {

    override def hasNext(): Future[Boolean] = {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(cur.isDefined)
    }

    override def next(): Future[Seq[Tuple[K, V]]] = {
      if(!firstTime){
        firstTime = true

        return first().map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)
            b.inOrder().filter(f)
        }
      }

      $this.next(cur.map(_.unique_id))(ord).map {
        case None =>
          cur = None
          Seq.empty[Tuple[K, V]]

        case Some(b) =>
          cur = Some(b)
          b.inOrder().filter(f)
      }
    }
  }

  def all(it: AsyncIndexIterator[Seq[Tuple[K, V]]] = inOrder())(implicit ec: ExecutionContext): Future[Seq[Tuple[K, V]]] = {
    it.hasNext().flatMap {
      case true => it.next().flatMap { list =>
        all(it).map {
          list ++ _
        }
      }
      case false => Future.successful(Seq.empty[Tuple[K, V]])
    }
  }

  def reverse(f: Tuple[K, V] => Boolean = _ => true)(implicit ord: Ordering[K]): AsyncIndexIterator[Seq[Tuple[K, V]]] = new RichAsyncIndexIterator[K, V](f) {

    override def hasNext(): Future[Boolean] = {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(cur.isDefined)
    }

    override def next(): Future[Seq[Tuple[K, V]]] = {
      if(!firstTime){
        firstTime = true

        return last().map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)
            b.inOrder().reverse.filter(f)
        }
      }

      $this.prev(cur.map(_.unique_id))(ord).map {
        case None =>
          cur = None
          Seq.empty[Tuple[K, V]]

        case Some(b) =>
          cur = Some(b)
          b.inOrder().reverse.filter(f)
      }
    }
  }

  def getLeftMost(start: Option[Block[K,V]]): Future[Option[Leaf[K,V]]] = {
    start match {
      case None => Future.successful(None)
      case Some(b) => b match {
        case b: Leaf[K,V] => Future.successful(Some(b))
        case b: Meta[K,V] =>

          b.setPointers()

          ctx.get(b.pointers(0)._2.unique_id).flatMap(b => getLeftMost(Some(b)))
      }
    }
  }

  /**
   * To be able to get next or prev we have to set parents from root to the node (partial path)
   */
  def setPath(b: Block[K,V])(implicit ord: Ordering[K]): Future[Boolean] = {

    //It makes sure leaf node it is part of current root tree...
    if(!ctx.isFromCurrentContext(b)){
      return Future.failed(Errors.BLOCK_NOT_SAME_CONTEXT(b.root, ctx.root))
    }

    logger.debug(s"\nSETTING PATH...\n")

    findPath(b.last, Some(b)).map { _ =>
      true
    }
  }

  def first(): Future[Option[Leaf[K,V]]] = {
    if(ctx.root.isEmpty) return Future.successful(None)

    val root = ctx.root.get

    /*if(ctx.isParentDefined(root)){
      return ctx.get(root).flatMap(opt => getLeftMost(opt))
    }

    ctx.get(root).flatMap{opt => setPath(opt.get).flatMap{ _ => getLeftMost(opt)}}*/

    ctx.get(root).flatMap{ b =>
      ctx.setParent(b.unique_id, b.lastOption, 0, None)
      getLeftMost(Some(b))
    }
  }

  def getRightMost(start: Option[Block[K,V]]): Future[Option[Leaf[K,V]]] = {
    start match {
      case None => Future.successful(None)
      case Some(b) => b match {
        case b: Leaf[K,V] => Future.successful(Some(b))
        case b: Meta[K,V] =>

          b.setPointers()

          ctx.get(b.pointers(b.pointers.length - 1)._2.unique_id).flatMap(b => getRightMost(Some(b)))
      }
    }
  }

  def last(): Future[Option[Leaf[K,V]]] = {
    if(ctx.root.isEmpty) return Future.successful(None)

    val root = ctx.root.get

    /*if(ctx.isParentDefined(root)){
      return ctx.get(root).flatMap(opt => getRightMost(opt))
    }

    ctx.get(root).flatMap{opt => setPath(opt.get).flatMap{ _ => getRightMost(opt)}}*/

    ctx.get(root).flatMap{ b =>
      ctx.setParent(b.unique_id, b.lastOption, 0, None)
      getRightMost(Some(b))
    }
  }

  def next(current: Option[(String, String)])(implicit ord: Ordering[K]): Future[Option[Leaf[K,V]]] = {

    def nxt(b: Block[K,V]): Future[Option[Leaf[K,V]]] = {

      val opt = ctx.getParent(b.unique_id)

      if(opt.isEmpty){
        return setPath(b).flatMap(_ => next(current))
      }

      val pinfo = opt.get

      pinfo.parent match {
        case None => Future.successful(None)
        case Some(pid) => ctx.getMeta(pid).flatMap { parent =>

          val pointers = parent.pointers
          val len = pointers.length

          parent.setPointers()

          if (pinfo.pos == len - 1) {
            nxt(parent)
          } else {
            ctx.get(pointers(pinfo.pos + 1)._2.unique_id).flatMap(b => getLeftMost(Some(b)))
          }
        }
      }
    }

    current match {
      case None => first()
      case Some(current) => ctx.get(current).flatMap {nxt(_)}
    }
  }

  def prev(current: Option[(String, String)])(implicit ord: Ordering[K]): Future[Option[Leaf[K,V]]] = {

    def prv(b: Block[K,V]): Future[Option[Leaf[K,V]]] = {

      val opt = ctx.getParent(b.unique_id)

      if(opt.isEmpty){
        return setPath(b).flatMap(_ => prev(current))
      }

      val pinfo = opt.get

      pinfo.parent match {
        case None => Future.successful(None)
        case Some(pid) => ctx.getMeta(pid).flatMap { parent =>
          parent.setPointers()

          if (pinfo.pos == 0) {
            prev(Some(parent.unique_id))
          } else {
            ctx.get(parent.pointers(pinfo.pos - 1)._2.unique_id).flatMap(b => getRightMost(Some(b)))
          }
        }
      }
    }

    current match {
      case None => first()
      case Some(current) => ctx.get(current).flatMap {prv(_)}
    }
  }

  def get(k: K)(implicit ord: Ordering[K] = builder.ord): Future[Option[Tuple[K,V]]] = {
    findPath(k)(ord).flatMap {
      case None => Future.successful(None)
      case Some(leaf) => Future.successful(leaf.find(k)(ord))
    }
  }

  def getAll(keys: Seq[K], mustFindAll: Boolean = false)(implicit ord: Ordering[K] = builder.ord): Future[GetResult[K, V]] = {

    val sorted = keys.sorted(ord)

    val len = sorted.length
    var pos = 0

    var results = Seq.empty[Tuple[K, V]]

    def get(): Future[Int] = {
      if(pos == len) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val k = list(0)

      findPath(k)(ord).map {
        case None => if(mustFindAll){
          throw new KEY_NOT_FOUND[K](k, ks)
        } else {
          1
        }

        case Some(leaf) =>

          val idx = list.indexWhere{k => ord.gt(k, leaf.last)}
          if(idx > 0) list = list.slice(0, idx)

          val (data, keyNotFound) = getKeysFromLeaf(leaf, list, mustFindAll)(ord)

          if(keyNotFound.isDefined) {
            throw new KEY_NOT_FOUND[K](k, ks)
          }

          results :++= data
          list.length
      }.flatMap { n =>
        pos += n
        get()
      }
    }

    get().map { _ =>
      GetResult[K, V](true, results, None)
    }.recover {
      case t: IndexError => GetResult[K, V](false, results, Some(t))
      case t: Throwable => throw t
    }
  }

  protected def getKeysFromLeaf(leaf: Leaf[K, V], list: Seq[K], mustFindAll: Boolean)(ord: Ordering[K]): (Seq[Tuple[K, V]], Option[K]) = {
    var results = Seq.empty[Tuple[K, V]]

    for(k <- list){
      val v = leaf.find(k)(ord)

      if(mustFindAll && v.isEmpty){
        return results -> Some(k)
      }

      if(v.isDefined) results :+= v.get
    }

    results -> None
  }

  def min(): Future[Option[Tuple[K,V]]] = {
    first().flatMap {
      case None => Future.successful(None)
      case Some(leaf) => Future.successful(leaf.min())
    }
  }

  def max(): Future[Option[Tuple[K,V]]] = {
    last().flatMap {
      case None => Future.successful(None)
      case Some(leaf) => Future.successful(leaf.max())
    }
  }

  def count(): Long = ctx.num_elements
  def levels(): Int = ctx.levels

  /*
   * Prints any subtree from the provided root
   * Caution: prettyPrint is currently synchronous!
   */
  def prettyPrint(root: Option[(String, String)] = ctx.root, timeout: Duration = Duration.Inf): (Int, Int) = {

    val levels = scala.collection.mutable.Map[Int, scala.collection.mutable.ArrayBuffer[Block[K,V]]]()
    var num_data_blocks = 0

    def inOrder(start: Block[K,V], level: Int): Unit = {

      val opt = levels.get(level)
      var l: scala.collection.mutable.ArrayBuffer[Block[K,V]] = null

      if(opt.isEmpty){
        l = scala.collection.mutable.ArrayBuffer[Block[K,V]]()
        levels  += level -> l
      } else {
        l = opt.get
      }

      start match {
        case data: Leaf[K,V] =>
          num_data_blocks += 1
          l += data

        case meta: Meta[K,V] =>

          l += meta

          val length = meta.pointers.length
          val pointers = meta.pointers

          for(i<-0 until length){
            inOrder(Await.result(ctx.get(pointers(i)._2.unique_id), timeout), level + 1)
          }

      }
    }

    root match {
      case Some(id) => inOrder(Await.result(ctx.get(id), timeout), 0)
      case _ =>
    }

    logger.info("BEGIN BTREE:\n")
    levels.keys.toSeq.sorted.foreach { case level =>
      logger.info(s"level[$level]: ${levels(level).map(_.print())}\n")
    }
    logger.info("END BTREE\n")

    levels.size -> num_data_blocks
  }

  protected[index] def getNumLevels(root: Option[(String, String)] = ctx.root): Future[Int] = {

    val levels = TrieMap.empty[Int, scala.collection.mutable.ArrayBuffer[Block[K,V]]]
    val num_data_blocks = new AtomicInteger(0)

    def inOrder(start: Block[K,V], level: Int): Future[Unit] = {

      val opt = levels.get(level)
      var l: scala.collection.mutable.ArrayBuffer[Block[K,V]] = null

      if(opt.isEmpty){
        l = scala.collection.mutable.ArrayBuffer[Block[K,V]]()
        levels  += level -> l
      } else {
        l = opt.get
      }

      start match {
        case data: Leaf[K,V] =>
          num_data_blocks.incrementAndGet()
          l += data
          Future.successful{}

        case meta: Meta[K,V] =>

          l += meta

          val length = meta.pointers.length
          val pointers = meta.pointers

          var tasks = Seq.empty[Future[Unit]]

          for(i<-0 until length){
            tasks :+= ctx.get(pointers(i)._2.unique_id).flatMap(b => inOrder(b, level + 1))
          }

          Future.sequence(tasks).map(_ => {})

      }
    }

    root match {
      case Some(id) => ctx.get(id).flatMap(b => inOrder(b, 0)).map(_ => levels.size)
      case _ => Future.successful(0)
    }
  }

  def execute(cmds: Seq[Command[K, V]], version: String = ctx.id): Future[BatchResult] = {

    def process(pos: Int, error: Option[Throwable]): Future[BatchResult] = {
      if(error.isDefined) {
        return Future.successful(BatchResult(false, error))
      }

      if(pos == cmds.length) {
        return Future.successful(BatchResult(true))
      }

      val cmd = cmds(pos)

      (cmd match {
        case cmd: Insert[K, V] => insert(cmd.list, version)
        case cmd: Remove[K, V] => remove(cmd.keys)
        case cmd: Update[K, V] => update(cmd.list, version)
      }).flatMap(prev => process(pos + 1, prev.error))
    }

    process(0, None)
  }
}