package services.scalable.index

import org.slf4j.LoggerFactory
import services.scalable.index.Commands._
import services.scalable.index.grpc.{IndexContext, RootRef}

import java.util.UUID
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
class Index[K, V](val ictx: IndexContext)(implicit val ec: ExecutionContext,
                                      val ctx: Context[K, V],
                                      val storage: Storage,
                                      val serializer: Serializer[Block[K, V]],
                                      val cache: Cache,
                                      val ord: Ordering[K],
                                      val idGenerator: IdGenerator){

  val id = UUID.randomUUID.toString

  ctx.createCtxSpace(id)

  val logger = LoggerFactory.getLogger(this.getClass)

  val $this = this

  var root: Option[(String, String)] = ictx.root.map{r => r.partition -> r.id}
  var num_elements: Long = ictx.numElements
  var levels: Int = ictx.levels

  def isFromCurrentContext(b: Block[K, V]): Boolean = {
    b.root.equals(root)
  }
  
  /**
   * Creates a snapshot of the tree that could be saved and accessed later
   * @return
   */
  def snapshot(): IndexContext = {
    ctx.getBlocks(this.id).filter(_._2.isNew).foreach { case (_, b) =>
      b.root = root
      b.isNew = false
    }

    logger.info(s"\nSNAPSHOT ${ictx.id}: ${root.map{r => RootRef(r._1, r._2)}}\n")

    IndexContext(ictx.id, ictx.numLeafItems, ictx.numMetaItems, root.map{r => RootRef(r._1, r._2)}, levels, num_elements)
  }

  def clear(): Unit = {
    ctx.clear(this.id)
  }

  def save(): Future[IndexContext] = {
    val s = snapshot()
    storage.save(s, ctx.getBlocks(this.id).map{case (id, block) => id -> serializer.serialize(block)}.toMap).map(_ => s)
  }

  def findPath(k: K, start: Block[K,V], limit: Option[Block[K,V]])(implicit ord: Ordering[K]): Future[Option[Leaf[K,V]]] = {

    if(limit.isDefined && limit.get.unique_id.equals(start.unique_id)){
      logger.debug(s"reached limit!")
      return Future.successful(None)
    }

    start match {
      case leaf: Leaf[K,V] => Future.successful(Some(leaf))
      case meta: Meta[K,V] =>

        meta.setPointers()(this.ctx, this.id)

        val bid = meta.findPath(k)

        ctx.get(this.id, bid).flatMap { block =>
          findPath(k, block, limit)
        }
    }
  }

  def findPath(k: K, limit: Option[Block[K,V]] = None)(implicit ord: Ordering[K]): Future[Option[Leaf[K,V]]] = {
    if(root.isEmpty) {
      return Future.successful(None)
    }

    val bid = root.get

    ctx.get(this.id, root.get).flatMap { start =>
      ctx.setParent(id, bid, 0, None)
      findPath(k, start, limit)
    }
  }

  protected def fixRoot(p: Block[K, V]): Future[Boolean] = {
    p match {
      case p: Meta[K,V] =>

        if(p.length == 1){
          val c = p.pointers(0)._2
          root = Some(c.unique_id)

          levels -= 1

          ctx.get(this.id, c.unique_id).map { block =>
            block.level = levels

            val copy = block.copy()(this.ctx, this.id)
            ctx.put(this.id, copy)

            ctx.setParent(this.id, copy.unique_id, 0, None)

            true
          }
        } else {
          root = Some(p.unique_id)
          ctx.setParent(this.id, p.unique_id, 0, None)

          Future.successful(true)
        }

      case p: Leaf[K,V] =>
        root = Some(p.unique_id)
        ctx.setParent(this.id, p.unique_id, 0, None)

        Future.successful(true)
    }
  }

  protected def recursiveCopy(block: Block[K,V])(implicit ord: Ordering[K]): Future[Boolean] = {

    val opt = ctx.getParent(this.id, block.unique_id)

    if(opt.isEmpty){
      return setPath(block).flatMap(_ => recursiveCopy(block))
    }

    val (p, pos) = opt.get

    p match {
      case None => fixRoot(block)
      case Some(pid) => ctx.getMeta(id, pid).flatMap { p =>
        val parent = p.copy()(this.ctx, this.id)

        parent.pointers(pos) = block.last -> Pointer(block.partition, block.id, block.nSubtree, block.level)
        ctx.setParent(id, block.unique_id, pos, Some(parent.unique_id))

        parent.setPointers()(this.ctx, this.id)

        recursiveCopy(parent)
      }
    }
  }

  protected def insertEmpty(data: Seq[Tuple[K,V]], upsert: Boolean)(implicit ord: Ordering[K]): Future[Int] = {
    val leaf = ctx.createLeaf(id)

    leaf.insert(data, upsert) match {
      case Success(n) =>

        levels += 1

        recursiveCopy(leaf).map(_ => n)
      case Failure(ex) => Future.failed(ex)
    }
  }

  protected def insertParent(left: Meta[K, V], prev: Block[K, V])(implicit ord: Ordering[K]): Future[Boolean] = {
    if(left.isFull()){
      val right = left.split()(this.ctx, this.id)

      if(ord.gt(prev.last, left.last)){
        right.insert(Seq(prev.last -> Pointer(prev.partition, prev.id, prev.nSubtree, prev.level)))(ctx, this.id, ord)
      } else {
        left.insert(Seq(prev.last -> Pointer(prev.partition, prev.id, prev.nSubtree, prev.level)))(ctx, this.id, ord)
      }

      return handleParent(left, right)
    }

    left.insert(Seq(prev.last -> Pointer(prev.partition, prev.id, prev.nSubtree, prev.level)))(ctx, this.id, ord) match {
      case Success(n) => recursiveCopy(left).map(_ => true)
      case Failure(ex) => Future.failed(ex)
    }
  }

  protected def handleParent(left: Block[K,V], right: Block[K,V])(implicit ord: Ordering[K]): Future[Boolean] = {

    val opt = ctx.getParent(this.id, left.unique_id)

    if(opt.isEmpty) {
      return setPath(left).flatMap(_ => handleParent(left, right))
    }

    val (p, pos) = opt.get

    p match {
      case None =>

        logger.debug(s"${Console.BLUE_B}NEW LEVEL!${Console.RESET}")

        val meta = ctx.createMeta(id)

        levels += 1
        meta.level = levels

        meta.insert(Seq(
          left.last -> Pointer(left.partition, left.id, left.nSubtree, left.level),
          right.last -> Pointer(right.partition, right.id, right.nSubtree, right.level)
        ))(ctx, this.id, ord)

        meta.setPointers()(this.ctx, this.id)

        recursiveCopy(meta)

      case Some(pid) => ctx.getMeta(id, pid).flatMap { p =>
        val parent = p.copy()(this.ctx, this.id)

        parent.pointers(pos) = left.last -> Pointer(left.partition, left.id, left.nSubtree, left.level)
        ctx.setParent(id, left.unique_id, pos, Some(parent.unique_id))

        insertParent(parent, right)
      }
    }
  }

  protected def splitLeaf(left: Leaf[K,V], data: Seq[Tuple[K,V]], upsert: Boolean)(implicit ord: Ordering[K]): Future[Int] = {
    val right = left.split()(this.ctx, this.id)

    val (k, _) = data(0)
    val leftLast = left.last
    val rightLast = right.last

    var list = data

    // Avoids searching for the path again! :)
    if(ord.gt(k, leftLast)){

      if(!ord.gt(k, rightLast)){
        list = list.takeWhile{case (k, _) => ord.lt(k, rightLast)}
      }

      val rn = right.insert(list, upsert)

      return handleParent(left, right).map(_ => rn.get)
    }

    val ln = left.insert(list.takeWhile{case (k, _) => ord.lt(k, leftLast)}, upsert)

    handleParent(left, right).map{_ => ln.get}
  }

  protected def insertLeaf(left: Leaf[K,V], data: Seq[Tuple[K,V]], upsert: Boolean)(implicit ord: Ordering[K]): Future[Int] = {
    if(left.isFull()){

      logger.debug(s"${Console.RED_B}LEAF FULL...${Console.RESET}")

      /*val right = left.split()
      return handleParent(left, right).map{_ => 0}*/

      return splitLeaf(left, data, upsert)
    }

    left.insert(data, upsert) match {
      case Success(n) => recursiveCopy(left).map{_ => n}
      case Failure(ex) => Future.failed(ex)
    }
  }

  def insert(data: Seq[Tuple[K,V]], upsert: Boolean = false)(implicit ord: Ordering[K]): Future[Int] = {

    val sorted = data.sortBy(_._1)

    if(sorted.exists{case (k, _) => sorted.count{case (k1, _) => ord.equiv(k, k1)} > 1}){
      //throw new RuntimeException(s"Repeated elements: ${sorted.map{case (k, v) => new String(k.asInstanceOf[Bytes])}}")
      return Future.failed(Errors.DUPLICATE_KEYS(data))
    }

    val len = sorted.length
    var pos = 0

    def insert(): Future[Int] = {
      if(pos == len) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _) = list(0)

      findPath(k).flatMap {
        case None => insertEmpty(list, upsert)
        case Some(leaf) =>

          val idx = list.indexWhere{case (k, _) => ord.gt(k, leaf.last)}
          if(idx > 0) list = list.slice(0, idx)

          insertLeaf(leaf.copy()(this.ctx, this.id), list, upsert)
      }.flatMap { n =>
        pos += n

        num_elements += n

        insert()
      }
    }

    insert()
  }

  protected def merge(left: Block[K, V], lpos: Int, right: Block[K, V], rpos: Int, parent: Meta[K,V])
                                 (implicit ord: Ordering[K]): Future[Boolean] = {

    //ctx.levels -= 1

    left.merge(right)(this.ctx, this.id)

    parent.setPointer(left, lpos)(this.ctx, this.id)
    parent.removeAt(rpos)(this.ctx, this.id)

    logger.debug(s"${Console.BLUE_B}merging ${if(left.isInstanceOf[Meta[K,V]]) "meta" else "leaf"} blocks...${Console.RESET}\n")

    if(parent.hasMinimum()){
      return recursiveCopy(parent)
    }

    val opt = ctx.getParent(this.id, parent.unique_id)

    if(opt.isEmpty){
      return setPath(parent).flatMap(_ => merge(left, lpos, right, rpos, parent))
    }

    val (g, gpos) = opt.get

    if(g.isEmpty){
      return recursiveCopy(left)
    }

    ctx.getMeta(id, g.get).flatMap { gp =>
      borrow(parent, gp.copy()(this.ctx, this.id), gpos)
    }
  }

  protected def borrowRight(target: Block[K, V], left: Option[Block[K, V]], right: Option[(String, String)], parent: Meta[K,V], pos: Int)
                                       (implicit ord: Ordering[K]): Future[Boolean] = {
    right match {
      case Some(id) => ctx.get(this.id, id).flatMap { r =>
        val right = r.copy()(this.ctx, this.id)

        if(right.canBorrowTo(target)){

          right.borrowRightTo(target)(this.ctx, this.id)

          parent.setPointer(target, pos)(this.ctx, this.id)
          parent.setPointer(right, pos + 1)(this.ctx, this.id)

          logger.debug(s"${Console.RED_B}borrowing from right ${if(target.isInstanceOf[Meta[K,V]]) "meta" else "leaf"}...${Console.RESET}\n")

          recursiveCopy(parent)

        } else {
          merge(target, pos, right, pos + 1, parent)
        }
      }

      case None => merge(left.get, pos - 1, target, pos, parent)
    }
  }

  protected def borrowLeft(target: Block[K, V], left: Option[(String, String)], right: Option[(String, String)], parent: Meta[K,V], pos: Int)
                                      (implicit ord: Ordering[K]): Future[Boolean] = {
    left match {
      case Some(id) => ctx.get(this.id, id).flatMap { l =>
        val left = l.copy()(this.ctx, this.id)

        if(left.canBorrowTo(target)){

          left.borrowLeftTo(target)(this.ctx, this.id)

          parent.setPointer(left, pos - 1)(this.ctx, this.id)
          parent.setPointer(target, pos)(this.ctx, this.id)

          logger.debug(s"${Console.GREEN_B}borrowing from left ${if(target.isInstanceOf[Meta[K,V]]) "meta" else "leaf"}...${Console.RESET}\n")

          recursiveCopy(parent)

        } else {
          borrowRight(target, Some(left), right, parent, pos)
        }
      }

      case None => borrowRight(target, None, right, parent, pos)
    }
  }

  protected def borrow(target: Block[K, V], parent: Meta[K,V], pos: Int)(implicit ord: Ordering[K]): Future[Boolean] = {

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

  protected def removeFromLeaf(target: Leaf[K,V], keys: Seq[K])(implicit ord: Ordering[K]): Future[Int] = {
    val result = target.remove(keys)

    if(result.isFailure) return Future.failed(result.failed.get)

    if(target.hasMinimum()){
      logger.debug(s"${Console.YELLOW_B}removing from leaf...${Console.RESET}\n")
      return recursiveCopy(target).map(_ => result.get)
    }

    val opt = ctx.getParent(this.id, target.unique_id)

    if(opt.isEmpty){
      return setPath(target).flatMap(_ => removeFromLeaf(target, keys))
    }

    val (p, pos) = opt.get

    if(p.isEmpty){

      if(target.isEmpty()){

        levels -= 1

        logger.debug(s"${Console.RED_B}[remove] TREE IS EMPTY${Console.RESET}")

        root = None
        return Future.successful(result.get)
      }

      return recursiveCopy(target).map(_ => result.get)
    }

    ctx.getMeta(id, p.get).flatMap { p =>
      borrow(target, p.copy()(this.ctx, this.id), pos).map(_ => result.get)
    }
  }

  def remove(keys: Seq[K])(implicit ord: Ordering[K]): Future[Int] = {
    val sorted = keys.distinct.sorted

    val len = sorted.length
    var pos = 0

    def remove(): Future[Int] = {
      if(pos == len) {
        num_elements -= sorted.length
        return Future.successful(sorted.length)
      }

      var list = sorted.slice(pos, len)
      val k = list(0)

      findPath(k).flatMap {
        case None => Future.failed(Errors.KEY_NOT_FOUND[K](k))
        case Some(leaf) =>

          val idx = list.indexWhere {k => ord.gt(k, leaf.last)}
          list = if(idx > 0) list.slice(0, idx) else list

          removeFromLeaf(leaf.copy()(this.ctx, this.id), list)
      }.flatMap { n =>
        pos += n
        remove()
      }
    }

    remove()
  }

  protected def updateLeaf(left: Leaf[K,V], data: Seq[Tuple[K,V]])(implicit ord: Ordering[K]): Future[Int] = {
    val result = left.update(data)

    if(result.isFailure) return Future.failed(result.failed.get)

    recursiveCopy(left).map(_ => result.get)
  }

  def update(data: Seq[Tuple[K,V]])(implicit ord: Ordering[K]): Future[Int] = {

    val sorted = data.sortBy(_._1)

    if(sorted.exists{case (k, _) => sorted.count{case (k1,_) => ord.equiv(k, k1)} > 1}){
      return Future.failed(Errors.DUPLICATE_KEYS(sorted))
    }

    val len = sorted.length
    var pos = 0

    def update(): Future[Int] = {
      if(len == pos) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _) = list(0)

      findPath(k).flatMap {
        case None => Future.failed(Errors.KEY_NOT_FOUND(k))
        case Some(leaf) =>

          val idx = list.indexWhere{case (k, _) => ord.gt(k, leaf.last)}
          if(idx > 0) list = list.slice(0, idx)

          updateLeaf(leaf.copy()(this.ctx, this.id), list)
      }.flatMap { n =>
        pos += n
        update()
      }
    }

    update()
  }

  /*def inOrder(start: Block[K,V]): Future[Seq[Tuple[K,V]]] = {
    start match {
      case leaf: Leaf[K,V] => Future.successful(leaf.inOrder())
      case meta: Meta[K,V] =>

        val pointers = meta.pointers
        val len = pointers.length

        var data = Seq.empty[(Int, Future[Seq[Tuple[K,V]]])]

        for(i<-0 until len){
          val (_, c) = pointers(i)

          //ctx.setParent(c, i, Some(meta))

          data = data :+ i -> ctx.get(c).flatMap{b => inOrder(b)}
        }

        //meta.setPointers()

        Future.sequence(data.sortBy(_._1).map(_._2)).map { results =>
          results.flatten
        }
    }
  }

  def inOrder(): Future[Seq[Tuple[K,V]]] = {
    ctx.root match {
      case None => Future.successful(Seq.empty[Tuple[K,V]])
      case Some(start) =>

        //ctx.setParent(start, 0, None)

        ctx.get(start).flatMap{b => inOrder(b)}
    }
  }*/

  def inOrder()(implicit ord: Ordering[K]): AsyncIterator[Seq[Tuple[K, V]]] = new AsyncIterator[Seq[(K, V)]] {

    var cur: Option[Leaf[K, V]] = None
    var firstTime = false

    override def hasNext(): Future[Boolean] = {
      if(!firstTime) return Future.successful(root.isDefined)
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
            b.inOrder()
        }
      }

      $this.next(cur.map(_.unique_id))(ord).map {
        case None =>
          cur = None
          Seq.empty[Tuple[K, V]]

        case Some(b) =>
          cur = Some(b)
          b.inOrder()
      }
    }
  }

  def reverse()(implicit ord: Ordering[K]): AsyncIterator[Seq[Tuple[K, V]]] = new AsyncIterator[Seq[(K, V)]] {

    var cur: Option[Leaf[K, V]] = None
    var firstTime = false

    override def hasNext(): Future[Boolean] = {
      if(!firstTime) return Future.successful(root.isDefined)
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
            b.inOrder().reverse
        }
      }

      $this.prev(cur.map(_.unique_id))(ord).map {
        case None =>
          cur = None
          Seq.empty[Tuple[K, V]]

        case Some(b) =>
          cur = Some(b)
          b.inOrder().reverse
      }
    }
  }

  def getLeftMost(start: Option[Block[K,V]]): Future[Option[Leaf[K,V]]] = {
    start match {
      case None => Future.successful(None)
      case Some(b) => b match {
        case b: Leaf[K,V] => Future.successful(Some(b))
        case b: Meta[K,V] =>

          b.setPointers()(this.ctx, this.id)

          ctx.get(this.id, b.pointers(0)._2.unique_id).flatMap(b => getLeftMost(Some(b)))
      }
    }
  }

  /**
   * To be able to get next or prev we have to set parents from root to the node (partial path)
   */
  def setPath(b: Block[K,V])(implicit ord: Ordering[K]): Future[Boolean] = {

    //It makes sure leaf node it is part of current root tree...
    if(!isFromCurrentContext(b)){
      return Future.failed(Errors.BLOCK_NOT_SAME_CONTEXT(b.root, root))
    }

    logger.debug(s"\nSETTING PATH...\n")

    findPath(b.last, Some(b)).map { _ =>
      true
    }
  }

  def first()(implicit ord: Ordering[K]): Future[Option[Leaf[K,V]]] = {
    if(this.root.isEmpty) return Future.successful(None)

    val root = this.root.get
    ctx.setParent(id, root, 0, None)

    /*if(ctx.isParentDefined(root)){
      return ctx.get(root).flatMap(opt => getLeftMost(opt))
    }

    ctx.get(root).flatMap{opt => setPath(opt.get).flatMap{ _ => getLeftMost(opt)}}*/

    ctx.get(this.id, root).flatMap(b => getLeftMost(Some(b)))
  }

  def getRightMost(start: Option[Block[K,V]]): Future[Option[Leaf[K,V]]] = {
    start match {
      case None => Future.successful(None)
      case Some(b) => b match {
        case b: Leaf[K,V] => Future.successful(Some(b))
        case b: Meta[K,V] =>

          b.setPointers()(this.ctx, this.id)

          ctx.get(this.id, b.pointers(b.pointers.length - 1)._2.unique_id).flatMap(b => getRightMost(Some(b)))
      }
    }
  }

  def last()(implicit ord: Ordering[K]): Future[Option[Leaf[K,V]]] = {
    if(this.root.isEmpty) return Future.successful(None)

    val root = this.root.get
    ctx.setParent(id, root, 0, None)

    /*if(ctx.isParentDefined(root)){
      return ctx.get(root).flatMap(opt => getRightMost(opt))
    }

    ctx.get(root).flatMap{opt => setPath(opt.get).flatMap{ _ => getRightMost(opt)}}*/

    ctx.get(this.id, root).flatMap(b => getRightMost(Some(b)))
  }

  def next(current: Option[(String, String)])(implicit ord: Ordering[K]): Future[Option[Leaf[K,V]]] = {

    def nxt(b: Block[K,V]): Future[Option[Leaf[K,V]]] = {

      val opt = ctx.getParent(this.id, b.unique_id)

      if(opt.isEmpty){
        return setPath(b).flatMap(_ => next(current))
      }

      val (p, pos) = opt.get

      p match {
        case None => Future.successful(None)
        case Some(pid) => ctx.getMeta(id, pid).flatMap { parent =>

          val pointers = parent.pointers
          val len = pointers.length

          parent.setPointers()(this.ctx, this.id)

          if (pos == len - 1) {
            nxt(parent)
          } else {
            ctx.get(this.id, pointers(pos + 1)._2.unique_id).flatMap(b => getLeftMost(Some(b)))
          }
        }
      }
    }

    current match {
      case None => first()
      case Some(current) => ctx.get(this.id, current).flatMap {nxt(_)}
    }
  }

  def prev(current: Option[(String, String)])(implicit ord: Ordering[K]): Future[Option[Leaf[K,V]]] = {

    def prv(b: Block[K,V]): Future[Option[Leaf[K,V]]] = {

      val opt = ctx.getParent(this.id, b.unique_id)

      if(opt.isEmpty){
        return setPath(b).flatMap(_ => prev(current))
      }

      val (p, pos) = opt.get

      p match {
        case None => Future.successful(None)
        case Some(pid) => ctx.getMeta(id, pid).flatMap { parent =>
          parent.setPointers()(this.ctx, this.id)

          if (pos == 0) {
            prev(Some(parent.unique_id))
          } else {
            ctx.get(this.id, parent.pointers(pos - 1)._2.unique_id).flatMap(b => getRightMost(Some(b)))
          }
        }
      }
    }

    current match {
      case None => first()
      case Some(current) => ctx.get(this.id, current).flatMap {prv(_)}
    }
  }

  def get(k: K)(implicit ord: Ordering[K]): Future[Option[Tuple[K,V]]] = {
    findPath(k).flatMap {
      case None => Future.successful(None)
      case Some(leaf) => Future.successful(leaf.find(k))
    }
  }

  def min()(implicit ord: Ordering[K]): Future[Option[Tuple[K,V]]] = {
    first().flatMap {
      case None => Future.successful(None)
      case Some(leaf) => Future.successful(leaf.min())
    }
  }

  def max()(implicit ord: Ordering[K]): Future[Option[Tuple[K,V]]] = {
    last().flatMap {
      case None => Future.successful(None)
      case Some(leaf) => Future.successful(leaf.max())
    }
  }

  def count(): Long = num_elements
  def getLevels(): Int = levels

  /*
   * Prints any subtree from the provided root
   * Caution: prettyPrint is currently synchronous!
   */
  def prettyPrint(root: Option[(String, String)] = this.root, timeout: Duration = Duration.Inf)(implicit kf: K => String, vf: V => String): (Int, Int) = {

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
            inOrder(Await.result(ctx.get(this.id, pointers(i)._2.unique_id), timeout), level + 1)
          }

      }
    }

    root match {
      case Some(id) => inOrder(Await.result(ctx.get(this.id, id), timeout), 0)
      case _ =>
    }

    logger.info("BEGIN BTREE:\n")
    levels.keys.toSeq.sorted.foreach { case level =>
      logger.info(s"level[$level]: ${levels(level).map(_.print())}\n")
    }
    logger.info("END BTREE\n")

    levels.size -> num_data_blocks
  }

  /*protected[index] def getNumLevels(root: Option[String] = ctx.root, timeout: Duration = Duration.Inf): Int = {

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
            inOrder(Await.result(ctx.get(pointers(i)._2), timeout), level + 1)
          }

      }
    }

    root match {
      case Some(id) => inOrder(Await.result(ctx.get(this.id), timeout), 0)
      case _ =>
    }

    levels.size
  }*/

  protected[index] def getNumLevels(root: Option[(String, String)] = this.root): Future[Int] = {

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
            tasks :+= ctx.get(this.id, pointers(i)._2.unique_id).flatMap(b => inOrder(b, level + 1))
          }

          Future.sequence(tasks).map(_ => {})

      }
    }

    root match {
      case Some(id) => ctx.get(this.id, id).flatMap(b => inOrder(b, 0)).map(_ => levels.size)
      case _ => Future.successful(0)
    }
  }

  def execute(cmds: Seq[Command[K, V]]): Future[Boolean] = {
    def process(pos: Int, previous: Boolean): Future[Boolean] = {

      if(!previous) return Future.successful(false)
      if(pos == cmds.length) return Future.successful(true)

      val cmd = cmds(pos)

      (cmd match {
        case cmd: Insert[K, V] => insert(cmd.list).map(_ == cmd.list.length)
        case cmd: Remove[K, V] => remove(cmd.keys).map(_ == cmd.keys.length)
        case cmd: Update[K, V] => update(cmd.list).map(_ == cmd.list.length)
      }).flatMap(ok => process(pos + 1, ok))
    }

    process(0, true)
  }

}