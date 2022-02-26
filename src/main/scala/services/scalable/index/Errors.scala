package services.scalable.index

object Errors {

  sealed trait IndexError

  case object LEAF_BLOCK_FULL extends RuntimeException("Leaf is full!") with IndexError
  case object META_BLOCK_FULL extends RuntimeException("Meta is full!") with IndexError

  case class LEAF_DUPLICATE_KEY[K,V](keys: Seq[Tuple[K,V]], inserting: Seq[Tuple[K,V]]) extends RuntimeException(s"Duplicate elements on leaf!")
    with IndexError
  case class LEAF_KEY_NOT_FOUND[K](keys: Seq[K]) extends RuntimeException(s"Missing key on leaf") with IndexError

  case class META_DUPLICATE_KEY[K](keys: Seq[Pointer[K]], inserting: Seq[Pointer[K]]) extends RuntimeException(s"Duplicate elements on meta!")
    with IndexError
  case class META_KEY_NOT_FOUND[K](keys: Seq[K]) extends RuntimeException(s"Missing key on meta") with IndexError

  case class BLOCK_NOT_FOUND(id: String) extends RuntimeException(s"Block ${id} not found!") with IndexError

  case class DUPLICATE_KEYS[K,V](keys: Seq[Tuple[K,V]]) extends RuntimeException("Duplicate keys") with IndexError

  case class KEY_NOT_FOUND[K](k: K) extends RuntimeException(s"Key not found!") with IndexError

  case class BLOCK_NOT_SAME_CONTEXT(broot: Option[String], croot: Option[String])
    extends RuntimeException(s"Current block's root ${broot} is not equal to the current root context: ${croot}") with IndexError

  case class INDEX_NOT_FOUND(id: String) extends RuntimeException(s"Index ${id} not found!") with IndexError

  case class INDEX_CREATION_ERROR(id: String) extends RuntimeException(s"There was a problem creating index ${id}!") with IndexError

  case class INDEX_ALREADY_EXISTS(id: String) extends RuntimeException(s"Index ${id} already exists!") with IndexError

}