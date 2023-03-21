package services.scalable.index

object Errors {

  sealed trait IndexError
  sealed trait TemporalIndexError extends IndexError

  case object LEAF_BLOCK_FULL extends RuntimeException("Leaf is full!") with IndexError
  case object META_BLOCK_FULL extends RuntimeException("Meta is full!") with IndexError

  case class LEAF_DUPLICATE_KEY[K, V](duplicates: Seq[K], ks: K => String) extends RuntimeException(s"Duplicate elements on leaf: ${duplicates.map(ks)}!")
    with IndexError
  case class LEAF_KEY_NOT_FOUND[K](keys: Seq[K], ks: K => String) extends RuntimeException(s"Missing key(s) on leaf: ${keys.map(ks)}") with IndexError

  case class META_DUPLICATE_KEY[K](duplicates: Seq[K], ks: K => String) extends RuntimeException(s"Duplicate elements on meta: ${duplicates.map(ks)}!")
    with IndexError
  case class META_KEY_NOT_FOUND[K](keys: Seq[K], ks: K => String) extends RuntimeException(s"Missing key(s) on meta: ${keys.map(ks)}!") with IndexError

  case class BLOCK_NOT_FOUND(id: String) extends RuntimeException(s"Block ${id} not found!") with IndexError

  case class DUPLICATED_KEYS[K,V](keys: Seq[K], ks: K => String) extends RuntimeException(s"Duplicate keys: ${keys.map(ks)}") with IndexError

  case class KEY_NOT_FOUND[K](k: K, ks: K => String) extends RuntimeException(s"Key not found: ${ks(k)}!") with IndexError

  case class BLOCK_NOT_SAME_CONTEXT(broot: Option[(String, String)], croot: Option[(String, String)])
    extends RuntimeException(s"Current block's root ${broot} is not equal to the current root context: ${croot}") with IndexError

  case class INDEX_NOT_FOUND(id: String) extends RuntimeException(s"Index ${id} not found!") with IndexError
  case class TEMPORAL_INDEX_NOT_FOUND(id: String) extends RuntimeException(s"Temporal Index ${id} not found!") with TemporalIndexError

  case class INDEX_CREATION_ERROR(id: String) extends RuntimeException(s"There was a problem creating index ${id}!") with IndexError

  case class INDEX_ALREADY_EXISTS(id: String) extends RuntimeException(s"Index ${id} already exists!") with IndexError
  case class TEMPORAL_INDEX_ALREADY_EXISTS(id: String) extends RuntimeException(s"Temporal Index ${id} already exists!") with TemporalIndexError

  case class VERSION_CHANGED[K, V](changes: Seq[Tuple2[K, Option[String]]], ks: K => String) extends RuntimeException(s"Key version for ${changes.map{case (k, vs) => ks(k) -> vs}} has changed!") with IndexError
}
