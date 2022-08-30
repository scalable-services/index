package services.scalable.index

object Commands {

  trait Command[K, V] {
    val id: String
  }

  case class Insert[K, V](override val id: String, list: Seq[(K, V)]) extends Command[K, V]
  case class Remove[K, V](override val id: String, keys: Seq[K]) extends Command[K, V]
  case class Update[K, V](override val id: String, list: Seq[(K, V, String)]) extends Command[K, V]

}
