package services.scalable.index

object Commands {

  trait Command[K, V]
  case class Insert[K, V](list: Seq[(K, V)]) extends Command[K, V]
  case class Remove[K, V](keys: Seq[K]) extends Command[K, V]
  case class Update[K, V](list: Seq[(K, V)]) extends Command[K, V]

}
