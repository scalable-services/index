package services.scalable.index

object Commands {

  trait Command[K, V] {
    val indexId: String
  }

  case class Insert[K, V](override val indexId: String, list: Seq[Tuple3[K, V, Boolean]]) extends Command[K, V]
  case class Remove[K, V](override val indexId: String, keys: Seq[Tuple2[K, Option[String]]]) extends Command[K, V]
  case class Update[K, V](override val indexId: String, list: Seq[Tuple3[K, V, Option[String]]]) extends Command[K, V]

}
