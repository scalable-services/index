package services.scalable.index

object Commands {

  trait Command[K, V] {
    val indexId: String
    val version: Option[String] = None
  }

  case class Insert[K, V](override val indexId: String, list: Seq[Tuple3[K, V, Boolean]],
                          override val version: Option[String] = None) extends Command[K, V]
  case class Remove[K, V](override val indexId: String, keys: Seq[Tuple2[K, Option[String]]],
                          override val version: Option[String] = None) extends Command[K, V]
  case class Update[K, V](override val indexId: String, list: Seq[Tuple3[K, V, Option[String]]],
                          override val version: Option[String] = None) extends Command[K, V]

}
