package services.scalable.index

sealed trait Result {
  val success: Boolean
  val error: Option[Throwable]
}

case class GetResult[K, V](override val success: Boolean, data: Seq[Tuple[K, V]], override val error: Option[Throwable] = None) extends Result

case class InsertionResult(override val success: Boolean, n: Int, override val error: Option[Throwable] = None) extends Result
case class UpdateResult(override val success: Boolean, n: Int, override val error: Option[Throwable] = None) extends Result
case class RemovalResult(override val success: Boolean, n: Int, override val error: Option[Throwable] = None) extends Result

case class BatchResult(override val success: Boolean, error: Option[Throwable] = None) extends Result
