package services.scalable.index

import scala.concurrent.Future

trait AsyncIterator[T] {

  def hasNext(): Future[Boolean]
  def next(): Future[T]

}
