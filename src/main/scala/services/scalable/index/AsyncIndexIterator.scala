package services.scalable.index

import scala.concurrent.Future

trait AsyncIndexIterator[T] {

  def hasNext(): Future[Boolean]
  def next(): Future[T]

}
