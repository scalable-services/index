package services.scalable.index

import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.RichAsyncIterator

import scala.concurrent.Future

/**
 * All 'term' parameters in the functions should be provided along the prefix.
 * If the prefix is important in the searching, it should be explicitly provided with the optional parameter, i.e.,
 * fromPrefix.
 * order[T].compare(k, term): the first parameter of the compare function is the key being compared. The second one is
 * the pattern to be compared to.
 */
class QueryableIndexTest[K, V](override val descriptor: IndexContext)(override val builder: IndexBuilder[K, V])
  extends Index[K, V](descriptor)(builder) {

  override val $this = this

  import builder._

}
