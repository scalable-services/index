package services.scalable.index

trait TreeOrdering[K] {

  def compare(x: K, y: K, isFatherOfLeaf: Boolean = false): Int

}
