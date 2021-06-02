package services.scalable.index

trait Serializer[T] {

  def serialize(t: T): Bytes
  def deserialize(b: Bytes): T
  
}
