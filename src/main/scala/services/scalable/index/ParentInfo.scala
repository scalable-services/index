package services.scalable.index

case class ParentInfo[K](var parent: Option[(String, String)] = None,
                      var key: Option[K] = None,
                      var pos: Int = 0)
