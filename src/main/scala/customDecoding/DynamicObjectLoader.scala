package customDecoding

object DynamicObjectLoader {
  def getObject(qualifiedObjectName: String): Decoder = {
    val clazz = Class.forName(qualifiedObjectName + "$")
    clazz.getField("MODULE$").get(clazz).asInstanceOf[Decoder]
  }
}
