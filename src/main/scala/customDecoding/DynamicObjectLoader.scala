package customDecoding

object DynamicObjectLoader {
  def getObject(qualifiedObjectName: String): ScalaDecoder = {
    val clazz = Class.forName(qualifiedObjectName + "$")
    clazz.getField("MODULE$").get(clazz).asInstanceOf[ScalaDecoder]
  }
}
