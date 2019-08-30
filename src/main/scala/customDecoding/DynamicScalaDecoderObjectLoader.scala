package customDecoding

object DynamicScalaDecoderObjectLoader {
  def getDecoderObject(qualifiedObjectName: String): ScalaDecoder = {
    val clazz = Class.forName(qualifiedObjectName + "$")
    clazz.getField("MODULE$").get(clazz).asInstanceOf[ScalaDecoder]
  }
}
