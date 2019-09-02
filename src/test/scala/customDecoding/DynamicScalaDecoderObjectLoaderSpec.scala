package customDecoding

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class DynamicScalaDecoderObjectLoaderSpec extends FlatSpec with GivenWhenThen with Matchers{


  "getDecoderObject" should "dynamically return a scala decoder object using it's fully qualified name " in {
    Given("an decoder object fully qualified name")
    val scalaDecoderQualifiedName="customDecoding.CustomScalaDecoder"
    When("getObject is invoked")
    val scalaDecoderObject=DynamicScalaDecoderObjectLoader.getDecoderObject(scalaDecoderQualifiedName)
    Then("the decoder Object should be returned")
    scalaDecoderObject.getClass should equal(CustomScalaDecoder.getClass)

  }

}
