package largeFileGeneration

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class GeneratorSpec extends FlatSpec with GivenWhenThen with Matchers{


  "generateFile" should "generate an asn.1 encoded file " in {

    Given("a destination path and the number of records wanted")

    val destinationPath = "testgen2.ber"
    val recordNumber = 99

    When("generateFile is invoked")

    Generator.generateFile(destinationPath,recordNumber)

    Then("an asn.1 encoded file should be generated at the given path with the number of records wanted")




  }

}
