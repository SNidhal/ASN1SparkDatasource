package Jsonreader

import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, StructField, StructType}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class JsonSchemaParserSpec extends FlatSpec with GivenWhenThen with Matchers {


  val schema = StructType(
    StructField("dateDuJour", DateType, false) ::
      StructField("size", DoubleType, false) ::
      StructField("state", BooleanType, false) :: Nil

  )

  "JsonSourceFileToStructType" should "take a json file path and return a StructType Object" in {

    import reader._
    Given("a json file path")
    val filePath = "src/test/resources/example_1.json"
    When("JsonSourceFileToStructType is invoked")
    val structTypeSchema = JsonSchemaParser.JsonSourceFileToStructType(filePath)
    Then("a StructType Object should be returned")
    structTypeSchema should equal(schema)

  }




}
