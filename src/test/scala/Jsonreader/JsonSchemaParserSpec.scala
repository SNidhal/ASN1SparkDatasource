package Jsonreader

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, StructField, StructType}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import reader.JsonSchemaParser

import scala.io.BufferedSource

class JsonSchemaParserSpec extends FlatSpec with GivenWhenThen with Matchers {


  val schema = StructType(
    StructField("dateDuJour", DateType, false) ::
      StructField("size", DoubleType, false) ::
      StructField("state", BooleanType, false) :: Nil

  )


  "parseJson" should "parse json input in Map[String,String] format" in {
    Given("a json buffereSource")
    val json1 = """{ "dateDuJour": "DATE", "size": "REAL", "state": "Boolean" }"""
    val jsoninput = new ByteArrayInputStream(json1.getBytes(StandardCharsets.UTF_8))
    val jsonBufferedSource = new BufferedSource(jsoninput)
    When("parseJson is invoked")
    val resultmap = JsonSchemaParser.parseJson(jsonBufferedSource)
    Then("a Map[String,String] object should be returned")
    resultmap should equal(Map("dateDuJour" -> "DATE", "size" -> "REAL", "state" -> "Boolean"))

  }


  "MapToStructType" should "convert json input to StructType Object" in {
    Given("a json Map")
    val mapsample = Map("dateDuJour" -> "DATE", "size" -> "REAL", "state" -> "Boolean")

    When("MapToStructType is invoked")

    val resultstructtypeobject = reader.JsonSchemaParser.MapToStructType(mapsample)

    Then("a StructType Object should be returned")

    resultstructtypeobject should equal(schema)

  }



  "JsonSourceFileToStructType" should "take a json file path and return a StructType Object" in {

    import reader._
    Given("a json file path")
    val filePath = "src/test/resources/example_1.json"
    When("JsonSourceFileToStructType is invoked")
    val structtypeschema = JsonSchemaParser.JsonSourceFileToStructType(filePath)
    Then("a StructType Object should be returned")
    structtypeschema should equal(schema)


  }




}
