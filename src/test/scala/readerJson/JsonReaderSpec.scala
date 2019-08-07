package readerJson

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, StructField, StructType}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import reader.JsonReader

import scala.io.BufferedSource

class JsonReaderSpec extends FlatSpec with GivenWhenThen with Matchers {


  val schema1 = StructType(
    StructField("dateDuJour", DateType, false) ::
      StructField("size", DoubleType, false) ::
      StructField("state", BooleanType, false) :: Nil

  )



  "parseWithJackson" should "parse json input in Map[String,String] format" in {
    Given("a json buffereSource")
    val json1 = """{ "dateDuJour": "DATE", "size": "REAL", "state": "Boolean" }"""
    val inputstrea = new ByteArrayInputStream(json1.getBytes(StandardCharsets.UTF_8))
    val jsonBufferedSource = new BufferedSource(inputstrea)
    When("parseWithJackson is invoked")
    val resultmap = JsonReader.parseJson(jsonBufferedSource)
    Then("a Map[String,String] format should be returned")
    resultmap should equal(Map("dateDuJour" -> "DATE", "size" -> "REAL", "state" -> "Boolean"))

  }


  "MapToStructType" should "convert json input to StructType Object" in {
    Given("a json Map")
    val jsonmapformat = Map("dateDuJour" -> "DATE", "size" -> "REAL", "state" -> "Boolean")

    When("MapToStructType is invoked")

    val resultsequence = reader.JsonReader.MapToStructType(jsonmapformat)

    Then("a StructType Object should be returned")

    resultsequence should equal(schema1)

  }



  "loadFile" should "take a json file path and return a StructType Object" in {

    import reader._
    Given("a json file path")
    val filePath = "src/test/resources/example_1.json"
    When("loadFile is invoked")
    val resultObject = JsonReader.loadFile(filePath)
    Then("a StructType Object should be returned")
    resultObject should equal(schema1)


  }




}
