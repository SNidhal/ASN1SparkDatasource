package asn1V1

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import org.apache.hadoop.io.Text

class Asn1ParserSpec extends FlatSpec with GivenWhenThen with Matchers {


  val resultSequence: Seq[Any] = Seq(1, "15555550100", "15555550101", "20131016", "134534", 65)


  "decodeRecord" should "decode an asn.1 encoded record" in {

    Given("an encoded record , the name of the generated class and a schema")

    val schema = StructType(
      StructField("recordNumber", IntegerType, false) ::
        StructField("callingNumber", StringType, true) ::
        StructField("calledNumber", StringType, true) ::
        StructField("startDate", StringType, true) ::
        StructField("startTime", StringType, true) ::
        StructField("duration", IntegerType, true) :: Nil

    )


    val className = "GenericCallDataRecord"

    val recordBytes = Array[Byte](48, 50, 66, 1, 1, 72, 11, 49, 53, 53, 53, 53, 53, 53, 48, 49, 48, 48, 73, 11, 49, 53, 53, 53, 53, 53, 53
      , 48, 49, 48, 49, 80, 8, 50, 48, 49, 51, 49, 48, 49, 54, 82, 6, 49, 51, 52, 53, 51, 52, 83, 1, 65, 48)


    val record = new Text(recordBytes)

    When("decodeRecord is invoked")

    val recordSequence = Asn1Parser.decodeRecord(record, className, schema)

    Then("a sequence of the record decoded fields should be returned")

    recordSequence should equal(resultSequence)

  }

}
