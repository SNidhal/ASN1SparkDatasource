package util

import java.io.ByteArrayInputStream

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.bouncycastle.asn1.{ASN1InputStream, ASN1Primitive, ASN1Sequence}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class UtilTest extends FlatSpec with GivenWhenThen with Matchers {

  val schema = StructType(
    StructField("recordNumber", StringType, false) ::
      StructField("callingNumber", StringType, true) ::
      StructField("calledNumber", StringType, true) ::
      StructField("StartDate", StringType, true) ::
      StructField("StartTime", StringType, true) ::
      StructField("Duration", StringType, true) :: Nil

  )
  val rearrangedSequence = Seq(65, 1, " ", " ", " ", " ")


  "rearrangeSequence" should "rearrange the sequence elements in a given order" in {
    Given("a sequence and an order")
    val initialSequence = Seq(1, " ", " ", " ", " ", 65)
    val order = Array("Duration", "recordNumber")

    When("rearrangeSequence is invoked")

    val resultSequence = Util.rearrangeSequence(order, initialSequence, schema)

    Then("a rearranged sequence in the given order should be returned")
    resultSequence should equal(rearrangedSequence)

  }

  val byteArray :Array[Byte] = Array[Byte](48,50,66,1,1,72,11,49,53,53,53,53,53,53,48,49,48,48,73,11,49,53,
    53,53,53,53,53,48,49,48,49,80,8,50,48,49,51,49,48,49,54,82,6,49,51,52,53,51,52,83,1,65)
 val convertedSequence =Seq("1", "15555550100", "15555550101", "20131016", "134534", "65")
  val inputStream = new ByteArrayInputStream(byteArray)
  val asn1InputStream: ASN1InputStream = new ASN1InputStream(inputStream)
  var asn1PrimitiveObject : ASN1Primitive = null
  asn1PrimitiveObject=asn1InputStream.readObject

  "asn1SequenceToSequence" should "Convert an ASN1 sequence to a normal sequence" in {
    Given("an ASN1 sequence")
    val asnSequence : ASN1Sequence = asn1PrimitiveObject.asInstanceOf[ASN1Sequence]

    When("asn1SequenceToSequence is invoked")

    val resultSequence = Util.asn1SequenceToSequence(asnSequence)

    Then("a normal sequence should be returned")
    resultSequence should equal(convertedSequence)

  }

}
