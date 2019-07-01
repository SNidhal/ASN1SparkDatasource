package asn1V1

import asn1V1.CustomDatasourceRelation
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import util.Util


class CustomDatasourceRelationTest  extends FlatSpec with GivenWhenThen with Matchers{

  val sch =StructType(
    StructField("recordNumber", StringType, false) ::
      StructField("callingNumber", StringType, true) ::
      StructField("calledNumber", StringType, true) ::
      StructField("StartDate", StringType, true) ::
      StructField("StartTime", StringType, true) ::
      StructField("Duration", StringType, true) ::Nil

  )
  val rearrangedSequence =Seq(65,1," "," "," "," ")


  "rearrangeSequence" should "rearrange the sequence elements in a given order" in {
    Given("a sequence and an order")
    val seqencetest=Seq(1," "," "," "," ",65)
    val order = Array("Duration", "recordNumber")

    When("rearrangeSequence is invoked")

    val resultSequence = Util.rearrangeSequence(order,seqencetest,sch)

    Then("a rearranged sequence in the given order should be returned")
    resultSequence should equal(rearrangedSequence)

  }

}
