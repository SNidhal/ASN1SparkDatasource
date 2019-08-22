package hadoopIO

import util.SeekableByteArrayInputStream

import org.apache.hadoop.fs.FSDataInputStream
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class MyFileRecordReaderSpec extends FlatSpec with GivenWhenThen with Matchers {


  "findRecordStart" should "find the index of the starting byte of a record within an input stream" in {
    Given("an input stream , the beginning and the end index of an input split  and a precision factor")

    val recordBytes = Array[Byte](20,48, 3, 48, 7, 1, 48, 2, 49, 53, 53, 53, 48, 1, 53,48)

    val inputSplitStartIndex=0
    val inputSplitEndIndex=15
    val precisionFactor=2
    val inputStream :SeekableByteArrayInputStream  = new SeekableByteArrayInputStream(recordBytes)
    val is : FSDataInputStream= new FSDataInputStream(inputStream)

    When("findRecordStart is invoked")

   val recordStartIndex = new MyFileRecordReader().findRecordStart(is,inputSplitStartIndex,inputSplitEndIndex,precisionFactor)

    Then("the index of the record start should be returned")

    recordStartIndex should equal(3)
  }

  "precisionCheck" should "check if the next record starts are right with a precision factor " in {
    Given("a precision factor, a file system input stream and an initial index")

    When("precisionCheck is invoked")


    Then("the index of the last record start according to the precision factor ")
  }

}
