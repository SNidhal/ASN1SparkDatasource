package reader

import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer

object JsonSchemaParser {

  def createStruct(myObject: String): DataType = {
    myObject match {
      case "String" => StringType
      case "Integer" => IntegerType
      case "Boolean" => BooleanType
      case "REAL" => DoubleType
      case "DATE" => DateType
      case "Long" => LongType
      case "Float" => FloatType
      case "Bit" => ByteType
      case "Null" => NullType
      case "Time" => TimestampType
      case "Short" => ShortType
    }
  }

  def JsonSourceFileToStructType(FilePath: String): StructType = {
    val dataFile = scala.io.Source.fromFile(FilePath)
    val sourceFileString = dataFile.mkString
    val list = ListBuffer.empty[StructField]
    sourceFileString.split(",").foreach(
      line => {
        val key = line.substring(line.indexOf("\"", 1) + 1, line.indexOf("\"", line.indexOf("\"", 1) + 1))
        val value = line.substring(line.indexOf("\"", line.indexOf(":")) + 1, line.indexOf("\"", line.indexOf("\"", line.indexOf(":")) + 1))
        list += StructField(key.toString, createStruct(value), false)
      }
    )
    val schema = StructType(list.toList)
    schema
  }
}
