package reader
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import scala.io.{BufferedSource, Source}


object JsonSchemaParser {

  def parseJson(json: BufferedSource): Map[String, String] = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue[Map[String, String]](json.reader())

  }

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

  def MapToStructType(myMap : Map[String,String]): StructType = {

    val list = ListBuffer.empty[StructField]
    myMap.foreach { x => {
      list += StructField(x._1.toString, createStruct(x._2), false)
    }
    }
    val schema = StructType(list.toList)
    schema
  }

  def loadFile(FilePath : String): StructType = {

    val fileToload = parseJson(Source.fromFile(FilePath))
    MapToStructType(fileToload)
  }

}
