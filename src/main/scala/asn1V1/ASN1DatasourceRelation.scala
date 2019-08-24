package asn1V1

import customDecoding.DynamicObjectLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import util.Util
import hadoopIO.RawFileAsBinaryInputFormat
import reader.{AsnSchemaParser, JsonSchemaParser}

case class ASN1DatasourceRelation(override val sqlContext: SQLContext, schemaFileType: String, path: String
                                  , userSchema: StructType, schemaFilePath: String, customDecoder: String
                                  , customDecoderLanguage: String)
  extends BaseRelation with TableScan with PrunedScan with Serializable {


  var currentSchema: StructType = _
  var initialSchema: StructType = _

  {
    if (userSchema != null) {
      currentSchema = StructType(Asn1Parser.flatten(userSchema))
      initialSchema = userSchema
    } else {
      initialSchema = inferSchema(schemaFilePath)
      currentSchema = StructType(Asn1Parser.flatten(initialSchema))

    }
  }


  override def schema: StructType = currentSchema


  override def buildScan(): RDD[Row] = {
    val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
    val FileRDD: RDD[(LongWritable, Text)] = sqlContext.sparkContext
      .newAPIHadoopFile(path, classOf[RawFileAsBinaryInputFormat], classOf[LongWritable], classOf[Text], conf)
    val encodedRecordsRDD: RDD[Text] = FileRDD.map(x => x._2)
    val decodedRecordsRDD = encodedRecordsRDD.map((encodedLine: Text) => {
      Asn1Parser.decodeRecord(encodedLine, currentSchema, true)
    })
    decodedRecordsRDD.map(s => Row.fromSeq(s))
  }


  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
    val FileRDD: RDD[(LongWritable, Text)] = sqlContext.sparkContext
      .newAPIHadoopFile(path, classOf[RawFileAsBinaryInputFormat], classOf[LongWritable], classOf[Text], conf)
    val encodedLinesRDD: RDD[Text] = FileRDD.map(x => x._2)
    val decodedLinesRDD = encodedLinesRDD.map(encodedLine => {
      if (customDecoder.equals("none")) {
        try {
          Asn1Parser.decodeRecord(encodedLine, initialSchema, true)
        }
        catch {
          case _: Exception => Seq()
        }
      } else {
        if (customDecoderLanguage.equals("java")) {
          val customDecoderClassInstance = Class.forName(customDecoder).newInstance
          val decodeMethod = customDecoderClassInstance.getClass.getMethod("decode", encodedLine.getClass,initialSchema.getClass)
          decodeMethod.invoke(customDecoderClassInstance, encodedLine,initialSchema).asInstanceOf[Seq[Any]]
        } else {
          DynamicObjectLoader.getObject(customDecoder).decode(encodedLine, initialSchema)
        }
      }
    })
    val filteredDecodedLinesRDD = decodedLinesRDD
      .map(s => Util.rearrangeSequence(requiredColumns, s, currentSchema))
      .map(s => s.filter(_ != " "))
      .map(s => Row.fromSeq(s))
    filteredDecodedLinesRDD
  }

  private def inferSchema(path: String): StructType = {
    var inferredSchema: StructType = null
    if (schemaFileType == "asn") {
      inferredSchema = AsnSchemaParser.getParsedSchema(path)
    } else if (schemaFileType == "json") {
      inferredSchema = JsonSchemaParser.JsonSourceFileToStructType(path)
    }
    inferredSchema
  }


}
