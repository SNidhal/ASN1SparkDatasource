package asn1V1

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import util.Util
import compiler.InferSchema
import hadoopIO.RawFileAsBinaryInputFormat

case class ASN1DatasourceRelation(override val sqlContext: SQLContext, path: String, userSchema: StructType, defPath: String)
  extends BaseRelation with TableScan with PrunedScan with Serializable {


  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      inferSchema(defPath)
    }
  }


  override def buildScan(): RDD[Row] = {


    val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)

    val FileRDD: RDD[(LongWritable, Text)] = sqlContext.sparkContext
      .newAPIHadoopFile(path, classOf[RawFileAsBinaryInputFormat], classOf[LongWritable], classOf[Text], conf)

    val encodedRecordsRDD: RDD[Text] = FileRDD.map(x => x._2)

    val decodedRecordsRDD = encodedRecordsRDD.map((encodedLine: Text) => {

      Asn1Parser.decodeRecord(encodedLine, "GenericCallDataRecord", schema)

    })
    decodedRecordsRDD.map(s => Row.fromSeq(s))

  }


  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {

    val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
    conf.set("mapred.max.split.size", "5")
    val FileRDD: RDD[(LongWritable, Text)] = sqlContext.sparkContext
      .newAPIHadoopFile(path, classOf[RawFileAsBinaryInputFormat], classOf[LongWritable], classOf[Text], conf)


    val encodedLinesRDD: RDD[Text] = FileRDD.map(x => x._2)

    val decodedLinesRDD = encodedLinesRDD.map(encodedLine => Asn1Parser.decodeRecord(encodedLine, "Human", schema))

    val filteredDecodedLinesRDD = decodedLinesRDD.map(s => Util.rearrangeSequence(requiredColumns, s, schema)).map(s => s.filter(_ != " ")).map(s => Row.fromSeq(s))

    filteredDecodedLinesRDD

  }

  private def inferSchema(path: String): StructType = {
    val inferredSchema = InferSchema.getInferredSchema(path)
    InferSchema.inferredSchema = new StructType
    inferredSchema
  }


}
