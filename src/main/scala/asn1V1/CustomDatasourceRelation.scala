package asn1V1

import java.io.{ByteArrayInputStream, InputStream}


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.bouncycastle.asn1.{ ASN1InputStream, ASN1Primitive, ASN1Sequence, DERTaggedObject, DLApplicationSpecific}
import util.Util

import compiler.InferSchema
import hadoopIO.RawFileAsBinaryInputFormat

case class CustomDatasourceRelation(override val sqlContext : SQLContext, path : String, userSchema : StructType,defPath:String)
  extends BaseRelation with TableScan with  PrunedScan with Serializable {

  val currentSchema:StructType =inferSchema(defPath)

  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      currentSchema
    }
  }


  override def buildScan(): RDD[Row] = {


    val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
    val FileRDD: RDD[(LongWritable, Text)] = sqlContext.sparkContext.newAPIHadoopFile(path, classOf[RawFileAsBinaryInputFormat], classOf[LongWritable], classOf[Text], conf)


    val  encodedRecordsRDD:RDD[String] = FileRDD.map(x=>x._2.toString)

    val decodedRecordsRDD = encodedRecordsRDD.map((encodedLine: Any) => {

      decodeRecord(encodedLine)

    })
    decodedRecordsRDD.flatMap(x=>x)


  }


  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {

    val schemaFields = currentSchema.fields


    val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
    val FileRDD: RDD[(LongWritable, Text)] = sqlContext.sparkContext.newAPIHadoopFile(path, classOf[RawFileAsBinaryInputFormat], classOf[LongWritable], classOf[Text], conf)



    val  encodedLinesRDD:RDD[String] = FileRDD.map(x=>x._2.toString)

    val decodedLinesRDD = encodedLinesRDD.map((x: Any) => {
      def decodeLine(x: Any) = {
        val inputStream = new ByteArrayInputStream(x.asInstanceOf[String].getBytes)
        val asn1InputStream: ASN1InputStream = new ASN1InputStream(inputStream)
        var asn1PrimitiveObject : ASN1Primitive = null
        var rowArray= Array[Seq[Any]]()
        while ({asn1PrimitiveObject = asn1InputStream.readObject;asn1PrimitiveObject!=null}) {
          rowArray=rowArray :+ Util.asn1SequenceToSequence(asn1PrimitiveObject.asInstanceOf[ASN1Sequence])
        }
        asn1InputStream.close()
        rowArray=rowArray.map(x=>x.zipWithIndex.map({case (value, index) =>
          val colName = schemaFields(index).name
          val castedValue = value

          if (requiredColumns.contains(colName)) Some(castedValue) else " "
        }))

        rowArray.map(s=>Util.rearrangeSequence(requiredColumns,s,currentSchema)).map(s=>s.filter(_!=" ")).map(s => Row.fromSeq(s))
      }


      decodeLine(x)

    })
    decodedLinesRDD.flatMap(x=>x)

  }

  private def inferSchema(path:String): StructType = {
    val res=InferSchema.getInferredSchema(path)
    InferSchema.inferredSchema = new StructType
    res
  }

  def decodeRecord(encodedRocord : Any) = {
    val inputStream = new ByteArrayInputStream(encodedRocord.asInstanceOf[String].getBytes)
    val asn1InputStream: ASN1InputStream = new ASN1InputStream(inputStream)
    var asn1PrimitiveObject : ASN1Primitive = null
    var rowArray= Array[Seq[Any]]()
    while ({asn1PrimitiveObject = asn1InputStream.readObject;asn1PrimitiveObject!=null}) {
      println(asn1InputStream.read())
      rowArray=rowArray :+ Util.asn1SequenceToSequence(asn1PrimitiveObject.asInstanceOf[ASN1Sequence])
    }
    asn1InputStream.close()
    rowArray.map(s => Row.fromSeq(s))

  }


}
