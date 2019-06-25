package asn1V1

import java.io.{ByteArrayInputStream, InputStream}
import java.util.List

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.function.Function
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.bouncycastle.asn1.{ASN1InputStream, ASN1Primitive, ASN1Sequence}
import util.Util
import java.util

import hadoopIO.RawFileAsBinaryInputFormat
import model.{CallDetailRecord, CallDetailRecord2}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema


class CustomDatasourceRelation(override val sqlContext : SQLContext, path : String, userSchema : StructType)
  extends BaseRelation with TableScan with  PrunedScan with Serializable {

  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      StructType(
        StructField("recordNumber", StringType, false) ::
        StructField("callingNumber", StringType, true) ::
        StructField("calledNumber", StringType, true) ::
          StructField("StartDate", StringType, true) ::
          StructField("StartTime", StringType, true) ::
        StructField("Duration", StringType, true) ::Nil

      )
    }
  }


  override def buildScan(): RDD[Row] = {


    val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
    val FileRDD: RDD[(LongWritable, Text)] = sqlContext.sparkContext.newAPIHadoopFile(path, classOf[RawFileAsBinaryInputFormat], classOf[LongWritable], classOf[Text], conf)


    val  encodedLinesRDD:RDD[String] = FileRDD.map(x=>x._2.toString)

    val decodedLinesRDD = encodedLinesRDD.map((encodedLine: Any) => {
      def decodeLine(x: Any) = {
        val inputStream = new ByteArrayInputStream(x.asInstanceOf[String].getBytes)
        val asn1InputStream: ASN1InputStream = new ASN1InputStream(inputStream)
        var asn1PrimitiveObject : ASN1Primitive = null
        var callDetailRecord : CallDetailRecord= null
        var rowArray= Array[Row]()
        while ({asn1PrimitiveObject = asn1InputStream.readObject;asn1PrimitiveObject!=null}) {
          callDetailRecord = new CallDetailRecord(asn1PrimitiveObject.asInstanceOf[ASN1Sequence])
          rowArray=rowArray :+ Row.fromSeq(Seq(callDetailRecord.getRecordNumber,callDetailRecord.getCallingNumber,callDetailRecord.getCalledNumber,callDetailRecord.getStartDate,callDetailRecord.getStartTime,callDetailRecord.getDuration))
        }
        asn1InputStream.close()
        rowArray
      }

      decodeLine(encodedLine)

    })
    decodedLinesRDD.flatMap(x=>x)


  }


  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {

    val schemaFields = schema.fields


    val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
    val FileRDD: RDD[(LongWritable, Text)] = sqlContext.sparkContext.newAPIHadoopFile(path, classOf[RawFileAsBinaryInputFormat], classOf[LongWritable], classOf[Text], conf)



    val  encodedLinesRDD:RDD[String] = FileRDD.map(x=>x._2.toString)

    val decodedLinesRDD = encodedLinesRDD.map((x: Any) => {
      def decodeLine(x: Any) = {
        val inputStream = new ByteArrayInputStream(x.asInstanceOf[String].getBytes)
        val asn1InputStream: ASN1InputStream = new ASN1InputStream(inputStream)
        var asn1PrimitiveObject : ASN1Primitive = null
        var callDetailRecord : CallDetailRecord= null
        var rowArray= Array[Seq[Any]]()
        while ({asn1PrimitiveObject = asn1InputStream.readObject;asn1PrimitiveObject!=null}) {
          callDetailRecord = new CallDetailRecord(asn1PrimitiveObject.asInstanceOf[ASN1Sequence])
          rowArray=rowArray :+ Seq(callDetailRecord.getRecordNumber.toString,callDetailRecord.getCallingNumber,callDetailRecord.getCalledNumber,callDetailRecord.getStartDate,callDetailRecord.getStartTime,callDetailRecord.getDuration.toString)
        }
        asn1InputStream.close()
        rowArray=rowArray.map(x=>x.zipWithIndex.map({case (value, index) =>
          val colName = schemaFields(index).name
          val castedValue = value

          if (requiredColumns.contains(colName)) Some(castedValue) else " "
        }))

        rowArray.map(s=>rearrangeSequence(requiredColumns,s)).map(s=>s.filter(_!=" ")).map(s => Row.fromSeq(s))
      }


      decodeLine(x)

    })
    decodedLinesRDD.flatMap(x=>x)

  }








  def rearrangeSequence(order : Array[String],sequence:Seq[Any]): Seq[Any] ={
    val schemaFields = schema.fields

    var blankSequence=sequence.map(x=>" ".asInstanceOf[Any])
    sequence.zipWithIndex.foreach({
      case (value, index) =>
        if(value!= " "){
          val columnName = schemaFields(index).name
          val orderColumnIndex =order.indexOf(columnName)
          blankSequence=blankSequence.updated(orderColumnIndex,value)
        }
    })

    blankSequence
  }


}
