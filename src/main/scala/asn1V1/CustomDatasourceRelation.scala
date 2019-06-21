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

/**
  * Created by rana on 29/9/16.
  */
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
    val rdd1: RDD[(LongWritable, Text)] = sqlContext.sparkContext.newAPIHadoopFile(path, classOf[RawFileAsBinaryInputFormat], classOf[LongWritable], classOf[Text], conf)


    val  rdd3:RDD[String] = rdd1.map(x=>x._2.toString)

    val rd = rdd3.map((x: Any) => {
      def foo(x: Any) = {
        val is = new ByteArrayInputStream(x.asInstanceOf[String].getBytes)
        val asnin: ASN1InputStream = new ASN1InputStream(is)
        var obj : ASN1Primitive = null
        var thisCdr : CallDetailRecord= null
        var arr= Array[Row]()
        while ({obj = asnin.readObject;obj!=null}) {
          thisCdr = new CallDetailRecord(obj.asInstanceOf[ASN1Sequence])
           arr=arr :+ Row.fromSeq(Seq(thisCdr.getRecordNumber,thisCdr.getCallingNumber,thisCdr.getCalledNumber,thisCdr.getStartDate,thisCdr.getStartTime,thisCdr.getDuration))
        }
        asnin.close()
        val cdr2 = new CallDetailRecord2(thisCdr.getRecordNumber, thisCdr.getCallingNumber, thisCdr.getCalledNumber, thisCdr.getStartDate, thisCdr.getStartTime, thisCdr.getDuration)
        arr
      }

      foo(x)

    })
  rd.flatMap(x=>x)


  }


  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {

    val schemaFields = schema.fields


    val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
    val rdd1: RDD[(LongWritable, Text)] = sqlContext.sparkContext.newAPIHadoopFile(path, classOf[RawFileAsBinaryInputFormat], classOf[LongWritable], classOf[Text], conf)



    val  rdd3:RDD[String] = rdd1.map(x=>x._2.toString)

    val rd = rdd3.map((x: Any) => {
      def foo(x: Any) = {
        val is = new ByteArrayInputStream(x.asInstanceOf[String].getBytes)
        val asnin: ASN1InputStream = new ASN1InputStream(is)
        var obj : ASN1Primitive = null
        var thisCdr : CallDetailRecord= null
        var arr= Array[Seq[Any]]()
        while ({obj = asnin.readObject;obj!=null}) {
          thisCdr = new CallDetailRecord(obj.asInstanceOf[ASN1Sequence])
          arr=arr :+ Seq(thisCdr.getRecordNumber.toString,thisCdr.getCallingNumber,thisCdr.getCalledNumber,thisCdr.getStartDate,thisCdr.getStartTime,thisCdr.getDuration.toString)
        }
        asnin.close()
        arr=arr.map(x=>x.zipWithIndex.map({case (value, index) =>
          val colName = schemaFields(index).name
          val castedValue = value

          if (requiredColumns.contains(colName)) Some(castedValue) else " "
        }))

        arr.map(s=>rearrange(requiredColumns,s)).map(s=>s.filter(_!=" ")).map(s => Row.fromSeq(s))
      }


      foo(x)

    })
    rd.flatMap(x=>x)

  }


  def rearrange(order : Array[String],sq:Seq[Any]): Seq[Any] ={
    val schemaFields = schema.fields
    val x=sq
    var x3 : Seq[Any]=null
    var x2=sq.map(x=>" ".asInstanceOf[Any])
    sq.zipWithIndex.foreach({
      case (value, index) =>
        if(value!= " "){
          val colName = schemaFields(index).name
          order.foreach(x=>println(x))
          println(colName)
          val ind =order.indexOf(colName)
          x2=x2.updated(ind,value)
        }
        value

    })

    x2
  }


}
