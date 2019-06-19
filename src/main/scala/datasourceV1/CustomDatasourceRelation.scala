package datasourceV1

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
  extends BaseRelation with TableScan with Serializable {

  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      StructType(
        StructField("recordNumber", IntegerType, false) ::
        StructField("callingNumber", StringType, true) ::
        StructField("calledNumber", StringType, true) ::
          StructField("StartDate", StringType, true) ::
          StructField("StartTime", StringType, true) ::
        StructField("Duration", IntegerType, true) ::Nil

      )
    }
  }


  override def buildScan(): RDD[Row] = {
    println("TableScan: buildScan called...")


    val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
    System.out.println("tessssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssst")
    val rdd1: RDD[(LongWritable, Text)] = sqlContext.sparkContext.newAPIHadoopFile("hdfs://hadoop1.example.com:8020/user/admin/test.ber", classOf[RawFileAsBinaryInputFormat], classOf[LongWritable], classOf[Text], conf)

    ///////////////////////

    val  rdd3:RDD[String] = rdd1.map(x=>x._2.toString)
//println("cooooooooooooooooooooooooooounnnntttttttttttt"+rdd3.count())

    val rd = rdd3.map((x: Any) => {
      def foo(x: Any) = {
        println(x)
        val is = new ByteArrayInputStream(x.asInstanceOf[String].getBytes)
        val asnin: ASN1InputStream = new ASN1InputStream(is)
        var obj : ASN1Primitive = null
        var thisCdr : CallDetailRecord= null
        var arr= Array[Row]()
        while ({obj = asnin.readObject;obj!=null}) {
          println("*****************************************************************************************************************"+arr.length)
          thisCdr = new CallDetailRecord(obj.asInstanceOf[ASN1Sequence])
          System.out.println("CallDetailRecord " + thisCdr.getRecordNumber + " Calling " + thisCdr.getCallingNumber + " Called " + thisCdr.getCalledNumber + " Start Date-Time " + thisCdr.getStartDate + "-" + thisCdr.getStartTime + " duration " + thisCdr.getDuration)
           arr=arr :+ Row.fromSeq(Seq(thisCdr.getRecordNumber,thisCdr.getCallingNumber,thisCdr.getCalledNumber,thisCdr.getStartDate,thisCdr.getStartTime,thisCdr.getDuration))
        }
        asnin.close()
        println("outttttttttttttttttt")
        val cdr2 = new CallDetailRecord2(thisCdr.getRecordNumber, thisCdr.getCallingNumber, thisCdr.getCalledNumber, thisCdr.getStartDate, thisCdr.getStartTime, thisCdr.getDuration)
        arr
      }

      //println("Row length =    "+Row(foo(x)).length)
       println("array size   : "+foo(x))
foo(x)

    })
rd.foreach(x=>print("array size  : ------------------------------------------------------"+x.length))
  rd.flatMap(x=>x)


  }


}
