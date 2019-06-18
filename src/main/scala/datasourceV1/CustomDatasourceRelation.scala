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

/**
  * Created by rana on 29/9/16.
  */
class CustomDatasourceRelation(override val sqlContext : SQLContext, path : String, userSchema : StructType)
  extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan with Serializable {

  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      StructType(
        StructField("recordNumber", IntegerType, false) ::
        StructField("callingNumber", StringType, true) ::
        StructField("calledNumber", StringType, true) ::
        StructField("startDate", StringType, true) ::Nil

      )
    }
  }


  override def buildScan(): RDD[Row] = {
    println("TableScan: buildScan called...")


    val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
    System.out.println("tessssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssst")
    val rdd1: RDD[(LongWritable, Text)] = sqlContext.sparkContext.newAPIHadoopFile("hdfs://hadoop1.example.com:8020/user/admin/test.ber", classOf[RawFileAsBinaryInputFormat], classOf[LongWritable], classOf[Text], conf)

    ///////////////////////

    val  rdd3:RDD[_] = rdd1.map(x=>x._2.toString)


    val rd: RDD[Row] = rdd3.map((x: Any) => {
      def foo(x: Any) = {
        val is = new ByteArrayInputStream(x.asInstanceOf[String].getBytes)
        val asnin: ASN1InputStream = new ASN1InputStream(is)
        var obj : ASN1Primitive = null
        var thisCdr : CallDetailRecord= null
        while (
          (obj = asnin.readObject) != null
        ) {
          thisCdr = new CallDetailRecord(obj.asInstanceOf[ASN1Sequence])
          System.out.println("CallDetailRecord " + thisCdr.getRecordNumber + " Calling " + thisCdr.getCallingNumber + " Called " + thisCdr.getCalledNumber + " Start Date-Time " + thisCdr.getStartDate + "-" + thisCdr.getStartTime + " duration " + thisCdr.getDuration)
        }
        asnin.close()
        val cdr2 = new CallDetailRecord2(thisCdr.getRecordNumber, thisCdr.getCallingNumber, thisCdr.getCalledNumber, thisCdr.getStartDate, thisCdr.getStartTime, thisCdr.getDuration)
        cdr2
      }

      foo(x).asInstanceOf[Row]
    })

  rd


  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    println("PrunedScan: buildScan called...")

    val schemaFields = schema.fields
    // Reading the file's content
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(f => f._2)

    val rows = rdd.map(fileContent => {
      val lines = fileContent.split("\n")
      val data = lines.map(line => line.split(",").map(word => word.trim).toSeq)
      val tmp = data.map(words => words.zipWithIndex.map{
        case (value, index) =>
          val colName = schemaFields(index).name
          val castedValue = Util.castTo(if (colName.equalsIgnoreCase("gender")) {if(value.toInt == 1) "Male" else "Female"} else value,
                                        schemaFields(index).dataType)
          if (requiredColumns.contains(colName)) Some(castedValue) else None
      })

      tmp.map(s => Row.fromSeq(s.filter(_.isDefined).map(value => value.get)))
    })

    rows.flatMap(e => e)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    println("PrunedFilterScan: buildScan called...")



    val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
    System.out.println("tessssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssst")
    val rdd1: RDD[(LongWritable, Text)] = sqlContext.sparkContext.newAPIHadoopFile("hdfs://hadoop1.example.com:8020/user/admin/test.ber", classOf[RawFileAsBinaryInputFormat], classOf[LongWritable], classOf[Text], conf)
    val rdd3: RDD[_] = rdd1.map(x=>x._2.toString())

    //val rdd2: JavaRDD[_] = rdd3.repartition(5)
    /*List rd =rdd2.map(new Function<Tuple2<LongWritable, Text>, Object>() {
        public Object call(Tuple2<LongWritable, Text> longWritableTextTuple2) throws Exception {
            return longWritableTextTuple2._2().toString();
        }
    }).collect();*/

    val rd = rdd3.map((x: Any) => {
      def foo(x: Any) = {
        val is: InputStream = new ByteArrayInputStream(x.asInstanceOf[String].getBytes)
        val asnin: ASN1InputStream = new ASN1InputStream(is)
        var obj: ASN1Primitive = null
        var thisCdr: CallDetailRecord = null
        while ( {
          (obj = asnin.readObject) != null
        }) {
          println(obj)
          thisCdr = new CallDetailRecord(obj.asInstanceOf[ASN1Sequence])
          System.out.println("CallDetailRecord " + thisCdr.getRecordNumber + " Calling " + thisCdr.getCallingNumber + " Called " + thisCdr.getCalledNumber + " Start Date-Time " + thisCdr.getStartDate + "-" + thisCdr.getStartTime + " duration " + thisCdr.getDuration)
        }
        asnin.close()
        val cdr2: CallDetailRecord2 = new CallDetailRecord2(thisCdr.getRecordNumber, thisCdr.getCallingNumber, thisCdr.getCalledNumber, thisCdr.getStartDate, thisCdr.getStartTime, thisCdr.getDuration)
        cdr2
      }

      foo(x)
    }).collect()


    /*import scala.collection.JavaConversions._
    for (o <- rd) {
      System.out.println("main")
      System.out.println(o)
    }
    System.out.println(" end size : " + rd.size)*/




    val schemaFields = schema.fields
    // Reading the file's content
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(f => f._2)

    val rows = rdd.map(fileContent => {
      val lines = fileContent.split("\n")
      val data = lines.map(line => line.split(",").map(word => word.trim).toSeq)
      val tmp = data.map(words => words.zipWithIndex.map{
        case (value, index) =>
          val colName = schemaFields(index).name
          Util.castTo(if (colName.equalsIgnoreCase("gender")) {if(value.toInt == 1) "Male" else "Female"} else value,
            schemaFields(index).dataType)
      })

      tmp.map(s => Row.fromSeq(s))
    })

    rows.flatMap(e => e)
  }
}
