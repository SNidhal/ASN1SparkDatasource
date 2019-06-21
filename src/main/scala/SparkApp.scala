import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkApp {

  def main(args: Array[String]): Unit = {
  println("Application started...")

  val conf = new SparkConf().setAppName("spark-custom-datasource")
  val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

  val df = spark.sqlContext.read.format("asn1V1").load("hdfs://hadoop1.example.com:8020/user/admin/test.ber")

  //print the schema
   df.printSchema()

  //print the data
  df.show()

  //save the data
  //  df.write.options(Map("format" -> "customFormat")).mode(SaveMode.Overwrite).format("io.dcengines.rana.datasource").save("out_custom/")
  //  df.write.options(Map("format" -> "json")).mode(SaveMode.Overwrite).format("io.dcengines.rana.datasource").save("out_json/")
  //  df.write.mode(SaveMode.Overwrite).format("io.dcengines.rana.datasource").save("out_none/")

  //select some specific columns
    //df.createOrReplaceTempView("test")
    //spark.sql("select * from test").show()

  //filter data
//  df.createOrReplaceTempView("test")
 // spark.sql("select * from test where salary = 50000").show()

  println("Application Ended...")

  }
}
