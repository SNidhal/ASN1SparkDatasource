import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkApp {

  def main(args: Array[String]): Unit = {

  val conf = new SparkConf().setAppName("spark-custom-datasource")
  val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

  val as1DataFrame = spark.sqlContext.read.format("asn1V1").load("test.ber")


    as1DataFrame.printSchema()


    as1DataFrame.createOrReplaceTempView("test")
    spark.sql("select callingNumber,Duration,recordNumber from test").show()



  }
}
