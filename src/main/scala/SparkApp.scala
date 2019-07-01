import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkApp {

  def main(args: Array[String]): Unit = {

    val asnFilePath = args(0)
    val master = args(1)

  val conf = new SparkConf().setAppName("spark-custom-datasource")
  val spark = SparkSession.builder().config(conf).master(master).getOrCreate()

  val as1DataFrame = spark.read.format("asn1V1").load(asnFilePath)


    as1DataFrame.printSchema()

    as1DataFrame.show()
//    as1DataFrame.createOrReplaceTempView("test")
//    spark.sql("select callingNumber,Duration,recordNumber from test where Duration > 60").show()


  }
}
