import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SparkApp {

  def main(args: Array[String]): Unit = {

    val asnFilePath = args(0)
    val master = args(1)

  val conf = new SparkConf().setAppName("spark-custom-datasource")
  val spark = SparkSession.builder().config(conf).master(master).getOrCreate()


  val as1DataFrame = spark.read.format("asn1V1")
                          .option("asnDefinitionPath","humanAnon.asn")
                          .load("humanAnon.ber")

    as1DataFrame.printSchema()

    as1DataFrame.createOrReplaceTempView("test")
    spark.sql("select * from test").show()
    println(as1DataFrame.rdd.partitions.length+"  partitions")


  }
}
