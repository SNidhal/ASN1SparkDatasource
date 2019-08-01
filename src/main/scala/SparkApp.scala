import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkApp {

  def main(args: Array[String]): Unit = {

    val asnFilePath = args(0)
    val master = args(1)

    val conf = new SparkConf().setAppName("spark-custom-datasource")
    val spark = SparkSession.builder().config(conf).master(master).getOrCreate()


    val schema = StructType(
      StructField("foo", StructType(StructField("bar", IntegerType, true) :: Nil), false) ::
        StructField("name", StringType, true) ::
        StructField("age", StringType, true) :: Nil
    )

    val as1DataFrame = spark.read.format("asn1V1")
      .option("schemaFileType","asn")
      .option("schemaFilePath", "cdr.asn")
      .load("test.ber")

    as1DataFrame.printSchema()

    as1DataFrame.createOrReplaceTempView("test")
    spark.sql("select * from test").count()
    println(as1DataFrame.rdd.partitions.length + "  partitions")

    Thread.sleep(100000000)

  }
}
