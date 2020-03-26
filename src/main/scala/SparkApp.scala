import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark-custom-datasource")
    val spark = SparkSession.builder().config(conf).master("local[*]").getOrCreate()



    val as1DataFrame = spark.read.format("asn1V1")
      .option("schemaFileType","asn")
      .option("schemaFilePath","src/test/resources/simpleTypes.asn")
      .load("src/test/resources/simpleTypes.ber")



    as1DataFrame.createOrReplaceTempView("test")
    spark.sql("select * from test").show()



    as1DataFrame.write.mode(SaveMode.Overwrite).parquet("/user/nidhal/test")

    Thread.sleep(111111111)

  }
}
