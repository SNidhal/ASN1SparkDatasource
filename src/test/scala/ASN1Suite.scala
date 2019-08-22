import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FeatureSpec, FunSuite, Matchers}

class ASN1Suite extends FunSuite with BeforeAndAfterAll with Matchers {


  val nestedFilePath = "src/test/resources/nestedExample.ber"
  val nestedFileDefinition = "src/test/resources/nestedExample.asn"

  val conf: SparkConf = new SparkConf().setAppName("spark-custom-datasource")
  private var spark: SparkSession = _


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder().config(conf).master("local[*]").getOrCreate()


  }

  override protected def afterAll(): Unit = {
    try {
      spark.stop()
    } finally {
      super.afterAll()
    }
  }


  test("nested data test") {
    val spark2 = spark
    import spark2.implicits._
    val someDF = Seq(
      (1, "Adam", 256)
    ).toDF

    val as1DataFrame = spark.read.format("asn1V1")
      .option("schemaFileType", "asn")
      .option("schemaFilePath", nestedFileDefinition)
      .load(nestedFilePath)

    as1DataFrame.collect() should contain theSameElementsAs someDF.collect()

  }

}
