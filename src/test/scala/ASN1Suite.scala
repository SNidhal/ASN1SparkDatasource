import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FeatureSpec, FunSuite, Matchers}

class ASN1Suite extends FunSuite with BeforeAndAfterAll with Matchers {


  val nestedFilePath = "src/test/resources/nestedExample.ber"
  val simpleTypeFilePath="src/test/resources/simpleTypes.ber"
  val nestedFileDefinition = "src/test/resources/nestedExample.asn"
  val simpleTypeFileDefinition="src/test/resources/simpleTypes.asn"

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

  test("custom scala decoder test") {
    val spark2 = spark
    import spark2.implicits._
    val simpleTypeDF = Seq(
      (1,"15555550100","15555550101","20131016","134534",65),
      (2,"15555550102","15555550104","20131016","134541",52),
      (3,"15555550103","15555550102","20131016","134751",62),
      (4,"15555550104","15555550102","20131016","134901",72),
      (5,"15555550101","15555550100","20131016","135134",32)
    ).toDF

    val as1DataFrame = spark.read.format("asn1V1")
      .option("schemaFileType","asn")
      .option("schemaFilePath",simpleTypeFileDefinition )
      .option("customDecoder","customDecoding.CustomScalaDecoder")
      .option("customDecoderLanguage","scala")
      .load(simpleTypeFilePath)

    as1DataFrame.collect() should contain theSameElementsAs simpleTypeDF.collect()

  }

  test("custom java decoder test") {
    val spark2 = spark
    import spark2.implicits._
    val simpleTypeDF = Seq(
      (1,"15555550100","15555550101","20131016","134534",65),
      (2,"15555550102","15555550104","20131016","134541",52),
      (3,"15555550103","15555550102","20131016","134751",62),
      (4,"15555550104","15555550102","20131016","134901",72),
      (5,"15555550101","15555550100","20131016","135134",32)
    ).toDF

    val as1DataFrame = spark.read.format("asn1V1")
      .option("schemaFileType","asn")
      .option("schemaFilePath",simpleTypeFileDefinition )
      .option("customDecoder","customDecoding.CustomJavaDecoder")
      .option("customDecoderLanguage","java")
      .load(simpleTypeFilePath)

    as1DataFrame.collect() should contain theSameElementsAs simpleTypeDF.collect()

  }

}
