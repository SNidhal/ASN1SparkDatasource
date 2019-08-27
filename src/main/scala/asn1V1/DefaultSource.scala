package asn1V1

import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext


class DefaultSource extends RelationProvider with SchemaRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val path = parameters.getOrElse("path", sys.error("'path' must be specified for ASN.1(Ber/Der) data."))
    val schemaFilePath = parameters.getOrElse("schemaFilePath", sys.error("path of the schema file must be specified."))
    val schemaFileType = parameters.getOrElse("schemaFileType", sys.error("type of the schema file must be specified."))
    val customDecoder = parameters.getOrElse("customDecoder", "none")
    val precisionFactor = parameters.getOrElse("precisionFactor", "5")
    val mainTag = parameters.getOrElse("mainTag", "sequence")
    val customDecoderLanguage = parameters.getOrElse("customDecoderLanguage", "none")

    if (customDecoder.equals("none") ^ customDecoderLanguage.equals("none"))
      sys.error("if you are using custom decoder both 'customDecoder' and 'customDecoderLanguage' must be specified.")

    ASN1DatasourceRelation(
      sqlContext,
      schemaFileType,
      path,
      schema,
      schemaFilePath,
      customDecoder,
      customDecoderLanguage,
      precisionFactor,
      mainTag)
  }

}
