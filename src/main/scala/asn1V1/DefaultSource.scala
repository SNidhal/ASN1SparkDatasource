package asn1V1

import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext


class DefaultSource extends RelationProvider with SchemaRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val path = parameters.get("path")
    val schemaFilePath = parameters.get("schemaFilePath")
    val schemaFileType = parameters.get("schemaFileType")
    val customDecoder = parameters.get("customDecoder")
    val customDecoderLanguage = parameters.get("customDecoderLanguage")

    path match {
      case Some(p) => {
        customDecoder match {
          case Some(cd) => {
            customDecoderLanguage match {
              case Some(cdl) => ASN1DatasourceRelation(sqlContext, schemaFileType.get, p, schema, schemaFilePath.get, cd, cdl)
              case _ => throw new IllegalArgumentException("custom decoder source language is required")
            }
          }
          case _ => ASN1DatasourceRelation(sqlContext, schemaFileType.get, p, schema, schemaFilePath.get, "none","none")
        }
      }
      case _ => throw new IllegalArgumentException("Path is required for asn.1 datasource format!!")
    }
  }

}
