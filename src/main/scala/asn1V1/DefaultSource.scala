package asn1V1

import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext


class DefaultSource extends RelationProvider with SchemaRelationProvider  {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val path = parameters.get("path")
    val schemaFilePath = parameters.get("schemaFilePath")
    val schemaFileType = parameters.get("schemaFileType")
    path match {
      case Some(p)  => ASN1DatasourceRelation(sqlContext,schemaFileType.get, p, schema,schemaFilePath.get)
      case _ => throw new IllegalArgumentException("Path is required for asn.1 datasource format!!")
    }
  }

}
