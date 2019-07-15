package asn1V1

import java.io.ByteArrayInputStream

import org.apache.hadoop.io.Text
import org.apache.spark.sql.types.StructType
import util.Util

object Asn1Parser {

  def decodeRecord(record: Text, className: String, schema: StructType): Seq[Any] = {
    val lineInputStream = new ByteArrayInputStream(record.getBytes)

    val generatedClassInstance  = Class.forName("util."+className).newInstance

    val decodeMethod =generatedClassInstance.getClass.getMethod("test",lineInputStream.getClass)


    decodeMethod.invoke(generatedClassInstance,lineInputStream)

    var sequence :Seq[Any] = Seq()

    schema.fields.foreach(field=>{
      if(generatedClassInstance.toString.indexOf(",",generatedClassInstance.toString.indexOf("\t"+field.name+":"))>0){
        val value =generatedClassInstance.toString
          .substring(generatedClassInstance.toString.indexOf("\t"+field.name+":")+field.name.length+3,generatedClassInstance.toString.indexOf(",",generatedClassInstance.toString.indexOf("\t"+field.name+":")))
        sequence = sequence :+ Util.castTo(value, field.dataType)
      }
      else{
        val value=generatedClassInstance.toString.substring(generatedClassInstance.toString.indexOf("\t"+field.name+":")+field.name.length+3,generatedClassInstance.toString.length)
        sequence = sequence :+ Util.castTo(value, field.dataType)
      }

    })
    sequence
  }
}
