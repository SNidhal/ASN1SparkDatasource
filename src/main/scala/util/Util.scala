package util

import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructType}


object Util {
  def castTo(value : String, dataType : DataType) = {
    dataType match {
      case _ : IntegerType => value.toInt
      case _ : LongType => value.toLong
      case _ : StringType => value
    }
  }


  def rearrangeSequence(order : Array[String],sequence:Seq[Any],sh:StructType): Seq[Any] ={
    val schemaFields = sh.fields

    var blankSequence=sequence.map(x=>" ".asInstanceOf[Any])
    sequence.zipWithIndex.foreach({
      case (value, index) =>
        if(value!= " "){
          val columnName = schemaFields(index).name
          val orderColumnIndex =order.indexOf(columnName)
          blankSequence=blankSequence.updated(orderColumnIndex,value)
        }
    })

    blankSequence
  }
}
