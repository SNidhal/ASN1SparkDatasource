package util

import java.io.UnsupportedEncodingException
import java.util

import org.apache.spark.sql.types.{DataType, DataTypes, IntegerType, LongType, StringType, StructField, StructType}
import org.bouncycastle.asn1.{ASN1Encodable, ASN1Primitive, ASN1Sequence, DLApplicationSpecific}


object Util {

  def castTo(value: String, dataType: DataType) = {
    dataType match {
      case _: IntegerType => value.toInt
      case _: LongType => value.toLong
      case _: StringType => value
    }
  }


  def rearrangeSequence(order: Array[String], sequence: Seq[Any], sh: StructType): Seq[Any] = {
    val schemaFields = sh.fields
    schemaFields.foreach(x => println(x.name))
    var blankSequence = sequence.map(x => " ".asInstanceOf[Any])
    sequence.zipWithIndex.foreach({
      case (value, index) =>
        val columnName = schemaFields(index).name
        val orderColumnIndex = order.indexOf(columnName)
        if (orderColumnIndex != -1)
          blankSequence = blankSequence.updated(orderColumnIndex, value)
    })
    blankSequence
  }


}
