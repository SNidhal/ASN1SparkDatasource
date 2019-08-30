package customDecoding

import java.io.ByteArrayInputStream

import asn1V1.Asn1Parser.decodeNestedRecord
import com.beanit.jasn1.ber.types.string.BerUTF8String
import com.beanit.jasn1.ber.types.{BerBoolean, BerDate, BerInteger, BerReal}
import com.beanit.jasn1.ber.{BerLength, BerTag}
import org.apache.hadoop.io.Text
import org.apache.spark.sql.types.{DataTypes, StructType}
import util.Util

object CustomScalaDecoder extends ScalaDecoder {

  override def decode(record: Text, schema: StructType): Seq[Any] = {
    val lineInputStream = new ByteArrayInputStream(record.getBytes)
    var sequence: Seq[Seq[Any]] = Seq()
    val tag = new BerTag(BerTag.UNIVERSAL_CLASS, BerTag.CONSTRUCTED, 16)
    var codeLength = 0
    var subCodeLength = 0
    val berTag = new BerTag
    if (true) {
      codeLength += tag.decodeAndCheck(lineInputStream);
    }
    val length = new BerLength
    codeLength += length.decode(lineInputStream)
    val totalLength = length.`val`
    codeLength += totalLength
    subCodeLength += berTag.decode(lineInputStream)
    schema.fields.foreach(field => {
      field.dataType match {
        case DataTypes.IntegerType =>
          var integerValue = new BerInteger
          integerValue.decode(lineInputStream, false)
          if (schema.fieldIndex(field.name) != schema.fields.length) subCodeLength += berTag.decode(lineInputStream)
          sequence = sequence :+ Seq(Util.castTo(integerValue.intValue().toString, field.dataType))

        case DataTypes.StringType =>
          var stringValue = new BerUTF8String()
          stringValue.decode(lineInputStream, false)
          if (schema.fieldIndex(field.name) != schema.fields.length)
            subCodeLength += berTag.decode(lineInputStream)
          sequence = sequence :+ Seq(Util.castTo(stringValue.toString, field.dataType))

        case DataTypes.BooleanType =>
          var booleanValue = new BerBoolean()
          booleanValue.decode(lineInputStream, false)
          if (schema.fieldIndex(field.name) != schema.fields.length) subCodeLength += berTag.decode(lineInputStream)
          sequence = sequence :+ Seq(Util.castTo(booleanValue.toString, field.dataType))

        case DataTypes.DateType =>
          var dateValue = new BerDate()
          dateValue.decode(lineInputStream, false)
          if (schema.fieldIndex(field.name) != schema.fields.length) subCodeLength += berTag.decode(lineInputStream)
          sequence = sequence :+ Seq(Util.castTo(dateValue.toString, field.dataType))

        case DataTypes.FloatType =>
          var floatValue = new BerReal()
          floatValue.decode(lineInputStream, false)
          if (schema.fieldIndex(field.name) != schema.fields.length) subCodeLength += berTag.decode(lineInputStream)
          sequence = sequence :+ Seq(Util.castTo(floatValue.toString, field.dataType))

        case struct: StructType =>
          sequence = sequence :+ decodeNestedRecord(lineInputStream, struct, false,16)
      }
    })
    sequence.flatten
  }
}
