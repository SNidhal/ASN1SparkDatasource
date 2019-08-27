package asn1V1

import java.io.{ByteArrayInputStream, InputStream}

import com.beanit.jasn1.ber.{BerLength, BerTag}
import com.beanit.jasn1.ber.types.{BerBoolean, BerDate, BerInteger, BerReal}
import com.beanit.jasn1.ber.types.string.BerUTF8String
import org.apache.hadoop.io.Text
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import util.Util

object Asn1Parser {

  def decodeRecord(record: Text, schema: StructType, withTag: Boolean, mainTag: Int): Seq[Any] = {
    val lineInputStream = new ByteArrayInputStream(record.getBytes)
    var sequence: Seq[Seq[Any]] = Seq()
    val tag = new BerTag(BerTag.UNIVERSAL_CLASS, BerTag.CONSTRUCTED, mainTag)
    var codeLength = 0
    var subCodeLength = 0
    val berTag = new BerTag
    if (withTag) {
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
          sequence = sequence :+ decodeNestedRecord(lineInputStream, struct, false,mainTag)
      }
    })
    sequence.flatten
  }


  def decodeNestedRecord(is: InputStream, schema: StructType, withTag: Boolean,mainTag: Int): Seq[Any] = {
    var sequence: Seq[Any] = Seq()
    val tag = new BerTag(BerTag.UNIVERSAL_CLASS, BerTag.CONSTRUCTED, mainTag)
    var codeLength = 0
    var subCodeLength = 0
    val berTag = new BerTag
    if (withTag) {
      codeLength += tag.decodeAndCheck(is)
    }
    val length = new BerLength
    codeLength += length.decode(is)
    val totalLength = length.`val`
    codeLength += totalLength
    subCodeLength += berTag.decode(is)
    schema.fields.foreach(field => {
      field.dataType match {
        case DataTypes.IntegerType =>
          var integerValue = new BerInteger
          integerValue.decode(is, false)
          if (schema.fieldIndex(field.name) != schema.fields.length) subCodeLength += berTag.decode(is)
          sequence = sequence :+ Util.castTo(integerValue.intValue().toString, field.dataType)

        case DataTypes.StringType =>
          var stringValue = new BerUTF8String()
          stringValue.decode(is, false)
          if (schema.fieldIndex(field.name) != schema.fields.length) subCodeLength += berTag.decode(is)
          sequence = sequence :+ Util.castTo(stringValue.toString, field.dataType)

        case DataTypes.BooleanType =>
          var booleanValue = new BerBoolean()
          booleanValue.decode(is, false)
          if (schema.fieldIndex(field.name) != schema.fields.length) subCodeLength += berTag.decode(is)
          sequence = sequence :+ Util.castTo(booleanValue.toString, field.dataType)

        case DataTypes.DateType =>
          var dateValue = new BerDate()
          dateValue.decode(is, false)
          if (schema.fieldIndex(field.name) != schema.fields.length) subCodeLength += berTag.decode(is)
          sequence = sequence :+ Util.castTo(dateValue.toString, field.dataType)

        case DataTypes.FloatType =>
          var floatValue = new BerReal()
          floatValue.decode(is, false)
          if (schema.fieldIndex(field.name) != schema.fields.length) subCodeLength += berTag.decode(is)
          sequence = sequence :+ Util.castTo(floatValue.toString, field.dataType)

        case DataTypes.DateType =>
          var dateValue = new BerDate()
          dateValue.decode(is, false)
          if (schema.fieldIndex(field.name) != schema.fields.length) subCodeLength += berTag.decode(is)
          sequence = sequence :+ Util.castTo(dateValue.toString, field.dataType)


        case struct: StructType =>
          sequence = sequence :+ decodeNestedRecord(is, struct, false,mainTag)
      }
    })
    sequence
  }


  def flatten(schema: StructType): Array[StructField] = {
    schema.fields.flatMap { f =>
      f.dataType match {
        case struct: StructType => flatten(struct).map(x => x.copy(name = f.name + "." + x.name))
        case _ => Array(f)
      }
    }
  }

}
