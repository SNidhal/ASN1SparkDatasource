package util

import java.io.UnsupportedEncodingException
import java.util

import org.apache.spark.sql.types.{DataType, DataTypes, IntegerType, LongType, StringType, StructField, StructType}
import org.bouncycastle.asn1.{ASN1Encodable, ASN1Primitive, ASN1Sequence, DLApplicationSpecific}


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

  @throws[UnsupportedEncodingException]
  def asn1SequenceToSequence(inSeq: ASN1Sequence): Seq[Any] = {

    var sequence:Seq[Any]=Seq()
   val schema: StructType= StructType(
          StructField("recordNumber", IntegerType, false) ::
            StructField("callingNumber", StringType, true) ::
            StructField("calledNumber", StringType, true) ::
            StructField("StartDate", StringType, true) ::
            StructField("StartTime", StringType, true) ::
            StructField("Duration", IntegerType, true) ::Nil

        )


    val cdr: ASN1Sequence = inSeq
    val en: util.Enumeration[ASN1Encodable] = cdr.getObjects.asInstanceOf[util.Enumeration[ASN1Encodable]]
    var i =0;
    while ( {
      en.hasMoreElements
    }) {

      val em: ASN1Encodable = en.nextElement
      val emp: ASN1Primitive = em.toASN1Primitive
      val emt: DLApplicationSpecific = emp.asInstanceOf[DLApplicationSpecific]
      if(schema.apply(i).dataType == DataTypes.StringType){
        val x: String = new String(emt.getContents, "UTF-8")
        sequence=sequence :+ x
      }
      else {
        val x = emt.getContents.apply(0)
        sequence=sequence :+ x.toString
      }
      i=i+1
    }
    sequence
  }

}
