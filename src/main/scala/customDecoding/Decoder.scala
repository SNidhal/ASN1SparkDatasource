package customDecoding

import org.apache.hadoop.io.Text
import org.apache.spark.sql.types.StructType

trait Decoder {
  def decode(record: Text, schema: StructType): Seq[Any]
}
