package customDecoding;

import org.apache.hadoop.io.Text;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Seq;

import java.io.IOException;

public interface JavaDecoder {
    public Seq<Object> decode(Text record, StructType schema) throws IOException;
}
