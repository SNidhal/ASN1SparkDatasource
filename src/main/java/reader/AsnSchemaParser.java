package reader;

import org.apache.spark.sql.types.StructType;

public class AsnSchemaParser {
    public static StructType inferredSchema=new StructType();

    public static StructType getParsedSchema(String path) {
        try {
            Compiler.InferSchema(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return inferredSchema;
    }
}
