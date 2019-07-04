package compiler;

import org.apache.spark.sql.types.StructType;

public class InferSchema {
    public static StructType inferredSchema=new StructType();

    public static StructType getInferredSchema(String path) {
        try {
            Compiler.InferSchema(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return inferredSchema;
    }
}
