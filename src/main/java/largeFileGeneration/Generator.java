package largeFileGeneration;

import com.beanit.jasn1.ber.ReverseByteArrayOutputStream;

import java.io.*;
import java.nio.file.Files;

public class Generator {

    public static void generateFile(String outputPath,long recordNumber) throws IOException {

        File baseFile = new File("src/main/java/largeFileGeneration/baseFile.ber");
        File destinationFile = new File(outputPath);
        destinationFile.createNewFile();
        byte[] fileContent = Files.readAllBytes(baseFile.toPath());
        InputStream is = new ByteArrayInputStream(fileContent);
        ReverseByteArrayOutputStream os = new ReverseByteArrayOutputStream(1000);
        GenericCallDataRecord record = new GenericCallDataRecord();

        record.decode(is);
        record.encode(os);

        record.decode(is);
        record.encode(os);

        record.decode(is);
        record.encode(os);

        OutputStream outStream = new FileOutputStream(destinationFile);

        int i;



        for (i = 0; i < recordNumber/3; i++) {
            outStream.write(os.getArray());
        }
    }
}
