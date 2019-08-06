package largeFileGeneration;

import com.beanit.jasn1.ber.ReverseByteArrayOutputStream;

import java.io.*;
import java.nio.file.Files;

public class Generator {

    public static void main(String[] args) throws IOException {
        File file = new File("src/main/java/largeFileGeneration/baseFile.ber");
        File file2 = new File("D:/asnLargeFiles/large.ber");
        byte[] fileContent = Files.readAllBytes(file.toPath());
        InputStream is = new ByteArrayInputStream(fileContent);
        ReverseByteArrayOutputStream os = new ReverseByteArrayOutputStream(1000);
        GenericCallDataRecord personnelRecord_decoded = new GenericCallDataRecord();


        personnelRecord_decoded.decode(is);
        personnelRecord_decoded.encode(os);

        personnelRecord_decoded.decode(is);
        personnelRecord_decoded.encode(os);

        personnelRecord_decoded.decode(is);
        personnelRecord_decoded.encode(os);


        OutputStream outStream = new FileOutputStream(file2);

        int i;

        for (i = 0; i < 9999; i++) {
            outStream.write(os.getArray());
        }
    }
}
