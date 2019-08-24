package customDecoding;

import com.beanit.jasn1.ber.BerLength;
import com.beanit.jasn1.ber.BerTag;
import com.beanit.jasn1.ber.types.BerInteger;
import com.beanit.jasn1.ber.types.string.BerUTF8String;
import org.apache.hadoop.io.Text;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

public class CustomJavaDecoder implements JavaDecoder {
    @Override
    public Seq<Object> decode(Text record, StructType schema) throws IOException {
        BerTag tag = new BerTag(BerTag.UNIVERSAL_CLASS, BerTag.CONSTRUCTED, 16);
        InputStream is = new ByteArrayInputStream(record.getBytes());
        ArrayList<Object> sequence = new ArrayList<>();
        int codeLength = 0;
        int subCodeLength = 0;
        BerTag berTag = new BerTag();
        codeLength += tag.decodeAndCheck(is);
        BerLength length = new BerLength();
        codeLength += length.decode(is);

        int totalLength = length.val;
        codeLength += totalLength;

        subCodeLength += berTag.decode(is);

        if (berTag.equals(BerTag.APPLICATION_CLASS, BerTag.PRIMITIVE, 2)) {
            BerInteger recordNumber = new BerInteger();
            subCodeLength += recordNumber.decode(is, false);
            subCodeLength += berTag.decode(is);
            sequence.add(recordNumber.intValue());
        } else {
            throw new IOException("Tag does not match the mandatory sequence element tag.");
        }


        if (berTag.equals(BerTag.APPLICATION_CLASS, BerTag.PRIMITIVE, 8)) {
            BerUTF8String callingNumber = new BerUTF8String();
            subCodeLength += callingNumber.decode(is, false);
            subCodeLength += berTag.decode(is);
            sequence.add(callingNumber.toString());
        } else {
            throw new IOException("Tag does not match the mandatory sequence element tag.");
        }

        if (berTag.equals(BerTag.APPLICATION_CLASS, BerTag.PRIMITIVE, 9)) {
            BerUTF8String calledNumber = new BerUTF8String();
            subCodeLength += calledNumber.decode(is, false);
            subCodeLength += berTag.decode(is);
            sequence.add(calledNumber.toString());
        } else {
            throw new IOException("Tag does not match the mandatory sequence element tag.");
        }

        if (berTag.equals(BerTag.APPLICATION_CLASS, BerTag.PRIMITIVE, 16)) {
            BerUTF8String startDate = new BerUTF8String();
            subCodeLength += startDate.decode(is, false);
            subCodeLength += berTag.decode(is);
            sequence.add(startDate.toString());
        } else {
            throw new IOException("Tag does not match the mandatory sequence element tag.");
        }

        if (berTag.equals(BerTag.APPLICATION_CLASS, BerTag.PRIMITIVE, 18)) {
            BerUTF8String startTime = new BerUTF8String();
            subCodeLength += startTime.decode(is, false);
            subCodeLength += berTag.decode(is);
            sequence.add(startTime.toString());

        } else {
            throw new IOException("Tag does not match the mandatory sequence element tag.");
        }

        if (berTag.equals(BerTag.APPLICATION_CLASS, BerTag.PRIMITIVE, 19)) {
            BerInteger duration = new BerInteger();
            subCodeLength += duration.decode(is, false);
            sequence.add(duration.intValue());
        }

        return (Seq<Object>) JavaConverters.asScalaIteratorConverter(sequence.iterator()).asScala().toSeq();

    }
}
