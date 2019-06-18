package util;

import model.CallDetailRecord;
import org.bouncycastle.asn1.ASN1InputStream;
import org.bouncycastle.asn1.ASN1Primitive;
import org.bouncycastle.asn1.ASN1Sequence;

import java.io.*;

public class StandaloneDecoder {
    public StandaloneDecoder(String filename) throws IOException {

        File fileIn = new File(filename);
        FileInputStream fin = new FileInputStream(fileIn);
        InputStream is=decompressStream(fin);

        ASN1InputStream asnin = new ASN1InputStream(is);
        ASN1Primitive obj = null;

        while ((obj = asnin.readObject()) != null) {

            CallDetailRecord thisCdr = new CallDetailRecord((ASN1Sequence) obj);

            System.out.println("CallDetailRecord "+thisCdr.getRecordNumber()+" Calling "+thisCdr.getCallingNumber()
                    +" Called "+thisCdr.getCalledNumber()+ " Start Date-Time "+thisCdr.getStartDate()+"-"
                    +thisCdr.getStartTime()+" duration "+thisCdr.getDuration()
            );

        }

        asnin.close();
        is.close();
        fin.close();
    }

    public static InputStream decompressStream(InputStream input) {
        InputStream returnStream=null;
        org.apache.commons.compress.compressors.CompressorInputStream cis = null;
        BufferedInputStream bis=null;
        try {
            bis = new BufferedInputStream(input);
            bis.mark(1024);   //Mark stream to reset if uncompressed data
            cis = new org.apache.commons.compress.compressors.CompressorStreamFactory().createCompressorInputStream(bis);
            returnStream = cis;
        } catch (org.apache.commons.compress.compressors.CompressorException ce) { //CompressorStreamFactory throws CompressorException for uncompressed files
            try {
                bis.reset();
            } catch (IOException ioe) {
                String errmessageIOE="IO Exception ( "+ioe.getClass().getName()+" ) : "+ioe.getMessage();
                System.out.println(errmessageIOE);
            }
            returnStream = bis;
        } catch (Exception e) {
            String errmessage="Exception ( "+e.getClass().getName()+" ) : "+e.getMessage();
            System.out.println(errmessage);
        }
        return returnStream;
    }

    public static void main(String[] args) {



        String filename = "test.ber";
        try {
            @SuppressWarnings("unused")
            StandaloneDecoder mainObj = new StandaloneDecoder(filename);
        } catch (IOException ioe) {
            String errmessage="ERROR. EXITING. Exception ( "+ioe.getClass().getName()+" ) : "+ioe.getMessage();
            System.out.println(errmessage);
            ioe.printStackTrace();
            System.exit(1);
        }
    }
}
