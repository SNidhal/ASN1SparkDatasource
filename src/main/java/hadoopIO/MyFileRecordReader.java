package hadoopIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.bouncycastle.asn1.ASN1InputStream;
import org.bouncycastle.asn1.ASN1Primitive;

import java.io.IOException;
import java.io.InputStream;

public class MyFileRecordReader extends RecordReader<LongWritable ,Text >  {
    private Path filePath;

    private FSDataInputStream fsin;


    private long blockStartPosition,blockEndPosition,currentPosition=0;


    private  LongWritable currentKey=new LongWritable();
    private Text currentValue=new Text();
    private boolean isProcessed = false;


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        currentKey.set(currentPosition+1);
        currentValue.clear();
        int newSize=0;
        long taille =Integer.MAX_VALUE;


        while(currentPosition<blockEndPosition){

            int cmp=0;
            int i;

            fsin.seek(currentPosition);
            while( (i=fsin.readByte())!=-1 && fsin.getPos()<blockEndPosition){
                cmp++;
                if (cmp==2){
                    taille=i+fsin.getPos();
                }
                if(fsin.getPos()<=taille) {
                    byte[] b = {(byte) i};
                    currentValue.append(b, 0, 1);
                }else {

                    currentPosition=taille;
                    return true;
                }
            }
            byte[] b = {(byte) i};
            currentValue.append(b, 0, 1);
            newSize= (int) taille;
          if(newSize==0){
                break;
            }
            currentPosition=taille;
        }
        if(currentPosition!=blockEndPosition)  {
            int i;
            while( (i=fsin.readByte())!=-1  && fsin.getPos()==taille){

                    byte[] b = {(byte) i};
                    currentValue.append(b, 0, 1);

            }
        }
        if(newSize==0){
            currentKey=null;
            currentValue=null;
            return false;

        }else {
            return true;
        }

    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public  Text getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(blockStartPosition==blockEndPosition){
            return 0.0f;
        }else{
            return Math.min(1.0f,(currentPosition-blockStartPosition)/(float)(blockEndPosition-blockStartPosition));
        }
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException {

        Configuration conf = context.getConfiguration();

        FileSplit localFileBlock =(FileSplit) split;

        blockStartPosition=localFileBlock.getStart();
        blockEndPosition =blockStartPosition+localFileBlock.getLength();


        filePath = ((FileSplit) split).getPath();
        FileSystem fileSystem = filePath.getFileSystem(conf);
        fsin = fileSystem.open(filePath);

        if(blockStartPosition!=0){

            while(isRecordStart(48)==false);

            blockStartPosition=fsin.getPos();

        }


        currentPosition=blockStartPosition;


    }

    @Override
    public void close() throws IOException {
        if (fsin!=null) fsin.close();
    }



    public boolean isRecordStart( int startingByte) throws IOException{

        int firstByte = fsin.readByte();
        int tempSize= fsin.readByte();
        long position=fsin.getPos();


        if(firstByte==startingByte){
            fsin.seek(tempSize+fsin.getPos());
            if(fsin.readByte()!= startingByte){
                fsin.seek(fsin.getPos()-1);
                blockStartPosition =fsin.getPos();
                return true;
            }

        }else{
            fsin.seek(position+1);
            int i;
            while((i=fsin.readByte())!=-1){
                if(i==startingByte){
                    fsin.seek(fsin.getPos()-1);
                    break;
                }
            }
            return false;

        }
       return false;
    }
}
