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

import java.io.EOFException;
import java.io.IOException;

public class MyFileRecordReader extends RecordReader<LongWritable, Text> {

    private Path filePath;

    private FSDataInputStream fileSystemInputStream;


    private long blockStartPosition, blockEndPosition, currentPosition = 0;


    private LongWritable currentKey = new LongWritable();
    private Text currentValue = new Text();


    @Override
    public boolean nextKeyValue() throws IOException {
        currentKey.set(currentPosition);
        currentValue.clear();
        int currentRecordSize = 0;

        long tempRecordSize = Integer.MAX_VALUE;


        while (currentPosition < blockEndPosition) {

            int localPosition = 0;
            int recordByte = 0;

            fileSystemInputStream.seek(currentPosition);
            while (fileSystemInputStream.getPos() < blockEndPosition) {
                recordByte = fileSystemInputStream.readByte();
                System.out.println("byte  : " + recordByte + "  position : " + fileSystemInputStream.getPos());
                localPosition++;
                if (localPosition == 2) {
                    tempRecordSize = recordByte + fileSystemInputStream.getPos();
                }
                if (fileSystemInputStream.getPos() <= tempRecordSize) {
                    byte[] b = {(byte) recordByte};
                    currentValue.append(b, 0, 1);
                } else {

                    currentPosition = tempRecordSize;
                    return true;
                }
            }
            currentRecordSize = (int) tempRecordSize;
            if (currentRecordSize == 0) {
                break;
            }
            currentPosition = tempRecordSize;
        }
        if (currentPosition != blockEndPosition && tempRecordSize != Integer.MAX_VALUE) {
            int i;
            System.out.println(fileSystemInputStream.getPos() + "**********" + tempRecordSize);
            while (fileSystemInputStream.getPos() < tempRecordSize) {
                i = fileSystemInputStream.readByte();
                System.out.println("byte spill : " + i + "  position : " + fileSystemInputStream.getPos());
                byte[] b = {(byte) i};
                currentValue.append(b, 0, 1);

            }
            return true;
        }
        if (currentRecordSize == 0) {
            currentKey = null;
            currentValue = null;
            return false;

        } else {
            return true;
        }

    }

    @Override
    public LongWritable getCurrentKey() {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() {
        return currentValue;
    }

    @Override
    public float getProgress() {
        if (blockStartPosition == blockEndPosition) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (currentPosition - blockStartPosition) / (float) (blockEndPosition - blockStartPosition));
        }
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException {

        Configuration conf = context.getConfiguration();

        FileSplit localFileBlock = (FileSplit) split;

        blockStartPosition = localFileBlock.getStart();
        blockEndPosition = blockStartPosition + localFileBlock.getLength();


        filePath = ((FileSplit) split).getPath();
        FileSystem fileSystem = filePath.getFileSystem(conf);
        fileSystemInputStream = fileSystem.open(filePath);


        if (blockStartPosition != 0) {
            fileSystemInputStream.seek(blockStartPosition);
            findRecordStart();
            //while (!isRecordStart(48)) ;
            blockStartPosition = findRecordStart();
            System.out.println("blockStartPosition " + blockStartPosition);
            if (blockStartPosition != -1) fileSystemInputStream.seek(blockStartPosition);
            else blockStartPosition = blockEndPosition;
        }


        currentPosition = blockStartPosition;


    }

    @Override
    public void close() throws IOException {
        if (fileSystemInputStream != null) fileSystemInputStream.close();
    }


    private boolean isRecordStart(int startingByte) throws IOException {
        if (fileSystemInputStream.getPos() < blockEndPosition) {

            if (blockStartPosition + 1 == blockEndPosition) {
                fileSystemInputStream.seek(blockEndPosition);
                return true;
            }

            int firstByte = fileSystemInputStream.readByte();
            int tempSize = fileSystemInputStream.readByte();
            long position = fileSystemInputStream.getPos();
            System.out.println(fileSystemInputStream.getPos() + " ++++++++++ " + blockEndPosition);

            if (firstByte == startingByte) {
                fileSystemInputStream.seek(tempSize + fileSystemInputStream.getPos());
                if (fileSystemInputStream.readByte() != startingByte) {
                    fileSystemInputStream.seek(fileSystemInputStream.getPos() - 1);
                    blockStartPosition = fileSystemInputStream.getPos();
                    return true;
                }

            } else {
                fileSystemInputStream.seek(position + 1);
                int i;
                while (fileSystemInputStream.getPos() < blockEndPosition) {
                    i = fileSystemInputStream.readByte();
                    if (i == startingByte) {
                        fileSystemInputStream.seek(fileSystemInputStream.getPos() - 1);
                        break;
                    }
                }
                return false;

            }
            return false;

        }
        return true;
    }

    private int findRecordStart() throws IOException {
        int i = 0;
        for (i = (int) blockStartPosition; i < blockEndPosition; i++) {
            fileSystemInputStream.seek(i);
            int startByte = fileSystemInputStream.readByte();
            if (startByte == 48) {
                System.out.println("48 position  " + fileSystemInputStream.getPos());
                fileSystemInputStream.seek(i + 1);
                int size = fileSystemInputStream.readByte();
                if (fileSystemInputStream.getPos() + size == blockEndPosition) {
                    System.out.println("end");
                    return i;
                } else {
                    try {
                        fileSystemInputStream.seek(fileSystemInputStream.getPos() + size);
                        int nextByte = fileSystemInputStream.readByte();
                        System.out.println("next byte  : " + nextByte);
                        if (nextByte == 48) {
                            System.out.println("start " + i);
                            return i;
                        }
                    }catch (EOFException e){
                        return -1;
                    }
                }

            }
        }
        return -1;
    }
}
