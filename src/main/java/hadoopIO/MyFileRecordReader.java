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
            while (fileSystemInputStream.getPos() < tempRecordSize) {
                i = fileSystemInputStream.readByte();
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
            blockStartPosition = findRecordStart(fileSystemInputStream,blockStartPosition,blockEndPosition,5);
            if (blockStartPosition != -1) fileSystemInputStream.seek(blockStartPosition);
            else blockStartPosition = blockEndPosition;
        }

        currentPosition = blockStartPosition;


    }

    @Override
    public void close() throws IOException {
        if (fileSystemInputStream != null) fileSystemInputStream.close();
    }


    public int findRecordStart(FSDataInputStream fileSystemInputStream,long blockStartPosition, long blockEndPosition, int precisionFactor) throws IOException {

        int position = 0;
        for (position = (int) blockStartPosition; position < blockEndPosition; position++) {
            fileSystemInputStream.seek(position);
            int startByte = fileSystemInputStream.readByte();
            if (startByte == 48) {
                fileSystemInputStream.seek(position + 1);
                int sizeByte = fileSystemInputStream.readByte();
                if (fileSystemInputStream.getPos() + sizeByte == blockEndPosition) {
                    return position;
                } else {
                    try {
                        fileSystemInputStream.seek(fileSystemInputStream.getPos() + sizeByte);
                        int nextByte = fileSystemInputStream.readByte();
                        if (nextByte == 48) {
                            int res = precisionCheck(precisionFactor - 1, fileSystemInputStream.getPos(),fileSystemInputStream);
                            if (res != -1) return position;
                        }
                    } catch (EOFException e) {
                        return -1;
                    }
                }

            }
        }
        return -1;
    }


    public int precisionCheck(int precisionFactor, long init,FSDataInputStream fileSystemInputStream) throws IOException {
        if (precisionFactor == 0) return (int) init;
        int sizeByte = fileSystemInputStream.readByte();

        try {
            fileSystemInputStream.seek(fileSystemInputStream.getPos() + sizeByte);
            int nextByte = fileSystemInputStream.readByte();
            if (nextByte == 48) {
                int res = precisionCheck(precisionFactor - 1, fileSystemInputStream.getPos(),fileSystemInputStream);
                return res;
            }
        } catch (EOFException e) {
            return -1;
        }
        return -1;
    }
}
