package hadoopIO;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class RawFileAsBinaryInputFormat extends FileInputFormat<LongWritable,Text >  {

    @Override
    public RecordReader<LongWritable , Text> createRecordReader(
            InputSplit split, TaskAttemptContext context)  {
        return new MyFileRecordReader();
    }
}
