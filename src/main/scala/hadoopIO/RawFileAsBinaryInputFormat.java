package hadoopIO;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class RawFileAsBinaryInputFormat extends FileInputFormat<LongWritable,Text >  {

    @Override
    protected boolean isSplitable(JobContext context, Path filename){
        return true;
    }

    @Override
    public RecordReader<LongWritable , Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new MyFileRecordReader();
    }
}
