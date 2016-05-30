import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class CustomInputFormat extends FileInputFormat<LongWritable,CustomEmp>{

	@Override
	public RecordReader<LongWritable, CustomEmp> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new CustomRecordReader();
	}

}
