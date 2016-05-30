import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class FlightFile extends FileInputFormat<LongWritable, FlightCustom>{

	@Override
	public RecordReader<LongWritable, FlightCustom> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new CustomRecordReader();
	}

}
