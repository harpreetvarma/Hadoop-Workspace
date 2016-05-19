package analysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ComplaintMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {

		String a[]=ivalue.toString().split(",");
		// a[17] complaint id
		// a[7] company
		try
		{
			context.write(new Text(a[7]), new IntWritable(1));
		}
		catch (Exception e)
		{
			
		}
	}

}
