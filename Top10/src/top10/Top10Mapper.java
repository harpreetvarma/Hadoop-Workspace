package top10;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10Mapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		String a[]=ivalue.toString().trim().split("\t");
		
		// a[0] videoId
		// a[3] category
		// a[5] views
		try
		{
		context.write(new Text(a[3].trim()),new Text(a[0].trim()+","+a[5].trim()));
		}
		catch(Exception e)
		{
			
		}
		
	}

}
