package q5_top_10_most_viewed_videos;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q5_Mapper extends Mapper<LongWritable,Text,Text,Text>{

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		String line[] = value.toString().split("\t");
		System.out.println(line[0]+" "+line[3]+" "+line[5]);
		try
		{
		context.write(new Text(line[3]), new Text(line[0]+","+line[5]));
		}
		catch(Exception e)
		{
			
		}
		
	}
	
	

}
