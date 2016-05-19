package reduce_join;


//Datasets : http://10.11.61.90/Datasets/Day_7_Datasets/Hadoop_Reducer_Side_Joins_Example/

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DeptProcessorMap extends Mapper<LongWritable, Text, Text, Text > {

	private String fileTag="DF";//this tag is to differentiate the input file data at reducer side.
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {		
		
		 //taking one line/record at a time and parsing them into key value pairs
		String line = value.toString();
//		String splitarray[] = line.split(",");
//		int deptNumber = Integer.parseInt(splitarray[0].trim());
//		String deptName = splitarray[1].trim();
//		String location = splitarray[2].trim();
//		
//		context.write(new IntWritable(deptNumber),new Text(fileTag+','+deptName+','+location));
		try
		{
		String[] arr=value.toString().split(",");
		int year=Integer.parseInt(arr[1]);
		int month=Integer.parseInt(arr[2]);
		int date=Integer.parseInt(arr[3]);
		System.out.println(arr[6]+"sfo");
		if(key.get()!=0)
		context.write(new Text(year+""+month+""+date),new Text(arr[4]+","+arr[5]+","+arr[6]));
		}
		catch(Exception e)
		{
			
		}
	}
}
