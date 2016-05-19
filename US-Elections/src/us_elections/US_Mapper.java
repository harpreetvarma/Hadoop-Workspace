package us_elections;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class US_Mapper extends Mapper<LongWritable, Text, US_ELECTIONS, LongWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {

		US_ELECTIONS object = new US_ELECTIONS();
		String lineArray[]=ivalue.toString().split(",");
		//lineArray [0] = state 
		// lineArray [4] = party 
		// lineArray [6] = votes
		try
		{
			if(lineArray[0]!=null && lineArray[4]!=null && lineArray[6]!=null)
			{
			object.setParty(lineArray[4]);
			object.setState(lineArray[0]);
			object.setVotes(new LongWritable(Long.parseLong(lineArray[6])));
		context.write(object,new LongWritable(Long.parseLong(lineArray[6])));
			}
		}
		catch (Exception e)
		{
			System.out.print("Exception occured"+e);
		}
	}

}
