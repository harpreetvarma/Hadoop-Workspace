package geoLocation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GeoMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {

		String a[]=ivalue.toString().split(",");
		// a[0] driver id
		// a[1] truck id
		// a[2] model
		// a[3]+2  miles
		// a[4]+2  gas
		
		// for loop for miles
		try
		{
		int miles=0;
		for(int i=3;i<a.length;i+=2)
		{
			miles=miles+Integer.parseInt(a[i].toString().trim());
		}
		// for loop for gas
		int gas=0;
		for(int i=4;i<a.length;i+=2)
		{
			gas=gas+Integer.parseInt(a[i].toString().trim());
		}
		context.write(new Text(a[0]), new Text(miles+","+gas+","+miles/gas));
		}
		catch(Exception e)
		{
		}
	}

}
