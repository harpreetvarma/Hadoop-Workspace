import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer1 extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
	
		long i = Long.MIN_VALUE;
		String s="";
		for(Text t:arg1)
		{
			String arr[]=t.toString().split(",");
			long k=Long.parseLong(arr[0]);
			if(k>i)
			{
				i=k;
				s=t.toString();
			}
		}
		arg2.write(new Text(arg0+","+s), new Text(""));
	}

}
