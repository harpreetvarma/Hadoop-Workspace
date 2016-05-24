package day6_Q1;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q1Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		long totalValue=0;
		int count = 0 ;
		float avg = 0;
		for (Text val : values) {
			int t = Integer.parseInt(val.toString());
			totalValue = totalValue + t;
			count++;
		}
		//avg = totalValue / count ;
		context.write(_key, new Text("Total Value is -->"+totalValue+"............... "+"count is -->"+count+"...................."+"Average is-->"+totalValue/count));
	}

}
