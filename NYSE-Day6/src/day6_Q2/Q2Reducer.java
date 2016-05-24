package day6_Q2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q2Reducer extends Reducer<CustomKeyClass, Text, Text, Text> {

	public void reduce(CustomKeyClass _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		long totalVal=0;
		for (Text val : values) {

			int t = Integer.parseInt(val.toString());
			totalVal = totalVal + t;
		}
		context.write(new Text(_key.stock + "  " +_key.year+" "), new Text(totalVal+""));
	}

}
