import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, Text, Text, IntWritable> {

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		int i = Integer.MIN_VALUE;

		for (Text val : arg1) {
			if (Integer.parseInt(val.toString()) > i) {
				i = Integer.parseInt(val.toString());
			}

		}

		context.write(arg0, new IntWritable(i));
	}

}
