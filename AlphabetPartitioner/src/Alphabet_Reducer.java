import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Alphabet_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text _key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
	     System.out.println(_key);
	     for ( IntWritable value : values) {
	    	 
			sum=sum+Integer.parseInt(value.toString());
		}

	     context.write(_key, new IntWritable(sum));
	}

}
