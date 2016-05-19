import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MatrixAdditionReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	private Text word = new Text();
	public void reduce(LongWritable _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		int a[]=new int[4];
		for (Text val : values) {
			String b[]=val.toString().split(" ");
			
			for(int i=0;i<b.length;i++)
			{
				a[i]=a[i]+Integer.parseInt(b[i]);
			}
			
		}
		
		word.set(Arrays.toString(a));
		context.write(_key, word);
	}

}
