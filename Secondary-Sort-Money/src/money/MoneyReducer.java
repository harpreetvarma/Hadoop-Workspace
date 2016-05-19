package money;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MoneyReducer extends Reducer<Money, Text, Text, Text> {

	public void reduce(Money _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		String s="";
		for (Text val : values) {
			s=s+val.toString();
		}
		context.write(new Text(_key.country),new Text(s));
	}

}
