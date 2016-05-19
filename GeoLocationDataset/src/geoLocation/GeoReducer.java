package geoLocation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GeoReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		String out="";
		for (Text val : values) {
			out=out+val.toString();
		}
		context.write(_key, new Text(out));
	}

}
