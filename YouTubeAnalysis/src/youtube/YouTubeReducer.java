package youtube;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class YouTubeReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		String s1="";
		for (Text val : values) {
			s1=s1+val.toString();
		}
		context.write(NullWritable.get(), new Text(s1));
	}

}
