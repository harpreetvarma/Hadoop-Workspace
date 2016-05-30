import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer1 extends Reducer<CustomKey,Text,Text,Text>{

	@Override
	protected void reduce(CustomKey arg0, Iterable<Text> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		for(Text v:arg1)
		{
			arg2.write(new Text(arg0.year+","+arg0.month+","+arg0.date+","+arg0.arrDelay),new Text(""));
		}
		
	}

}
