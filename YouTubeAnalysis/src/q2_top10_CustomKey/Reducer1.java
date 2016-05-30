package q2_top10_CustomKey;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Custom,Text,Text,Text> {

	int i=0;
	@Override
	protected void reduce(Custom arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		for(Text val:arg1)
		{
			if(i++<10)
			arg2.write(new Text(arg0.id+" "+arg0.rating), new Text(""));
			else
				break;
		}
		
	}

}
