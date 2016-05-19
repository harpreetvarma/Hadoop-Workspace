package top10;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10Reducer extends Reducer<Text, Text, Text, Text> {

	private TreeMap<Integer,String> valueMap=new TreeMap<Integer,String>();
	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		
		// _key category
		//a[0] id
		//a[1] views
		for (Text val : values) {
			String a[]=val.toString().split(",");
			valueMap.put(Integer.parseInt(a[1]), a[0]);
			if(valueMap.size()>10)
				valueMap.remove(valueMap.firstKey());
		}
		for(int i:valueMap.keySet())
		{
			String video_id=valueMap.get(i);
			context.write(new Text(_key+"  "+video_id),new Text(i+""));
		}
	}
	
}
