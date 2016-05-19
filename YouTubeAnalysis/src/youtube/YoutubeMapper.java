package youtube;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YoutubeMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private TreeMap<Float,String> orderingMap = new TreeMap<Float,String>();
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		String a[]=StringUtils.split(ivalue.toString().trim(), "\t");
		// a[6] rating
		// a[0] id
		
		if(a[6]!=null && a[0]!=null)
		{
		orderingMap.put(Float.parseFloat(a[6]), a[0].trim()+" "+a[6].trim());
		if(orderingMap.size()>10)
		{
			orderingMap.remove(orderingMap.firstKey());
		}
		
		}
	}
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(String s:orderingMap.values())
		{
			context.write(NullWritable.get(), new Text(s));
			
		}
	}

}
