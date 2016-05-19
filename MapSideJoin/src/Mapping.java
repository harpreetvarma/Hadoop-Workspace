import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Mapping extends Mapper<LongWritable, Text, Text, Text> {
	

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		String arr[]=ivalue.toString().split(",");
		if(arr[17].equals("SFO"))
		{
		int year=Integer.parseInt(arr[0].trim());
		int month=Integer.parseInt(arr[1].trim());
		int date=Integer.parseInt(arr[2].trim());
		String s=year+""+month+""+date;
	//	System.out.println(s+"----"+ivalue);
		if(hm.containsKey(s))
		context.write(ivalue, new Text(hm.get(s)));
		//context.write(new Text(s),ivalue);
		}
	}
	public HashMap<String,String> hm = new HashMap<String,String>();
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf= context.getConfiguration();
		FileSystem fs=FileSystem.getLocal( conf);
		 Path[] dataFile = DistributedCache.getLocalCacheFiles(conf);
		    // [0] because we added just one file.
		    String str = "";
		    BufferedReader cacheReader = new BufferedReader(new InputStreamReader(fs.open(dataFile[0])));
		    // now one can use BufferedReader's readLine() to read data
			while ((str = cacheReader.readLine()) != null) {    
					String[] arr=str.split(",");
					try
					{
					int year=Integer.parseInt(arr[1].trim());
					int month=Integer.parseInt(arr[2].trim());
					int date=Integer.parseInt(arr[3].trim());
					String s=year+""+month+""+date;
					String s1=arr[4]+","+arr[5]+","+arr[6];
					hm.put(s,s1);
			//		System.out.println(s+" "+s1);
					}
					catch(Exception e)
					{
						
					}
				} 
	}

}
