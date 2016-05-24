/**
 * 
 * 
 * Year 0	Month 1	DayofMonth 2	DayOfWeek 3	  DepTime 4	  CRSDepTime 5	ArrTime 6	CRSArrTime 7	UniqueCarrier 8	
 * FlightNum  9  	TailNum  10	 ActualElapsedTime  11	CRSElapsedTime	  12   AirTime  13	ArrDelay 14	DepDelay 15
 * Origin 16	Dest  17	Distance 18	 TaxiIn	19   TaxiOut 20   Cancelled  21   CancellationCode 22	Diverted 23
 * CarrierDelay 24	WeatherDelay 25  NASDelay 26	SecurityDelay 27	LateAircraftDelay 28

 * 
 * 0 1 2 4 6 8 9 11 14 15 16 17 
 * 
 */


package map_join_try;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;


public class Map_Join_Mapper extends Mapper<LongWritable, Text, Text, Text> {
	private HashMap<String, String> hm = new HashMap<String, String>();
	private BufferedReader br;
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.getLocal(conf);
		Path[] dataFiles = DistributedCache.getLocalCacheFiles(conf);
		String str = "";
		br = new BufferedReader(new InputStreamReader(fs.open(dataFiles[0])));
		while ((str = br.readLine()) != null) {
			try
			{
			String arr[] = str.split(",");
			int year=Integer.parseInt(arr[1]);
			int month=Integer.parseInt(arr[2]);
			int date=Integer.parseInt(arr[3]);
			hm.put(year+""+month+""+date,arr[4]+","+arr[5]+","+arr[6]);
			//System.out.println(arr[1]+" "+arr[2]+" "+arr[3]+" "+arr[4]+","+arr[5]+","+arr[6]);
			}
			catch(Exception e)
			{
				
			}
		}
	}

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		String s1="";
		String arr[]=ivalue.toString().split(",");
		if(arr[17].equals("SFO"))
		{
		int year=Integer.parseInt(arr[0]);
		int month=Integer.parseInt(arr[1]);
		int date=Integer.parseInt(arr[2]);
		s1=s1+year+""+month+""+date;
		System.out.println(s1);
		if(hm.containsKey(s1))
		{
		System.out.println(ivalue);
		context.write(new Text(arr[0]+","+arr[1]+","+arr[2]+","+arr[4]+","+arr[6]+","+arr[8]+","+arr[9]+","+arr[11]+","+arr[14]+","+arr[15]+","+arr[16]+","+arr[17]+","+hm.get(s1)),new Text(""));
		}
		}
	}
}
