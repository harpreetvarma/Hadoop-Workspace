package map_side_join_practise;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;



//00:57
public class Join_Driver {

	public static void main(String[] args) throws Exception {
	
		Job job = new Job();
		Configuration conf = job.getConfiguration();
		BasicConfigurator.configure();
		DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/MapJoin/sfo_weather.csv"), conf);
		
		job.setJarByClass(map_side_join_practise.Join_Driver.class);
		job.setMapperClass(map_side_join_practise.Mapper1.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("hdfs://localhost:9000/MapJoinDelay/flight_delays1.csv"));
		FileOutputFormat.setOutputPath(job, new Path("Map-Side-Output"));
		
		if(!job.waitForCompletion(true))
			return;
		
		
	}

}
