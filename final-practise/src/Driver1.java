import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;




public class Driver1 {

	public static void main(String []args) throws Exception
	{
		Job job = new Job();
		Configuration conf = job.getConfiguration();
		BasicConfigurator.configure();
		job.setJarByClass(Driver1.class);
		job.setMapperClass(Mapper1.class);
		DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/MapJoin/sfo_weather.csv"), conf);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/MapJoinDelay/flight_delays1.csv"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		if(job.waitForCompletion(true))
		{
			Job job1 = new Job();
			Configuration conf1 = job1.getConfiguration();
			
			job1.setJarByClass(Driver1.class);
			job1.setMapperClass(Mapper2.class);
			job1.setReducerClass(Reducer1.class);
			job1.setMapOutputKeyClass(CustomKey.class);
			job1.setOutputValueClass(Text.class);
			
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job1, new Path("output"));
			FileOutputFormat.setOutputPath(job1, new Path("output-new"));
			
			if(!job1.waitForCompletion(true))
				return;
		}
	}

}