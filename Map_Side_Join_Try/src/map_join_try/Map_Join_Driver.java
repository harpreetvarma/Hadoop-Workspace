package map_join_try;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Map_Join_Driver extends Configured {

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		Configuration conf=job.getConfiguration();
		BasicConfigurator.configure();
		job.setJarByClass(map_join_try.Map_Join_Driver.class);
		job.setMapperClass(map_join_try.Map_Join_Mapper.class);
		DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/MapJoin/sfo_weather.csv"), conf);
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/MapJoinDelay/flight_delays1.csv"));
		FileOutputFormat.setOutputPath(job, new Path("output_new"));

		if (job.waitForCompletion(true))
		{
			Job job1 = new Job();
			Configuration conf1=job1.getConfiguration();
			
			job1.setJarByClass(map_join_try.Map_Join_Driver.class);
			job1.setMapperClass(map_join_try.Flight_Mapper.class);
			job1.setReducerClass(map_join_try.Flight_Reducer.class);
			
			// TODO: specify output types
			job1.setOutputKeyClass(Flight.class);
			job1.setOutputValueClass(Text.class);

			// TODO: specify input and output DIRECTORIES (not files)
			FileInputFormat.setInputPaths(job1, new Path("output_new"));
			FileOutputFormat.setOutputPath(job1, new Path("output1"));
			
			if (!job1.waitForCompletion(true))
			{
				return;
			}
			return;
		}
	}

}
