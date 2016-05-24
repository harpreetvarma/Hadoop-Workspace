package pack;

import org.apache.hadoop.conf.Configuration;

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.log4j.BasicConfigurator;
public class Driving {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		BasicConfigurator.configure();
		DistributedCache.addCacheFile(new URI("sfo_weather.csv"),conf);
		job.setJarByClass(pack.Driving.class);
		job.setMapperClass(pack.Mapping.class);

		// TODO: specify a reducer
	//	job.setReducerClass(Reducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("flight_delays1.csv"));
		FileOutputFormat.setOutputPath(job, new Path("output"));

		if (!job.waitForCompletion(true))
			return;
	}

}
