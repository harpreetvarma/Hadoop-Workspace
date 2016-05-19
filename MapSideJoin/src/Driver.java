import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import com.sun.jndi.toolkit.url.Uri;


public class Driver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Driver(),
				args);
		System.exit(res);
	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		BasicConfigurator.configure();
		DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/MapJoin/sfo_weather.csv"),conf);
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapping.class);
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/MapJoinDelay/flight_delays1.csv"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/MapJoin/hpv_out1"));
        int exitStatus = job.waitForCompletion(true) ? 0 : 1;	
		return exitStatus;
	}

}
