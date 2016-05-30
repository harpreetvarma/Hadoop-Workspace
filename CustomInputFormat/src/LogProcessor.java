import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class LogProcessor extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new LogProcessor(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		/* input parameters */
		// String inputPath = args[0];
		// String outputPath = args[1];
		// int numReduce = Integer.parseInt(args[2]);
		Job job = new Job(getConf(), "log-analysis");

		BasicConfigurator.configure();
		job.setJarByClass(LogProcessor.class);
		job.setMapperClass(LogProcessorMap.class);
		job.setReducerClass(LogProcessorReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(LogFileInputFormat.class);// specifying
															// InputFormatClass
															// here
		FileInputFormat.setInputPaths(job, new Path("ips_requests_input.txt"));
		FileOutputFormat.setOutputPath(job, new Path("outputPath"));
		// job.setNumReduceTasks(numReduce);

		int exitStatus = job.waitForCompletion(true) ? 0 : 1;
		return exitStatus;
	}
}
