package day6_Q2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Q2Driver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		BasicConfigurator.configure();
		job.setJarByClass(day6_Q2.Q2Driver.class);
		job.setMapperClass(day6_Q2.Q2Mapper.class);

		job.setReducerClass(day6_Q2.Q2Reducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(CustomKeyClass.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("nyse_2014.csv"));
		FileOutputFormat.setOutputPath(job, new Path("Q2-output"));

		if (!job.waitForCompletion(true))
			return;
	}

}
