package sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class SecondaryDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		BasicConfigurator.configure();
		job.setJarByClass(sort.SecondaryDriver.class);
		job.setMapperClass(sort.SecondaryMapper.class);
		job.setReducerClass(sort.SecondaryReducer.class);
		// TODO: specify output types
		job.setOutputKeyClass(Temperature.class);
		job.setOutputValueClass(Text.class);
		
		job.setPartitionerClass(SecondaryPartitioner.class);
		job.setGroupingComparatorClass(SecondaryGroupingComparator.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/temp/Dates_Temparature.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/temp/hpv_output"));

		if (!job.waitForCompletion(true))
			return;
	}

}
