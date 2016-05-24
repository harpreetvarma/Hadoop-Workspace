package pack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;




public class Q1_Driver {

	public static void main(String[] args) throws Exception {
		Job job  = new Job();
		Configuration conf = job.getConfiguration(); 
		BasicConfigurator.configure();
		job.setJarByClass(pack.Q1_Driver.class);
		job.setMapperClass(pack.Example_Mapper.class);
		job.setReducerClass(pack.Example_Reducer.class);
		job.setPartitionerClass(pack.partitioner.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setNumReduceTasks(2);
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/MapJoinDelay/flight_delays1.csv"));
		FileOutputFormat.setOutputPath(job, new Path("output"));	
		
		if(!job.waitForCompletion(true))
			return;
		
	}

}
