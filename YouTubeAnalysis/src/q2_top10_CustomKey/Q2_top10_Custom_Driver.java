package q2_top10_CustomKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import q5_top10_CustomKey.CustomKey;



public class Q2_top10_Custom_Driver {

	public static void main(String []args) throws Exception
	{
		Job job = new Job();
		Configuration conf = job.getConfiguration();
		BasicConfigurator.configure();
		job.setJarByClass(q2_top10_CustomKey.Q2_top10_Custom_Driver.class);
		job.setMapperClass(q2_top10_CustomKey.Mapper1.class);
		job.setReducerClass(q2_top10_CustomKey.Reducer1.class);
		
		job.setMapOutputKeyClass(Custom.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("0.txt"));
		FileOutputFormat.setOutputPath(job, new Path("Q2-Custom-top10"));
		
		if(!job.waitForCompletion(true))
			return;
	}

}
