import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Alphabet_Driver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Alphabet_Driver(),
				args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage:  <input_path> <output_path> <num_reduce_tasks>");
			System.exit(-1);
		}

		/* input parameters */
		String inputPath = args[0];	
		String outputPath = args[1];
		int numReduce = Integer.parseInt(args[2]); 

		Configuration conf = new Configuration();
		Job job = new Job(conf, "log-analysis");

		job.setJarByClass(Alphabet_Driver.class);
		job.setMapperClass(Alphabet_Mapper.class);
		job.setReducerClass(Alphabet_Reducer.class);

	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		job.setNumReduceTasks(numReduce);

	 	job.setPartitionerClass(Alphabet_Partitioner.class);//add partitioner class here
		int exitStatus = job.waitForCompletion(true) ? 0 : 1;
		return exitStatus;
	}
}

