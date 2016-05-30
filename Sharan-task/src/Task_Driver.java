import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;


public class Task_Driver {

	public static void main(String []args) throws Exception
	{
		Job job = new Job();
		BasicConfigurator.configure();
		job.setJarByClass(Task_Driver.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setPartitionerClass(part.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("hdfs://localhost:9000/MapJoinDelay/flight_delays1.csv"));
		FileOutputFormat.setOutputPath(job, new Path("Task1"));
		
		if(!job.waitForCompletion(true))
			return;
	}
	
	
	

}
