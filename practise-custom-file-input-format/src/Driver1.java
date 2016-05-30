import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;




public class Driver1 {

	public static void main(String[] args) throws Exception {
		
		Job job = new Job();
		Configuration conf = job.getConfiguration();
		BasicConfigurator.configure();
		job.setJarByClass(Driver1.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("custom-file-input.csv"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		job.setInputFormatClass(CustomInputFormat.class);
		//job.setMapperClass(Mapper1.class);
		
		if(!job.waitForCompletion(true))
			return;
	}

}
