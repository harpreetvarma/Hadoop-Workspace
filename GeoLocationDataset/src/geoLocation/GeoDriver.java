package geoLocation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class GeoDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		BasicConfigurator.configure();
		job.setJarByClass(geoLocation.GeoDriver.class);
		job.setMapperClass(geoLocation.GeoMapper.class);

		job.setReducerClass(geoLocation.GeoReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/GeoLocationDataset/trucks.csv"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/GeoLocationDataset/out"));

		if (!job.waitForCompletion(true))
			return;
	}

}
