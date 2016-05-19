package money;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class MoneyDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		BasicConfigurator.configure();
		job.setJarByClass(money.MoneyDriver.class);
		job.setMapperClass(money.MoneyMapper.class);
		job.setReducerClass(money.MoneyReducer.class);
		job.setPartitionerClass(Moneypartition.class);
		job.setGroupingComparatorClass(MoneyGrouping.class);
		// TODO: specify output types
		job.setOutputKeyClass(Money.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/stock-input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/stock-input/output"));

		if (!job.waitForCompletion(true))
			return;
	}

}
