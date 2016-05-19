package sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondaryMapper extends Mapper<LongWritable, Text, Temperature, Text> {

	Temperature obj=new Temperature();
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		String arr[]=ivalue.toString().split(",");
		 String yearMonth = arr[0] + arr[1];
		obj.setYearMonth(yearMonth);
		obj.setDay(arr[2]);
		obj.setTemperature(arr[3]);
		context.write(obj, new Text(arr[3]));
	}

}
