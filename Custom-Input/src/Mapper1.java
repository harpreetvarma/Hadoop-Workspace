import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Mapper1 extends Mapper<LongWritable,FlightCustom,Text,Text>{

	@Override
	protected void map(LongWritable key, FlightCustom value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		
		
	}

}
