import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class LogProcessorReduce extends
		Reducer<Text,IntWritable,Text,IntWritable> {

   public void reduce(Text key, Iterable<Text> values, 
                      Context context) throws IOException, InterruptedException {
     int sum = 0;
     System.out.println(key);
     for ( Text value : values) {
    	 
		sum=sum+Integer.parseInt(value.toString());
	}

     context.write(key, new IntWritable(sum));
   }
}

