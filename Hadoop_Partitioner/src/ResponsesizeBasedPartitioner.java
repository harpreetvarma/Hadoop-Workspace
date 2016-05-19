
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class ResponsesizeBasedPartitioner extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text ipAddress, IntWritable value, int numPartitions) {
		
		
		if(value.get()<=25000)
			return ((1) % numPartitions);
                else if((value.get()>25001)&&(value.get()<=50000))
			return ((2) % numPartitions);
		else if((value.get()>50000)&&(value.get()<=100000))
			return ((3) % numPartitions);
		else if(value.get()>100000)
			return ((4) % numPartitions);
		else
		return 0;
	}
}

