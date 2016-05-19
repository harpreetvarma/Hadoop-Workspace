package sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondaryPartitioner extends Partitioner<Temperature,Text> {

	@Override
	public int getPartition(Temperature arg0, Text arg1, int arg2) {
		// TODO Auto-generated method stub
		return Math.abs(arg0.yearMonth.hashCode()%arg2);
	}

}
