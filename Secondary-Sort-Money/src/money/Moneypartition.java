package money;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

public class Moneypartition extends Partitioner<Money,Text>{

	public int getPartition(Money arg0, Text arg1, int arg2) {
		// TODO Auto-generated method stub
		return Math.abs(arg0.country.hashCode()%arg2);
	}

}
