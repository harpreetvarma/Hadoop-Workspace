

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
public class Alphabet_Partitioner extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text arg0, IntWritable arg1, int arg2) {
		// TODO Auto-generated method stub
		String t=arg0.toString();
		
		if(t.charAt(0)=='a'||t.charAt(0)=='A')
			return ((1) % arg2);
                else if(t.charAt(0)=='b'||t.charAt(0)=='B')
			return ((2) % arg2);
                else
                	return ((3) % arg2);
		//return 0;
	}

}
