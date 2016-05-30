import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class part extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text arg0, Text arg1, int arg2) {
		// TODO Auto-generated method stub
		
		String a[]=arg1.toString().split(",");
		int year = Integer.parseInt(a[2]);
		
		if(year==2007)
		return 0;
		else
			return 1;
	}

}
