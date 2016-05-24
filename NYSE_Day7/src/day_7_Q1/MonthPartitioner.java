package day_7_Q1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MonthPartitioner extends Partitioner<CustomKey,Text> {

	@Override
	public int getPartition(CustomKey arg0, Text arg1, int arg2) {
		// TODO Auto-generated method stub
		String s=arg0.date.toString();
		int i=s.indexOf("-");
		int j=s.lastIndexOf("-");
		String s1=s.substring(i+1,j);
		if(s1.equals("Jan"))
			return 0;
		else if(s1.equals("Feb"))
			return 1;
		else if(s1.equals("Mar"))
			return 2;
		else if(s1.equals("Apr"))
			return 3;
		else if(s1.equals("May"))
			return 4;
		else if(s1.equals("Jun"))
			return 5;
		else if(s1.equals("Jul"))
			return 6;
		else if(s1.equals("Aug"))
			return 7;
		else if(s1.equals("Sep"))
			return 8;
		else if(s1.equals("Oct"))
			return 9;
		else if(s1.equals("Nov"))
			return 10;
		else
			return 11;
	}

}
