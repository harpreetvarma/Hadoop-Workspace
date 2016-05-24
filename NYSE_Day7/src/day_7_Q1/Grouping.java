package day_7_Q1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Grouping extends WritableComparator {

	public Grouping()
	{
		super(CustomKey.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		CustomKey obj1 = (CustomKey)a;
		CustomKey obj2 = (CustomKey)b;
		return obj1.date.compareTo(obj2.date);
	}

}
