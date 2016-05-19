package sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondaryGroupingComparator extends WritableComparator {

	public SecondaryGroupingComparator()
	{
		super(Temperature.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		Temperature obj=(Temperature)a;
		Temperature obj1=(Temperature)b;
		return obj.yearMonth.compareTo(obj1.yearMonth);
	}

}
