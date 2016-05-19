package money;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MoneyGrouping extends WritableComparator {

	public MoneyGrouping()
	{
		super(Money.class,true);
	}
	
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		Money obj=(Money)a;
		Money obj1=(Money)b;
		return obj.country.compareTo(obj1.country);
	}

}
