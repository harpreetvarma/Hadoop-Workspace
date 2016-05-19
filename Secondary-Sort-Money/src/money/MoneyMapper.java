package money;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MoneyMapper extends Mapper<LongWritable, Text, Money, Text> {

	private Money m=new Money();
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {

		String a[]=ivalue.toString().split(",");
		// a[0] country
		// a[1] year
		// a[2] money
		try
		{
		m.setCountry(a[0]);
		m.setYear(a[1]);
		//m.setMoney(a[2]);
		context.write(m, new Text("("+a[1]+","+a[2]+")"));
		}
		catch (Exception e)
		{
			
		}
	}

}
