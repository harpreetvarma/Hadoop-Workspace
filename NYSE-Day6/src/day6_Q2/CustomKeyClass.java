package day6_Q2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomKeyClass implements WritableComparable<CustomKeyClass>{

	public Text year =new Text();
	public Text stock = new Text();
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		year.readFields(arg0);
		stock.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		year.write(arg0);
		stock.write(arg0);
	}

	public int compareTo(CustomKeyClass arg0) {
		// TODO Auto-generated method stub
		int cv  = this.year.compareTo(arg0.year);
		if (cv == 0)
			cv = this.stock.compareTo(arg0.stock);
		return cv;
	}
	
	public void setYear(Text y)
	{
		this.year = y;
	}
	public  void setStock(Text s)
	{
		this.stock = s;
	}

}
