package flights;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Flight implements WritableComparable<Flight>{
	
	public IntWritable year = new IntWritable();
	public IntWritable month = new IntWritable();
	public IntWritable date = new IntWritable();
	public IntWritable delay = new IntWritable();

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		year.readFields(arg0);
		month.readFields(arg0);
		date.readFields(arg0);
		delay.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		year.write(arg0);
		month.write(arg0);
		date.write(arg0);
		delay.write(arg0);
	}

	public int compareTo(Flight arg0) {
		// TODO Auto-generated method stub
		int cv = this.year.compareTo(arg0.year);
		if(cv==0)
			cv = this.month.compareTo(arg0.month);
		if(cv==0)
			cv =  this.date.compareTo(arg0.date);
		if (cv==0)
			cv = -1 * this.delay.compareTo(arg0.delay);
		return cv;
	}
	public void setYear(IntWritable y)
	{
		this.year = y ;
	}
	public void setMonth(IntWritable m)
	{
		this.month = m ;
	}
	public void setDate(IntWritable d)
	{
		this.date = d ;
	}
	public void setDelay(IntWritable t)
	{
		this.delay = t;
	}

}
