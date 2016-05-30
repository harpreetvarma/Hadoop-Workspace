import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;


public class CustomKey implements WritableComparable<CustomKey>{


	public IntWritable year = new IntWritable();
	public IntWritable month = new IntWritable();
	public IntWritable date= new IntWritable();
	public IntWritable arrDelay = new IntWritable();
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		year.readFields(arg0);
		month.readFields(arg0);
		date.readFields(arg0);
		arrDelay.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		year.write(arg0);
		month.write(arg0);
		date.write(arg0);
		arrDelay.write(arg0);
	}

	public int compareTo(CustomKey arg0) {
		// TODO Auto-generated method stub
		int cv = this.year.compareTo(arg0.year);
		if(cv==0)
			cv=this.month.compareTo(arg0.month);
		if(cv==0)
			cv=this.date.compareTo(arg0.date);
		if(cv==0)
			cv=-1*this.arrDelay.compareTo(arg0.arrDelay);
		return cv;
	}
	

	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = year;
	}

	public IntWritable getMonth() {
		return month;
	}

	public void setMonth(IntWritable month) {
		this.month = month;
	}

	public IntWritable getDate() {
		return date;
	}

	public void setDate(IntWritable date) {
		this.date = date;
	}

	public IntWritable getArrDelay() {
		return arrDelay;
	}

	public void setArrDelay(IntWritable arrDelay) {
		this.arrDelay = arrDelay;
	}

}
