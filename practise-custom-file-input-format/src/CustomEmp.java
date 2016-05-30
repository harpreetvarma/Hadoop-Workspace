import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class CustomEmp implements WritableComparable<CustomEmp>{
	
	public IntWritable sal = new IntWritable();
	public Text cat = new Text();
	
	
	public void set(String sal,String cat)
	{
		this.sal.set(Integer.parseInt(sal));
		this.cat.set(cat);
	}

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		sal.readFields(arg0);
		cat.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		sal.write(arg0);
		cat.write(arg0);
	}

	public int compareTo(CustomEmp arg0) {
		// TODO Auto-generated method stub
		int cv = this.cat.compareTo(arg0.cat);
		if(cv==0)
			cv = -1*this.sal.compareTo(arg0.sal);
		return cv;
	}

}
