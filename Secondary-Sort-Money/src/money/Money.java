package money;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Money implements WritableComparable<Money> {

	public Text country=new Text();
	public Text year = new Text();
	//public Text money = new Text();
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		country.readFields(arg0);
		year.readFields(arg0);
		//money.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		country.write(arg0);
		year.write(arg0);
		//money.write(arg0);
		
	}

	public int compareTo(Money arg0) {
		// TODO Auto-generated method stub
		int cv=this.country.compareTo(arg0.country);
		if(cv==0)
			cv=this.year.compareTo(arg0.year);
//		if(cv==0)
//			cv=this.money.compareTo(arg0.money)*-1;
		return cv;
	}

	public void setCountry(String arg) {
		this.country.set(arg);
	}

//	@Override
//	public boolean equals(Object arg0) {
//		// TODO Auto-generated method stub
//		final Money obj=(Money)arg0;
//		return this.country.equals(obj.country)&&this.year.equals(obj.year)&&this.money.equals(obj.money);
//	}
//
//	@Override
//	public int hashCode() {
//		// TODO Auto-generated method stub
//		int hashCode = this.year != null ? year.hashCode() : 0;
//		hashCode = 30 * hashCode
//				+ (this.money != null ? money.hashCode() : 0);
//		return hashCode;
//	}

	public void setYear(String arg) {
		this.year.set(arg);
	}

//	public void setMoney(String arg) {
//		this.money.set(arg);
//	}
}
