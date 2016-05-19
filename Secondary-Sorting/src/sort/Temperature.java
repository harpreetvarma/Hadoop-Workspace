package sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Temperature implements WritableComparable<Temperature> {
	public Text yearMonth = new Text();
	public Text day = new Text();
	public Text temperature = new Text();

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		yearMonth.readFields(arg0);
		day.readFields(arg0);
		temperature.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		yearMonth.write(arg0);
		day.write(arg0);
		temperature.write(arg0);
	}

	public int compareTo(Temperature arg0) {
		// TODO Auto-generated method stub
		int compareValue = this.yearMonth.compareTo(arg0.yearMonth);
		if (compareValue == 0)
			compareValue = this.temperature.compareTo(arg0.temperature);
		return compareValue;
	}

	public void setYearMonth(String arg) {
		this.yearMonth.set(arg);
	}

	public void setDay(String arg) {
		this.day.set(arg);
	}

	public void setTemperature(String arg) {
		this.temperature.set(arg);
	}

	public Temperature() {
	}

	public Temperature(String yearMonth, String day, String temperature) {
		this.yearMonth.set(yearMonth);
		this.day.set(day);
		this.temperature.set(temperature);
	}

	@Override
	public boolean equals(Object arg0) {
		// TODO Auto-generated method stub
		final Temperature obj = (Temperature) arg0;
		return this.yearMonth.equals(obj.yearMonth) && this.day.equals(obj.day)
				&& this.temperature.equals(obj.temperature);
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		int hashCode = this.yearMonth != null ? yearMonth.hashCode() : 0;
		hashCode = 30 * hashCode
				+ (this.temperature != null ? temperature.hashCode() : 0);
		return hashCode;
	}

}
