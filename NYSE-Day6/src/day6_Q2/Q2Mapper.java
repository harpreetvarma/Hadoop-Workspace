/**
 * 
 * use the NYSE dataset with following attributes:
stockTicker – stock name or symbol   0
tradeDate   1
openingPrice  2
highPrice 3
lowPrice 4
closingPrice 5
volume  6

2.	Find the Total Volume per Year per Stock 
 */


package day6_Q2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q2Mapper extends Mapper<LongWritable, Text, CustomKeyClass, Text> {

	CustomKeyClass obj ;
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		obj = new CustomKeyClass();
		String line[]=ivalue.toString().split(",");
		String year=line[1].substring(7,line[1].length());
		String stock = line[0];
		String vol = line [6];
		obj.setStock(new Text(stock));
		obj.setYear(new Text(year));
		
		context.write(obj,new Text(vol));

	}

}
