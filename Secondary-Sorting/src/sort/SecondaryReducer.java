package sort;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondaryReducer extends Reducer<Temperature, Text, Text, Text> {

	public void reduce(Temperature key, Iterable<Text> values, Context context) 
	    	throws IOException, InterruptedException {
	    	StringBuilder builder = new StringBuilder();
		int count=0;
		builder.append("(");
	    	for (Text value : values) {
	    		
			if(count!=0){
	    		builder.append(",");
			builder.append(value.toString());
			}
			else{
			builder.append(value.toString());
			}
			count++;
			}
		builder.append(")");
		String Year=(key.yearMonth.toString()).substring(0, 4);
		String Month=(key.yearMonth.toString()).substring(4, key.yearMonth.toString().length());
		
	        context.write(new Text(Year+"-"+Month), new Text(builder.toString()));
	    }

}
