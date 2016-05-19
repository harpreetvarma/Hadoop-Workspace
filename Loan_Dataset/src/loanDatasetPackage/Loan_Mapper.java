package loanDatasetPackage;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Loan_Mapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {

		String line[]=ivalue.toString().split(",");
		// l[0] loan id
		// l[1] gender
		// 2 Married
		// 3 Depender
		// 4 education 
		// 5 self_employed
		// 6 ApplicantIncome,
		// 7 CoapplicantIncome
		// 8 LoanAmount
		// 9 Loan_Amount_Term
		// 10 Credit_History
		// 11 Property_Area
		// 12 loan status
		
		try
		{
			if(line[1].equalsIgnoreCase("Female") && line[12].equalsIgnoreCase("Y"))
			{
				context.write(new Text(ivalue),new Text(""));
			}
			
		}
		catch(Exception e)
		{
			
		}
		
	}

}
