package pack;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CustomRecordWriter extends RecordWriter<Text, Text> {

	DataOutputStream out;
	public CustomRecordWriter(DataOutputStream stream)
	{
		out = stream;
		try
		{
			out.writeBytes("<results>");
		}
		catch (Exception ex)
		{
			
		}
	}
	
	
	
	@Override
	public void close(TaskAttemptContext arg0) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		
		try
		{
			out.writeBytes("</results>");
		}
		finally
		{
			out.close();
		}

	}

	@Override
	public void write(Text arg0, Text arg1) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub

        out.writeBytes("<result>");
        out.writeBytes("\r\n<key>");
       out.writeBytes(arg0.toString());
       out.writeBytes("</key>");
       out.writeBytes("\r\n<value>");
       out.writeBytes(arg1.toString());
       out.writeBytes("</value>\r\n");
       out.writeBytes("</result>");
       out.writeBytes("\r\n");  

	}

}
