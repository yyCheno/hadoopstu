package findMaxTemp;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FindMaxTempMapper extends Mapper<LongWritable,Text,Text,DoubleWritable> {
public FindMaxTempMapper() {
	// TODO Auto-generated constructor stub
}
    @Override
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
		String line = value.toString();
		String year  = line.substring(14, 19);
		System.out.println(year);
try {
	double airTemperature = Double.parseDouble(line.substring(26, 31));
	context.write(new Text(year), new DoubleWritable(airTemperature));
} catch (Exception e) {
	// TODO: handle exception
	return;
}
			
	
	

	}
}
