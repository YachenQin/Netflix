package stubs;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper1 extends MapReduceBase implements Mapper<LongWritable,Text,IntWritable,Text>{

	private IntWritable customerId = new IntWritable();
	private Text movieRating = new Text();
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub

		
		String line= value.toString();
		String[] a= line.split(",");
			
		customerId.set(Integer.parseInt(a[1]));
		movieRating.set(Integer.parseInt(a[0])+ ","+ Float.parseFloat(a[2]));
		
		output.collect(customerId, movieRating);
		
			
		
		
	}

}
