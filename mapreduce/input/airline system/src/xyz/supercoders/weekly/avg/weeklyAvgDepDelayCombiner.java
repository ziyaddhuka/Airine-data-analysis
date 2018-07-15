package xyz.supercoders.weekly.avg;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class weeklyAvgDepDelayCombiner 
extends Reducer<IntWritable, Text, IntWritable, Text>  {

	public void reduce(IntWritable _key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

		int total=0,depdelay=0;
		
		for (Text val : values) {
	
			depdelay+=Integer.parseInt(val.toString());
			total++;	
		}
		context.write(_key,new Text(depdelay+","+total));
	}

}
