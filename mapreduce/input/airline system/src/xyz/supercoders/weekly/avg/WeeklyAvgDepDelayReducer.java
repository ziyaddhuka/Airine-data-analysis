package xyz.supercoders.weekly.avg;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeeklyAvgDepDelayReducer extends 
Reducer<IntWritable, Text, IntWritable, FloatWritable> {

	public void reduce(IntWritable _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		
		float avgdepdelay=0;
		int totaldepdelay=0,total_count=0; 
		
		for (Text val : values) {
			String line=val.toString();
			String[] tokens =line.split(",");
			
			int total=Integer.parseInt(tokens[1]);
			int depdelay=Integer.parseInt(tokens[0]);
				
			totaldepdelay+=depdelay;
			total_count+=total;
		}
		avgdepdelay=totaldepdelay/total_count;
		context.write(_key,new FloatWritable(avgdepdelay));
	}
}
