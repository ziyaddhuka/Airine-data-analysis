package xyz.supercoders.weekly.avg;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeeklyAvgDepDelayMapper extends 
Mapper<LongWritable, Text, IntWritable, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException ,NumberFormatException{
		
		String line=ivalue.toString();
		String[] tokens =line.split(",");
		int year;
		
		if(!(tokens[0].equals("NA") || tokens[15].equals("NA"))){
				year=Integer.parseInt(tokens[0]);
				context.write(new IntWritable(year), new Text(tokens[15]));
		}
	}
}
