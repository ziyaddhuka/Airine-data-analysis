package xyz.supercoders;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeeklyMapper extends Mapper<LongWritable, Text, IntWritable, weeklycountWritable> {

	  private weeklycountWritable weeklycount = new weeklycountWritable();
	  private IntWritable arrdelay = new IntWritable();
	  private IntWritable depdelay = new IntWritable();
	  private IntWritable cancelled = new IntWritable();
	  private IntWritable diverted = new IntWritable();
	
	
	public void map(LongWritable ikey, Text ivalue, Context context) 
			throws IOException, InterruptedException {
		
		String line=ivalue.toString();
		String[] tokens =line.split(",");
		
		if(!(tokens[3].equals("NA") || tokens[21].equals("NA") || tokens[23].equals("NA")
				|| tokens[14].equals("NA") || tokens[15].equals("NA"))){
	
			int WeekOfDay=Integer.parseInt(tokens[3]);
			arrdelay.set(Integer.parseInt(tokens[14]));
			depdelay.set(Integer.parseInt(tokens[15]));
			cancelled.set(Integer.parseInt(tokens[21]));
			diverted.set(Integer.parseInt(tokens[23]));
			weeklycount.set(arrdelay,depdelay,cancelled,diverted);

			context.write(new IntWritable(WeekOfDay), weeklycount);
	
		}
	}
}
