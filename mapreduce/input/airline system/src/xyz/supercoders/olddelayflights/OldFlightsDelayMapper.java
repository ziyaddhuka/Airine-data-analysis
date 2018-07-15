package xyz.supercoders.olddelayflights;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OldFlightsDelayMapper extends Mapper<LongWritable, Text, IntWritable, OldflightsWritable> {

	OldflightsWritable oldflights=new OldflightsWritable();
	private IntWritable arrdelay = new IntWritable();
	private IntWritable depdelay = new IntWritable();
	  
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {

		String line=ivalue.toString();
		String[] tokens =line.split(",");

		if(!(tokens[14].equals("NA") || tokens[15].equals("NA"))){
			
			int flightnum=Integer.parseInt(tokens[9]);
			arrdelay.set(Integer.parseInt(tokens[14]));
			depdelay.set(Integer.parseInt(tokens[15]));
			oldflights.set(arrdelay, depdelay);
			
			context.write(new IntWritable(flightnum), oldflights);
		}	
	}
}
