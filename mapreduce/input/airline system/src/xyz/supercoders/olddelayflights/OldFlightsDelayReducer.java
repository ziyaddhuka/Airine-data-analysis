package xyz.supercoders.olddelayflights;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OldFlightsDelayReducer extends Reducer<IntWritable, OldflightsWritable, IntWritable, Text> {

	public void reduce(IntWritable _key, Iterable<OldflightsWritable> values, Context context) 
			throws IOException, InterruptedException {
		// process values
		int arrdelay,depdelay,count=0,totaldelay=0;
		Float  avgtotaldelay;
		
		for (OldflightsWritable val : values) {

			arrdelay=val.getarrdelay().get();
			depdelay=val.getdepdelay().get();	
			totaldelay+=arrdelay+depdelay;
			count++;
		}
		
		avgtotaldelay=(float)(totaldelay/count);
		context.write(_key,new Text(count+"\t"+avgtotaldelay));
	}

}
