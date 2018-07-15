package xyz.supercoders;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeeklyReducer extends Reducer<IntWritable, weeklycountWritable, IntWritable, Text> {

	public void reduce(IntWritable _key, Iterable<weeklycountWritable> values, Context context) throws IOException, InterruptedException {
		
		int countcancel=0,countdiverted=0,countarrdelay=0,countdepdelay=0,counttotal=0;
	
		for (weeklycountWritable val : values) {
			
			int arrdelay=val.getarrdelay().get();
			if(arrdelay>0)
				countarrdelay++;
			
			int depdelay=val.getdepdelay().get();
			if(depdelay>0)
				countdepdelay++;
			
			int cancelled=val.getcancelled().get();
			if(cancelled==1)
				countcancel++;
			
			int diverted=val.getdiverted().get();
			if(diverted==1)
				countdiverted++;
			
			counttotal++;
		}
		
		context.write(_key,new Text(countarrdelay+"\t"+countdepdelay+"\t"+countcancel+"\t"
				+countdiverted+"\t"+counttotal));
	}
}
