package xyz.supercoders.olddelayflights;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OldFlightsDelayDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Do older flights suffer more delay?");
		
		job.setJarByClass(xyz.supercoders.olddelayflights.OldFlightsDelayDriver.class);
		job.setMapperClass(xyz.supercoders.olddelayflights.OldFlightsDelayMapper.class);
		job.setReducerClass(xyz.supercoders.olddelayflights.OldFlightsDelayReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(OldflightsWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, 
				new Path("/media/dd/New Volume/airline/input_airline"));
		FileOutputFormat.setOutputPath(job, 
				new Path("/media/dd/New Volume/airline/mapreduce/oldflightsdelaywritable_out"));

		if (!job.waitForCompletion(true))
			return;
	}
}
