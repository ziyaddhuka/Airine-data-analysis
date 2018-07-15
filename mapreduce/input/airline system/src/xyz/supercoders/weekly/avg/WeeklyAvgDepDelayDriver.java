package xyz.supercoders.weekly.avg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeeklyAvgDepDelayDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Weekly Average Depature Delay");
		
		job.setJarByClass(xyz.supercoders.weekly.avg.WeeklyAvgDepDelayDriver.class);
		job.setMapperClass(xyz.supercoders.weekly.avg.WeeklyAvgDepDelayMapper.class);
		job.setReducerClass(xyz.supercoders.weekly.avg.WeeklyAvgDepDelayReducer.class);
		job.setCombinerClass(xyz.supercoders.weekly.avg.weeklyAvgDepDelayCombiner.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, 
				new Path("/media/dd/New Volume/airline/input_airline"));
		FileOutputFormat.setOutputPath(job, 
				new Path("/media/dd/New Volume/airline/mapreduce/yearlyavgdepdelaycombiner_output"));

		if (!job.waitForCompletion(true))
			return;
	}

}
