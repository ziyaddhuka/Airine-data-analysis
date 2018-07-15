package xyz.supercoders;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Weekly Airlines Data");
		job.setJarByClass(xyz.supercoders.TestDriver.class);
		
		
		job.setMapperClass(xyz.supercoders.WeeklyMapper.class);
		job.setReducerClass(xyz.supercoders.WeeklyReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(weeklycountWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, 
				new Path("/media/dd/New Volume/airline/input_airline"));
		FileOutputFormat.setOutputPath(job, new Path("/media/dd/New Volume/airline/mapreduce/countweeklywritable_output"));

		if (!job.waitForCompletion(true))
			return;
	}

}
