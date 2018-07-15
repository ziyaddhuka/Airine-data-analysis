package xyz.supercoders.olddelayflights;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;


public class OldflightsWritable implements Writable{

		private IntWritable arrdelay,depdelay;

	//Default Constructor
		public OldflightsWritable() 
		{
		  this.arrdelay = new IntWritable();
		  this.depdelay=new IntWritable();
		  
		} 
	
	 //Custom Constructor
		public OldflightsWritable(IntWritable arrdelay,IntWritable depdelay) 
		{
		  this.arrdelay=arrdelay;
		  this.depdelay=depdelay;
		}
	
		 //Setter method to set the values of datedelayWritable object
		 public void set(IntWritable arrdelay,IntWritable depdelay)
		   
		 {
			  this.arrdelay=arrdelay;
			  this.depdelay=depdelay;
		 }
		
		 //to get IP address from WebLog Record
		 public IntWritable getarrdelay()
		 {
		  return arrdelay; 
		 }
		 
		 
		 public IntWritable getdepdelay()
		 {
		  return depdelay; 
		 }
		 
		 //overriding default readFields method. 
		 //It de-serializes the byte stream data
		 public void readFields(DataInput in) throws IOException 
		 {
		  arrdelay.readFields(in);
		  depdelay.readFields(in);
		 }
		
		 
		 //It serializes object data into byte stream data
		 public void write(DataOutput out) throws IOException 
		 {
		  arrdelay.write(out);
		  depdelay.write(out);
		 }
		 
}

