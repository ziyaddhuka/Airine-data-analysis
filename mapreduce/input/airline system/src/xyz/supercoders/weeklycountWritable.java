package xyz.supercoders;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class weeklycountWritable implements Writable
{

		private IntWritable arrdelay,depdelay,cancelled,diverted;

	//Default Constructor
		public weeklycountWritable() 
		{
			this.arrdelay = new IntWritable();
		  this.depdelay=new IntWritable();
		  this.cancelled = new IntWritable();
		  this.diverted=new IntWritable();
		}
	
	 //Custom Constructor
		public weeklycountWritable(IntWritable arrdelay,IntWritable depdelay,
			 IntWritable cancelled,IntWritable diverted) 
		{
		  this.arrdelay=arrdelay;
		  this.depdelay=depdelay;
		  this.cancelled=cancelled;
		  this.diverted=diverted;
		}
	
		 //Setter method to set the values of datedelayWritable object
		 public void set(IntWritable arrdelay,IntWritable depdelay,
		 IntWritable cancelled,IntWritable diverted)  
		 {
			  this.arrdelay=arrdelay;
			  this.depdelay=depdelay;
			  this.cancelled=cancelled;
			  this.diverted=diverted;
		 }
		
		 //to get IP address from WebLog Record
		 public IntWritable getarrdelay()
		 {
		  return arrdelay; 
		 }
		 
		 public IntWritable getcancelled()
		 {
		  return cancelled; 
		 }
		 
		 public IntWritable getdiverted()
		 {
		  return diverted; 
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
		  cancelled.readFields(in);
		  diverted.readFields(in);
		 }
		
		 
		 //It serializes object data into byte stream data
		 public void write(DataOutput out) throws IOException 
		 {
		  arrdelay.write(out);
		  depdelay.write(out);
		  cancelled.write(out);
		  diverted.write(out);
		 }
		 
}
