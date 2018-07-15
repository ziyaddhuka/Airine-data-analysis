weekly_records = LOAD '/media/dd/New Volume/airline/input_airline'
	       USING PigStorage(',')
	       as
	       (Year:int,Month:chararray,DayofMonth:chararray,DayOfWeek:chararray,
  	       DepTime:chararray,CRSDepTime:chararray,ArrTime:chararray,CRSArrTime:chararray,
  	       UniqueCarrier:chararray,FlightNum:chararray,TailNum:chararray,
  	       ActualElapsedTime:chararray,CRSElapsedTime:chararray,AirTime:chararray,
  	       ArrDelay:chararray,DepDelay:chararray,
 	       Origin:chararray,Dest:chararray,Distance:chararray,TaxiIn:chararray,TaxiOut:chararray,
  	       Cancelled:chararray,CancellationCode:chararray,
	       Diverted:chararray,
	       CarrierDelay:chararray,WeatherDelay:chararray,NASDelay:chararray,SecurityDelay:chararray,LateAircraftDelay:chararray);

f_c_weekly_records = foreach weekly_records generate $3,$9,$14,$15,$21,$23;

filtered_weekly_records = filter f_c_weekly_records by (($2!='NA') AND ($3!='NA') AND ($4!='NA') AND ($5!='NA')) ;

store filtered_weekly_records into '/media/dd/New Volume/airline/pig/1987-2008_weekly_pig.txt';

weekly = LOAD '/media/dd/New Volume/airline/pig/1987-2008_weekly_pig.txt'
	       as
	       (Dayofweek:int,UniCarrier:int,ArrDelay:int,DepDelay:int,Cancel:int,Diverted:int);

f_oldflights_records = foreach weekly generate $0,$2,$3,$4,$5;

g_f_oldflights_records = group f_oldflights_records by $0;

count_arrdelay = foreach g_f_oldflights_records {
			row_arrdelay = filter $1  by (ArrDelay > 0);
			generate group,COUNT(row_arrdelay);
			};
store count_arrdelay into '/media/dd/New Volume/airline/pig/1987-2008_count/arrdelaycount_pig.txt';			

count_depdelay = foreach g_f_oldflights_records {
			row_depdelay = filter $1  by (DepDelay > 0);
			generate group,COUNT(row_depdelay);
			};
store count_depdelay into '/media/dd/New Volume/airline/pig/1987-2008_count/depdelaycount_pig.txt';


count_cancel = foreach g_f_oldflights_records {
			row_cancel = filter $1  by (Cancel > 0);
			generate group,COUNT(row_cancel);
			};
store count_cancel into '/media/dd/New Volume/airline/pig/1987-2008_count/countcancel_pig.txt';
				
count_diverted = foreach g_f_oldflights_records {
			row_diverted = filter $1  by (Diverted > 0);
			generate group,COUNT(row_diverted);
			};
store count_diverted into '/media/dd/New Volume/airline/pig/1987-2008_count/countdiverted_pig.txt';

