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

f_c_weekly_records = foreach weekly_records generate $1,$2,$14;

filtered_weekly_records = filter f_c_weekly_records by $2!='NA';

store filtered_weekly_records into '/media/dd/New Volume/airline/input_airline/1987-2008_pig.txt';

weekly = LOAD '/media/dd/New Volume/airline/input_airline/1987-2008_pig.txt'
	       as
	       (Month:int,Dayofweek:int,ArrDelay:int);

g_weekly = group weekly by $0;

records_weekly = foreach g_weekly generate group as month,MAX(weekly.$2) as maxdelay;

joined_records = JOIN records_weekly BY (month,maxdelay) LEFT OUTER, weekly BY (Month,ArrDelay);

store_joined_records = foreach joined_records generate $0,$3,$4;

store store_joined_records into '/media/dd/New Volume/airline/pig/1987-2008_MaxDelayMonth';


