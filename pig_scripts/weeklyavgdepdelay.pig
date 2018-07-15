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

f_c_weekly_records = foreach weekly_records generate $3,$15;

filtered_weekly_records = filter f_c_weekly_records by $1!='NA';

store filtered_weekly_records into '/media/dd/New Volume/airline/input_airline/1987-2008_pig.txt';

weekly = LOAD '/media/dd/New Volume/airline/input_airline/1987-2008_pig.txt'
	       as
	       (Dayofweek:int,DepDelay:int);

g_weekly = group weekly by $0;

records_weekly = foreach g_weekly generate group,AVG(weekly.$1);

store records_weekly into '/media/dd/New Volume/airline/input_airline/pig/1987-2008_avgdepdelay.txt';
