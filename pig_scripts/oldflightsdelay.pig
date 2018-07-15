oldflights_records = LOAD '/media/dd/New Volume/airline/input_airline'
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

f_c_oldflights_records = foreach oldflights_records generate $3,$9,$14,$15,$21,$23;

filtered_oldflights_records = filter f_c_oldflights_records by (($2!='NA') AND ($3!='NA') AND ($4!='NA') AND ($5!='NA')) ;

store filtered_oldflights_records into '/media/dd/New Volume/airline/pig/1987-2008_flights_pig.txt';

weekly = LOAD '/media/dd/New Volume/airline/pig/1987-2008_flights_pig.txt'
	       as
	       (Dayofweek:int,UniCarrier:int,ArrDelay:int,DepDelay:int,Cancel:int,Diverted:int);

f_oldflights_records = foreach weekly generate $1,($2+$3) as totdelay;;

g_f_oldflights_records = group f_oldflights_records by $0;

avg_oldflights_records = foreach g_f_oldflights_records generate group,COUNT(f_oldflights_records.$1) as counter,
		AVG(f_oldflights_records.$1) as totavg;

store avg_oldflights_records into '/media/dd/New Volume/airline/pig/1987-2008_oldflights_pig.txt';
