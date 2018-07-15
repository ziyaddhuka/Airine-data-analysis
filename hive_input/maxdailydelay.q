use airline;

insert overwrite directory '/media/dd/New Volume/airline/hive_output/maxdaily_delay' 
select dayofmonth,month,max(ArrDelay) 
from ontime
group by DayofMonth,month;

