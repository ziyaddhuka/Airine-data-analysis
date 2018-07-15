use airline;

insert overwrite directory '/media/dd/New Volume/airline/hive_output/weekly_avg_depdelay' 
select DayofWeek,avg(DepDelay) 
from ontime
group by DayofWeek;


