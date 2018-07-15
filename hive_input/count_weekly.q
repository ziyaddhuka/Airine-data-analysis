use airline;

insert overwrite directory '/media/dd/New Volume/airline/hive_output/count/weekly_depdelay' 
select DayofWeek,count(*) 
from ontime
where depdelay > 0
group by DayofWeek;


insert overwrite directory '/media/dd/New Volume/airline/hive_output/count/weekly_arrdelay' 
select DayofWeek,count(*) 
from ontime
where arrdelay > 0
group by DayofWeek;


insert overwrite directory '/media/dd/New Volume/airline/hive_output/count/weekly_cancelled' 
select DayofWeek,count(*) 
from ontime
where cancelled > 0
group by DayofWeek;

insert overwrite directory '/media/dd/New Volume/airline/hive_output/count/weekly_diverted' 
select DayofWeek,count(*) 
from ontime
where diverted > 0
group by DayofWeek;

