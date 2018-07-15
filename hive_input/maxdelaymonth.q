use airline;

insert overwrite directory '/media/dd/New Volume/airline/hive_output/max_delay_month' 
select t2.month,t1.dayofmonth,t2.maxdelay
from ontime as t1
Right join (select month,max(arrdelay) as maxdelay from ontime group by month) as t2
on  t1.arrdelay=t2.maxdelay;
