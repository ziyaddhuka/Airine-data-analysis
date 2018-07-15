use airline;

insert overwrite directory '/media/dd/New Volume/airline/hive_output/oldflights_delay' 
select flightnum ,count(depdelay),avg(depdelay + arrdelay)
from ontime
group by flightnum ;

