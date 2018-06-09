# coding: utf-8
from config import *
from hbquery import *

now_time = time.time()

start_date = START_YMDH
end_date = '2018013123'

add_partition(MysqlTable.BASIC_INDEX_HOUR, end_date)
play_times(start_date, end_date, date_type='hour', query_pro=False, insert=True)
clicks(start_date, end_date, date_type='hour', query_pro=False, insert=True)
active_devices(start_date, end_date, date_type='hour', query_pro=False, insert=True)
watch_time(start_date, end_date, date_type='hour', query_pro=False, insert=True)
use_time(start_date, end_date, date_type='hour', insert=True)
launch_times(start_date, end_date, date_type='hour', insert=True)
total_devices(start_date, end_date, date_type='hour', query_pro=False, insert=True)

add_partition(MysqlTable.SPECIFIC_INDEX_HOUR, end_date)
timevalid_total_devices(end_date, insert=True)

cost_time = int(time.time() - now_time)
print 'Cost time:', cost_time/3600, 'h', (cost_time % 3600)/60, 'm', cost_time % 60, 's'
