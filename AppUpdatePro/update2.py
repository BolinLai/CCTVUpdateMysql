# coding: utf-8
from config import *
from hbquery import *

now_time = time.time()

start_date = START_YMDH
end_date = '2018013123'

add_partition(MysqlTable.BASIC_INDEX_HOUR_PRO, end_date)
play_times(start_date, end_date, date_type='hour', query_tag=False, query_dev=False, insert=True)
clicks(start_date, end_date, date_type='hour', query_tag=False, query_dev=False, insert=True)
active_devices(start_date, end_date, date_type='hour', query_tag=False, query_dev=False, insert=True)
watch_time(start_date, end_date, date_type='hour', query_tag=False, query_dev=False, insert=True)

cost_time = int(time.time() - now_time)
print 'Cost time:', cost_time/3600, 'h', (cost_time % 3600)/60, 'm', cost_time % 60, 's'
