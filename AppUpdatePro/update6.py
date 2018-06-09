# coding: utf-8
from config import *
from hbquery import *

now_time = time.time()

start_date = START_YMD
end_date = '20180131'

add_partition(MysqlTable.BASIC_INDEX_DAYS_PRO, end_date)
active_devices_days(start_date=start_date, end_date=end_date, date_type='7days', query_tag=False, query_dev=False, insert=True)
timevalid_active_devices_days(start_date=start_date, end_date=end_date, date_type='7days', query_tag=False, query_dev=False, insert=True)

active_devices_days(start_date=start_date, end_date=end_date, date_type='30days', query_tag=False, query_dev=False, insert=True)
timevalid_active_devices_days(start_date=start_date, end_date=end_date, date_type='30days', query_tag=False, query_dev=False, insert=True)

active_devices_days(start_date=start_date, end_date=end_date, date_type='month', query_tag=False, query_dev=False, insert=True)
timevalid_active_devices_days(start_date=start_date, end_date=end_date, date_type='month', query_tag=False, query_dev=False, insert=True)

cost_time = int(time.time() - now_time)
print 'Cost time:', cost_time/3600, 'h', (cost_time % 3600)/60, 'm', cost_time % 60, 's'
