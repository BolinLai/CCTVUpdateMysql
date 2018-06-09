# coding: utf-8
from config import *
from hbquery import *

now_time = time.time()

start_date = FINANCE_START_YMD
end_date = '20180331'

add_partition(MysqlTable.BASIC_INDEX_EVERYDAY_PRO, end_date)
add_partition(MysqlTable.BASIC_INDEX_EVERYDAY, end_date)
add_partition(MysqlTable.BASIC_INDEX_DAYS, end_date)
play_times('finance', start_date, end_date, date_type='everyday', insert=True)
clicks('finance', start_date, end_date, date_type='everyday', query_pro=False, insert=True)
active_devices('finance', start_date, end_date, date_type='everyday', query_pro=False, insert=True)
total_devices('finance', start_date, end_date, date_type='everyday', query_pro=False, insert=True)
active_devices_days('finance', start_date, end_date, date_type='week', query_pro=False, insert=True)

cost_time = int(time.time() - now_time)
print 'Cost time:', cost_time/3600, 'h', (cost_time % 3600)/60, 'm', cost_time % 60, 's'
