# coding: utf-8
from config import *
from hbquery import *

now_time = time.time()

start_date = START_YMD
end_date = '20180131'

add_partition(MysqlTable.SPECIFIC_INDEX, end_date)
watchtime_dist(start_date=start_date, end_date=end_date, date_type='everyday', insert=True)
watchtime_dist(start_date=start_date, end_date=end_date, date_type='history', insert=True)

cost_time = int(time.time() - now_time)
print 'Cost time:', cost_time/3600, 'h', (cost_time % 3600)/60, 'm', cost_time % 60, 's'
