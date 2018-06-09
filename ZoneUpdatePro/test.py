import pprint
import datetime

from model import *
from hbquery import *
from method import *
from config import *

now_time = time.time()

# insert_update_time(START_UPDATE_DICTS)
# file_output(path='udate_hour.txt', date_type='hour')
# file_output(path='udate_day.txt', date_type='day')

# print get_hours('2017120200', '2017120300')
# pprint.pprint(get_months('20170601', '20170930', ledge=True, redge=False))
# pprint.pprint(TAGS)

# print get_update_ymdh(MysqlTable.BASIC_INDEX_DAYS, date_type='30days')

# add_partition(MysqlTable.BASIC_INDEX_HOUR_PRO, '2016092615')
# add_partition(MysqlTable.BASIC_INDEX_HOUR, '2017092700')

# print isdataready('2018012512')

# play_times('2018031200', '2018031223', date_type='hour', query_pro=False, query_tag=True, insert=False)

# active_devices('2017081805', '2017081807', date_type='hour', query_pro=False, query_tag=True, insert=False)

# total_devices('2018030200', '2018030223', date_type='hour', query_pro=False, query_tag=True, query_area=False, insert=False)

# watch_time('2017091620', '2017091620', date_type='hour', query_pro=False, query_tag=True, insert=False)

# timevalid_active_devices('20170902', '20170902', date_type='everyday', query_pro=False, query_tag=True, insert=False)

# active_devices_days('20180316', '20180318', date_type='7days', query_pro=True, query_tag=True, insert=False)

# timevalid_active_devices_days('20170902', '20171003', date_type='30days', query_pro=False, query_tag=True, insert=False)

# watchtime_dist('20180502', '20180502', date_type='everyday', insert=False)

# timevalid_total_devices('2017091623', insert=False)

# print pro_total_devices('2017091023', program_id='VIDE1483698059081147_1', area_id='000000')

# print datetime.datetime.strptime('2017080900', '%Y%m%d%H').month


print time.time() - now_time
