import pprint
import time

from method import *
from hbquery import *

begin_time = time.time()

# insert_update_time(START_UPDATE_DICTS)
# file_output(path='udate_hour.txt', date_type='hour')
# file_output(path='udate_day.txt', date_type='day')

# print TOTAL_TAGS
# print len(FIRST_TAGS)
# print len(TOTAL_TAGS)

# pprint.pprint(NODES)

# pprint.pprint(TOTAL_TAGS)
# pprint.pprint(get_subtag('NavX1483689221212427'))
# print split_dates('20170809', '20170903', 2, 'everyday')
# pprint.pprint(get_weeks('20180123', '20180901'))
# print get_weeks('20170207', '20170208', ledge=False, redge=True)

# print isdataready('2018012517')

# add_partition(MysqlTable.BASIC_INDEX_HOUR, '2017103123')

# print get_update_ymdh(MysqlTable.BASIC_INDEX_HOUR)

# play_times('2017040100', '2017040223', 'hour', query_pro=False, query_tag=True, query_dev=False, insert=False)

# clicks('20170401', '20170402', 'everyday', query_pro=False, query_tag=True, query_dev=False, insert=False)

# active_devices('20170401', '20170402', 'everyday', query_pro=False, query_tag=True, query_dev=False, insert=False)

# total_devices('2017040115', '2017040115', 'hour', query_pro=False, query_tag=True, query_area=True, query_dev=True, insert=False)

# watch_time('2017040100', '2017040102', 'hour', query_pro=False, query_tag=False, query_dev=True, insert=False)

# use_time('2017040100', '2017040123', 'hour', query_tag=False, query_dev=True, insert=False)

# launch_times('2017040100', '2017040123', 'hour', query_area=False, query_dev=True, insert=False)

# timevalid_active_devices('2017040100', '2017040123', 'hour', query_pro=False, query_tag=False, query_dev=True, insert=False)

# active_devices_days('20170430', '20170501', 'month', query_pro=False, query_tag=False, query_dev=True, insert=False)

# timevalid_active_devices_days('20170401', '20170407', '7days', query_pro=False, query_tag=True, query_dev=False, insert=False)

# launch_times_days('20170401', '20170430', '30days', query_area=False, query_dev=True, insert=False)

watchtime_dist('20170401', '20170407', 'history', query_tag=True, insert=False)

# usetime_dist('20170401', '20170407', 'everyday', query_tag=True, insert=False)

# new_retention('20170203', '20180212', date_type='day', insert=False)

# active_retention('20170420', '20170630', date_type='month', insert=False)

# timevalid_total_devices('2017040123', query_tag=True, insert=False)

print "Cost time:", time.time()-begin_time
