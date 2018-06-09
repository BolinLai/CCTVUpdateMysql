import pprint
import time

from method import *
from hbquery import *

begin_time = time.time()

insert_update_time(START_UPDATE_DICTS)

# play_times('finance', '20170908', '20170909', 'everyday', query_pro=False, query_tag=True, insert=False)

# clicks('finance', '20170908', '20170909', 'everyday', query_pro=False, query_tag=True, insert=False)

# active_devices('finance', '20170908', '20170909', 'everyday', query_pro=False, query_tag=True, insert=False)

# total_devices('finance', '20170908', '20170909', 'everyday', query_pro=False, query_tag=True, query_area=True, insert=False)

# active_devices_days('finance', '20170908', '20170913', 'week', query_pro=False, query_tag=True, insert=False)

print "Cost time:", time.time()-begin_time
