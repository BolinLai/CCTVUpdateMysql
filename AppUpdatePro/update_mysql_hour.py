# coding: utf-8
import time

from config import *
from hbquery import *
from method import get_update_ymdh, get_date, isdataready, add_partition

now_time = time.time()
now_date = time.strftime('%Y%m%d', time.localtime(now_time))


date = get_update_ymdh(MysqlTable.BASIC_INDEX_HOUR, date_type='hour')
date2 = get_update_ymdh(MysqlTable.BASIC_INDEX_HOUR_PRO, date_type='hour')
if date == date2:
    start_date = get_date(date, 1) if date else START_YMDH
    end_date = get_date(now_date, -1) + '23'
    if int(start_date) <= int(end_date):
        # dates = get_hours(start_date, end_date)
        dates = get_days(start_date[:8], end_date[:8])
        dates.sort(reverse=True)
        for date in dates:
            if isdataready(date):
                end_date = date + '23'
                add_partition(MysqlTable.BASIC_INDEX_HOUR_PRO, end_date)
                add_partition(MysqlTable.BASIC_INDEX_HOUR, end_date)
                play_times(start_date, end_date, date_type='hour', insert=True)
                clicks(start_date, end_date, date_type='hour', insert=True)
                active_devices(start_date, end_date, date_type='hour', insert=True)
                watch_time(start_date, end_date, date_type='hour', insert=True)
                use_time(start_date, end_date, date_type='hour', insert=True)
                launch_times(start_date, end_date, date_type='hour', insert=True)
                total_devices(start_date, end_date, date_type='hour', query_pro=False, insert=True)
                insert_update_time({MysqlTable.BASIC_INDEX_HOUR: {'hour': end_date},
                                    MysqlTable.BASIC_INDEX_HOUR_PRO: {'hour': end_date}})
                break
        else:
            print "Data of {start_date}-{end_date} is not ready!!!".format(start_date=start_date, end_date=end_date)
else:
    print "There are two different dates of {} and {}!!!".format(MysqlTable.BASIC_INDEX_HOUR, MysqlTable.BASIC_INDEX_HOUR_PRO)

date = get_update_ymdh(MysqlTable.SPECIFIC_INDEX_HOUR, date_type='None')
start_date = get_date(date, 1) if date else START_YMDH
end_date = get_date(now_date, -1) + '23'
if int(start_date) <= int(end_date):
    # dates = get_hours(start_date, end_date)
    dates = get_days(start_date[:8], end_date[:8])
    dates.sort(reverse=True)
    for date in dates:
        if isdataready(date):
            end_date = date + '23'
            add_partition(MysqlTable.SPECIFIC_INDEX_HOUR, end_date)
            timevalid_total_devices(end_date, insert=True)
            insert_update_time({MysqlTable.SPECIFIC_INDEX_HOUR: {'None': end_date}})
            break
    else:
        print "Data of {start_date}-{end_date} is not ready!!!".format(start_date=start_date, end_date=end_date)

file_output(path='udate_hour.txt', date_type='hour')

cost_time = int(time.time() - now_time)
print 'Start:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now_time))
print 'Cost time:', cost_time/3600, 'h', (cost_time % 3600)/60, 'm', cost_time % 60, 's'
print "=============================================================="
