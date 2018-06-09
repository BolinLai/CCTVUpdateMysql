# coding: utf-8
import time

from config import *
from hbquery import *
from method import get_update_ymd, get_date, isdataready, add_partition

now_time = time.time()
now_date = time.strftime('%Y%m%d', time.localtime(now_time))


date = get_update_ymd(MysqlTable.BASIC_INDEX_EVERYDAY, source='finance')
date2 = get_update_ymd(MysqlTable.BASIC_INDEX_EVERYDAY_PRO, source='finance')
if date == date2:
    start_date = get_date(date, 1) if date else FINANCE_START_YMD
    end_date = get_date(now_date, -1)
    if int(start_date) <= int(end_date):
        dates = get_days(start_date, end_date)
        dates.sort(reverse=True)
        for date in dates:
            if isdataready(date, source='finance'):
                end_date = date
                add_partition(MysqlTable.BASIC_INDEX_EVERYDAY_PRO, end_date)
                add_partition(MysqlTable.BASIC_INDEX_EVERYDAY, end_date)
                play_times('finance', start_date, end_date, date_type='everyday', insert=True)
                clicks('finance', start_date, end_date, date_type='everyday', query_pro=False, insert=True)
                active_devices('finance', start_date, end_date, date_type='everyday', query_pro=False, insert=True)
                total_devices('finance', start_date, end_date, date_type='everyday', query_pro=False, insert=True)
                insert_update_time({MysqlTable.BASIC_INDEX_EVERYDAY: {'FINANCE': end_date},
                                    MysqlTable.BASIC_INDEX_EVERYDAY_PRO: {'FINANCE': end_date}})
                break
        else:
            print "Finance data of {start_date}-{end_date} is not ready!!!".format(start_date=start_date, end_date=end_date)
else:
    print "There are two different dates of {} and {} about finance!!!".format(MysqlTable.BASIC_INDEX_EVERYDAY, MysqlTable.BASIC_INDEX_EVERYDAY_PRO)


date = get_update_ymd(MysqlTable.BASIC_INDEX_EVERYDAY, source='children')
date2 = get_update_ymd(MysqlTable.BASIC_INDEX_EVERYDAY_PRO, source='children')
if date == date2:
    start_date = get_date(date, 1) if date else CHILDREN_START_YMD
    end_date = get_date(now_date, -1)
    if int(start_date) <= int(end_date):
        dates = get_days(start_date, end_date)
        dates.sort(reverse=True)
        for date in dates:
            if isdataready(date, source='children'):
                end_date = date
                add_partition(MysqlTable.BASIC_INDEX_EVERYDAY_PRO, end_date)
                add_partition(MysqlTable.BASIC_INDEX_EVERYDAY, end_date)
                play_times('children', start_date, end_date, date_type='everyday', insert=True)
                clicks('children', start_date, end_date, date_type='everyday', query_pro=False, insert=True)
                active_devices('children', start_date, end_date, date_type='everyday', query_pro=False, insert=True)
                total_devices('children', start_date, end_date, date_type='everyday', query_pro=False, insert=True)
                insert_update_time({MysqlTable.BASIC_INDEX_EVERYDAY: {'CHILDREN': end_date},
                                    MysqlTable.BASIC_INDEX_EVERYDAY_PRO: {'CHILDREN': end_date}})
                break
        else:
            print "Children data of {start_date}-{end_date} is not ready!!!".format(start_date=start_date, end_date=end_date)
else:
    print "There are two different dates of {} and {} about children!!!".format(MysqlTable.BASIC_INDEX_EVERYDAY, MysqlTable.BASIC_INDEX_EVERYDAY_PRO)


date = get_update_ymd(MysqlTable.BASIC_INDEX_EVERYDAY, source='music')
date2 = get_update_ymd(MysqlTable.BASIC_INDEX_EVERYDAY_PRO, source='music')
if date == date2:
    start_date = get_date(date, 1) if date else MUSIC_START_YMD
    end_date = get_date(now_date, -1)
    if int(start_date) <= int(end_date):
        dates = get_days(start_date, end_date)
        dates.sort(reverse=True)
        for date in dates:
            if isdataready(date, source='music'):
                end_date = date
                add_partition(MysqlTable.BASIC_INDEX_EVERYDAY_PRO, end_date)
                add_partition(MysqlTable.BASIC_INDEX_EVERYDAY, end_date)
                play_times('music', start_date, end_date, date_type='everyday', insert=True)
                clicks('music', start_date, end_date, date_type='everyday', query_pro=False, insert=True)
                active_devices('music', start_date, end_date, date_type='everyday', query_pro=False, insert=True)
                total_devices('music', start_date, end_date, date_type='everyday', query_pro=False, insert=True)
                insert_update_time({MysqlTable.BASIC_INDEX_EVERYDAY: {'MUSIC': end_date},
                                    MysqlTable.BASIC_INDEX_EVERYDAY_PRO: {'MUSIC': end_date}})
                break
        else:
            print "Music data of {start_date}-{end_date} is not ready!!!".format(start_date=start_date, end_date=end_date)
else:
    print "There are two different dates of {} and {} about music!!!".format(MysqlTable.BASIC_INDEX_EVERYDAY, MysqlTable.BASIC_INDEX_EVERYDAY_PRO)


date = get_update_ymd(MysqlTable.BASIC_INDEX_EVERYDAY, source='cctv5')
date2 = get_update_ymd(MysqlTable.BASIC_INDEX_EVERYDAY_PRO, source='cctv5')
if date == date2:
    start_date = get_date(date, 1) if date else CCTV5_START_YMD
    end_date = get_date(now_date, -1)
    if int(start_date) <= int(end_date):
        dates = get_days(start_date, end_date)
        dates.sort(reverse=True)
        for date in dates:
            if isdataready(date, source='cctv5'):
                end_date = date
                add_partition(MysqlTable.BASIC_INDEX_EVERYDAY_PRO, end_date)
                add_partition(MysqlTable.BASIC_INDEX_EVERYDAY, end_date)
                play_times('cctv5', start_date, end_date, date_type='everyday', insert=True)
                clicks('cctv5', start_date, end_date, date_type='everyday', query_pro=False, insert=True)
                active_devices('cctv5', start_date, end_date, date_type='everyday', query_pro=False, insert=True)
                total_devices('cctv5', start_date, end_date, date_type='everyday', query_pro=False, insert=True)
                insert_update_time({MysqlTable.BASIC_INDEX_EVERYDAY: {'CCTV5': end_date},
                                    MysqlTable.BASIC_INDEX_EVERYDAY_PRO: {'CCTV5': end_date}})
                break
        else:
            print "CCTV5 data of {start_date}-{end_date} is not ready!!!".format(start_date=start_date, end_date=end_date)
else:
    print "There are two different dates of {} and {} about cctv5!!!".format(MysqlTable.BASIC_INDEX_EVERYDAY, MysqlTable.BASIC_INDEX_EVERYDAY_PRO)


date = get_update_ymd(MysqlTable.BASIC_INDEX_DAYS, source='finance')
start_date = get_date(date, 1) if date else FINANCE_START_YMD
end_date = get_date(now_date, -1)
if int(start_date) <= int(end_date):
    dates = get_days(start_date, end_date)
    dates.sort(reverse=True)
    for date in dates:
        end_date = date
        add_partition(MysqlTable.BASIC_INDEX_DAYS, end_date)
        active_devices_days('finance', start_date=start_date, end_date=end_date, date_type='week', query_pro=False, insert=True)
        insert_update_time({MysqlTable.BASIC_INDEX_DAYS: {'FINANCE': end_date}})
        break
    else:
        print "Finance data of {start_date}-{end_date} is not ready!!!".format(start_date=start_date, end_date=end_date)


date = get_update_ymd(MysqlTable.BASIC_INDEX_DAYS, source='children')
start_date = get_date(date, 1) if date else CHILDREN_START_YMD
end_date = get_date(now_date, -1)
if int(start_date) <= int(end_date):
    dates = get_days(start_date, end_date)
    dates.sort(reverse=True)
    for date in dates:
        end_date = date
        add_partition(MysqlTable.BASIC_INDEX_DAYS, end_date)
        active_devices_days('children', start_date=start_date, end_date=end_date, date_type='week', query_pro=False, insert=True)
        insert_update_time({MysqlTable.BASIC_INDEX_DAYS: {'CHILDREN': end_date}})
        break
    else:
        print "Children data of {start_date}-{end_date} is not ready!!!".format(start_date=start_date, end_date=end_date)


date = get_update_ymd(MysqlTable.BASIC_INDEX_DAYS, source='music')
start_date = get_date(date, 1) if date else MUSIC_START_YMD
end_date = get_date(now_date, -1)
if int(start_date) <= int(end_date):
    dates = get_days(start_date, end_date)
    dates.sort(reverse=True)
    for date in dates:
        end_date = date
        add_partition(MysqlTable.BASIC_INDEX_DAYS, end_date)
        active_devices_days('music', start_date=start_date, end_date=end_date, date_type='week', query_pro=False, insert=True)
        insert_update_time({MysqlTable.BASIC_INDEX_DAYS: {'MUSIC': end_date}})
        break
    else:
        print "Music data of {start_date}-{end_date} is not ready!!!".format(start_date=start_date, end_date=end_date)


date = get_update_ymd(MysqlTable.BASIC_INDEX_DAYS, source='cctv5')
start_date = get_date(date, 1) if date else CCTV5_START_YMD
end_date = get_date(now_date, -1)
if int(start_date) <= int(end_date):
    dates = get_days(start_date, end_date)
    dates.sort(reverse=True)
    for date in dates:
        end_date = date
        add_partition(MysqlTable.BASIC_INDEX_DAYS, end_date)
        active_devices_days('cctv5', start_date=start_date, end_date=end_date, date_type='week', query_pro=False, insert=True)
        insert_update_time({MysqlTable.BASIC_INDEX_DAYS: {'CCTV5': end_date}})
        break
    else:
        print "CCTV5 data of {start_date}-{end_date} is not ready!!!".format(start_date=start_date, end_date=end_date)

cost_time = int(time.time() - now_time)
print 'Start:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now_time))
print 'Cost time:', cost_time/3600, 'h', (cost_time % 3600)/60, 'm', cost_time % 60, 's'
print "===================================================================="
