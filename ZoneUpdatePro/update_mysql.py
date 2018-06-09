# coding: utf-8
from config import *
from hbquery import *
from method import get_update_ymd, get_date, isdataready, add_partition

now_time = time.time()
now_date = time.strftime('%Y%m%d', time.localtime(now_time))

date = get_update_ymd(MysqlTable.BASIC_INDEX_EVERYDAY, date_type='everyday')
date2 = get_update_ymd(MysqlTable.BASIC_INDEX_EVERYDAY_PRO, date_type='everyday')
if date == date2:
    start_date = get_date(date, 1) if date else START_YMD
    end_date = get_date(now_date, -1)
    if int(start_date) <= int(end_date):
        dates = get_days(start_date, end_date)
        dates.sort(reverse=True)
        for date in dates:
            if isdataready(date):
                end_date = date
                add_partition(MysqlTable.BASIC_INDEX_EVERYDAY_PRO, end_date)
                add_partition(MysqlTable.BASIC_INDEX_EVERYDAY, end_date)
                active_devices(start_date, end_date, date_type='everyday', insert=True)
                timevalid_active_devices(start_date, end_date, date_type='everyday', insert=True)
                insert_update_time({MysqlTable.BASIC_INDEX_EVERYDAY: {'everyday': end_date},
                                    MysqlTable.BASIC_INDEX_EVERYDAY_PRO: {'everyday': end_date}})
                break
        else:
            print "Data of {start_date}-{end_date} is not ready!!!".format(start_date=start_date, end_date=end_date)
else:
    print "There are two different dates of {} and {}!!!".format(MysqlTable.BASIC_INDEX_EVERYDAY, MysqlTable.BASIC_INDEX_EVERYDAY_PRO)


date = get_update_ymd(MysqlTable.BASIC_INDEX_DAYS, date_type='7days')
date2 = get_update_ymd(MysqlTable.BASIC_INDEX_DAYS_PRO, date_type='7days')
if date == date2:
    start_date = get_date(date, 1) if date else START_YMD
    end_date = get_date(now_date, -1)
    if int(start_date) <= int(end_date):
        dates = get_days(start_date, end_date)
        dates.sort(reverse=True)
        for date in dates:
            if isdataready(date):
                end_date = date
                add_partition(MysqlTable.BASIC_INDEX_DAYS_PRO, end_date)
                add_partition(MysqlTable.BASIC_INDEX_DAYS, end_date)
                active_devices_days(start_date=start_date, end_date=end_date, date_type='7days', insert=True)
                timevalid_active_devices_days(start_date=start_date, end_date=end_date, date_type='7days', insert=True)
                insert_update_time({MysqlTable.BASIC_INDEX_DAYS: {'7days': end_date},
                                    MysqlTable.BASIC_INDEX_DAYS_PRO: {'7days': end_date}})
                break
        else:
            print "Data of {start_date}-{end_date} is not ready!!!".format(start_date=start_date, end_date=end_date)
else:
    print "There are two different dates of {} and {}!!!".format(MysqlTable.BASIC_INDEX_DAYS, MysqlTable.BASIC_INDEX_DAYS_PRO)


date = get_update_ymd(MysqlTable.BASIC_INDEX_DAYS, date_type='30days')
date2 = get_update_ymd(MysqlTable.BASIC_INDEX_DAYS_PRO, date_type='30days')
if date == date2:
    start_date = get_date(date, 1) if date else START_YMD
    end_date = get_date(now_date, -1)
    if int(start_date) <= int(end_date):
        dates = get_days(start_date, end_date)
        dates.sort(reverse=True)
        for date in dates:
            if isdataready(date):
                end_date = date
                add_partition(MysqlTable.BASIC_INDEX_DAYS_PRO, end_date)
                add_partition(MysqlTable.BASIC_INDEX_DAYS, end_date)
                active_devices_days(start_date=start_date, end_date=end_date, date_type='30days', insert=True)
                timevalid_active_devices_days(start_date=start_date, end_date=end_date, date_type='30days', insert=True)
                insert_update_time({MysqlTable.BASIC_INDEX_DAYS: {'30days': end_date},
                                    MysqlTable.BASIC_INDEX_DAYS_PRO: {'30days': end_date}})
                break
        else:
            print "Data of {start_date}-{end_date} is not ready!!!".format(start_date=start_date, end_date=end_date)
else:
    print "There are two different dates of {} and {}!!!".format(MysqlTable.BASIC_INDEX_DAYS, MysqlTable.BASIC_INDEX_DAYS_PRO)


date = get_update_ymd(MysqlTable.BASIC_INDEX_DAYS, date_type='month')
date2 = get_update_ymd(MysqlTable.BASIC_INDEX_DAYS_PRO, date_type='month')
if date == date2:
    start_date = get_date(date, 1) if date else START_YMD
    end_date = get_date(now_date, -1)
    if int(start_date) <= int(end_date):
        dates = get_days(start_date, end_date)
        dates.sort(reverse=True)
        for date in dates:
            if isdataready(date):
                end_date = date
                add_partition(MysqlTable.BASIC_INDEX_DAYS_PRO, end_date)
                add_partition(MysqlTable.BASIC_INDEX_DAYS, end_date)
                active_devices_days(start_date=start_date, end_date=end_date, date_type='month', insert=True)
                timevalid_active_devices_days(start_date=start_date, end_date=end_date, date_type='month', insert=True)
                insert_update_time({MysqlTable.BASIC_INDEX_DAYS: {'month': end_date},
                                    MysqlTable.BASIC_INDEX_DAYS_PRO: {'month': end_date}})
                break
        else:
            print "Data of {start_date}-{end_date} is not ready!!!".format(start_date=start_date, end_date=end_date)
else:
    print "There are two different dates of {} and {}!!!".format(MysqlTable.BASIC_INDEX_DAYS, MysqlTable.BASIC_INDEX_DAYS_PRO)


date = get_update_ymd(MysqlTable.SPECIFIC_INDEX, date_type='None')
start_date = get_date(date, 1) if date else START_YMD
end_date = get_date(now_date, -1)
if int(start_date) <= int(end_date):
    dates = get_days(start_date, end_date)
    dates.sort(reverse=True)
    for date in dates:
        if isdataready(date):
            end_date = date
            add_partition(MysqlTable.SPECIFIC_INDEX, end_date)
            watchtime_dist(start_date=start_date, end_date=end_date, date_type='everyday', insert=True)
            watchtime_dist(start_date=start_date, end_date=end_date, date_type='history', insert=True)
            insert_update_time({MysqlTable.SPECIFIC_INDEX: {'None': end_date}})
            break
    else:
        print "Data of {start_date}-{end_date} is not ready!!!".format(start_date=start_date, end_date=end_date)

file_output(path='udate_day.txt', date_type='day')

cost_time = int(time.time() - now_time)
print 'Start:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now_time))
print 'Cost time:', cost_time/3600, 'h', (cost_time % 3600)/60, 'm', cost_time % 60, 's'
print "========================================================="
