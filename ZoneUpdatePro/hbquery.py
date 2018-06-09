# coding: utf-8
import pprint

from method import *
from model import *


# Basic Every-Date Index
def play_times(start_date, end_date, date_type, query_pro=True, query_tag=True, insert=False):
    """
    收视次数查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：hour everyday
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    if date_type == 'hour':
        start_dates, end_dates = split_dates(start_date, end_date, step=24, date_type=date_type)
    elif date_type == 'everyday':
        start_dates, end_dates = split_dates(start_date, end_date, step=30, date_type=date_type)
    else:
        start_dates, end_dates = [start_date], [end_date]
    date_index = 'ymd' if date_type != 'hour' else 'ymdh'
    conn, cur = mysql_connect()
    count = 1
    for sdate, edate in zip(start_dates, end_dates):
        if query_pro:
            data = Data(index_name='play_times', date_type=date_type, index_type='PRO')
            data.init_sql(index=[date_index, 'province', 'content_id', 'count(*)'], date=[sdate, edate],
                          filtering=SqlWhere.COMMON, group_by=[date_index, 'province', 'content_id'],
                          order_by=[date_index, 'province', 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_tag:
            data = Data(index_name='play_times', date_type=date_type, index_type='TAG')
            data.init_sql(index=[date_index, 'province', 'Lower(tagid)', 'count(*)'], date=[sdate, edate],
                          filtering=SqlWhere.COMMON, group_by=[date_index, 'province', 'Lower(tagid)'],
                          order_by=[date_index, 'province', 'Lower(tagid)'])
            data.query()
            data.trans_to_dict()
            data.revise_index()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if sdate != edate:
            print 'Play times', sdate+'-'+edate, 'finished...'
        else:
            print 'Play times', sdate, 'finished...'

        if count >= 50 or edate == end_dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


def active_devices(start_date, end_date, date_type, query_pro=True, query_tag=True, insert=False):
    """
    活跃用户数查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：hour everyday
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    if date_type == 'hour':
        start_dates, end_dates = split_dates(start_date, end_date, step=24, date_type=date_type)
    elif date_type == 'everyday':
        start_dates, end_dates = split_dates(start_date, end_date, step=30, date_type=date_type)
    else:
        start_dates, end_dates = [start_date], [end_date]
    date_index = 'ymd' if date_type != 'hour' else 'ymdh'
    conn, cur = mysql_connect()
    count = 1
    for sdate, edate in zip(start_dates, end_dates):
        if query_pro:
            data = Data(index_name='active_devices', date_type=date_type, index_type='PRO')
            data.init_sql(index=[date_index, 'province', 'content_id', 'count(distinct device_id||province)'], date=[sdate, edate],
                          filtering=SqlWhere.COMMON, group_by=[date_index, 'province', 'content_id'],
                          order_by=[date_index, 'province', 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", 'content_id', 'count(distinct device_id||province)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON,
                          group_by=[date_index, 'content_id'], order_by=[date_index, 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_tag:
            data = Data(index_name='active_devices', date_type=date_type, index_type='TAG')
            data.init_sql(index=[date_index, 'province', 'Lower(tagid)', 'count(distinct device_id||province)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON,
                          group_by=[date_index, 'province', 'Lower(tagid)'],
                          order_by=[date_index, 'province', 'Lower(tagid)'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", 'Lower(tagid)', 'count(distinct device_id||province)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'Lower(tagid)'],
                          order_by=[date_index, 'Lower(tagid)'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            first_tags = FIRST_TAGS[:]
            first_tags.append('tagall')
            for tag in first_tags:
                if (tag in TAGS.keys() and TAGS[tag]) or (tag not in TAGS.keys() and tag == 'tagall'):
                    data.init_sql(index=[date_index, 'province', "'"+tag+"'", 'count(distinct device_id||province)'],
                                  date=[sdate, edate], tag=tag, filtering=SqlWhere.COMMON,
                                  group_by=[date_index, 'province'], order_by=[date_index, 'province'])
                    data.query()
                    data.trans_to_dict()
                    if insert:
                        data.cursor_execute(cur=cur)
                    else:
                        pprint.pprint(data.dicts)

            for tag in first_tags:
                if (tag in TAGS.keys() and TAGS[tag]) or (tag not in TAGS.keys() and tag == 'tagall'):
                    data.init_sql(index=[date_index, "'000000'", "'"+tag+"'", 'count(distinct device_id||province)'],
                                  date=[sdate, edate], tag=tag, filtering=SqlWhere.COMMON,
                                  group_by=[date_index], order_by=[date_index])
                    data.query()
                    data.trans_to_dict()
                    if insert:
                        data.cursor_execute(cur=cur)
                    else:
                        pprint.pprint(data.dicts)

        if sdate != edate:
            print 'Active devices', sdate+'-'+edate, 'finished...'
        else:
            print 'Active devices', sdate, 'finished...'

        if count >= 40 or edate == end_dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


def total_devices(start_date, end_date, date_type, query_pro=True, query_tag=True, query_area=True, insert=False):
    """
    累积用户数查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：hour everyday
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param query_area: 是否查询地区
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    dates = get_days(start_date, end_date) if date_type != 'hour' else get_hours(start_date, end_date)

    if date_type == 'hour':
        dates = [item for item in dates if item[-2:] == '23']  # total_devices只按天统计

    conn, cur = mysql_connect()
    count = 1
    for date in dates:
        if query_pro:
            data = Data(index_name='total_devices', date_type=date_type, index_type='PRO')
            data.init_sql(index=["'"+date+"'", "'000000'", 'content_id', 'count(distinct device_id||province)'], date=[date],
                          filtering=SqlWhere.COMMON, group_by=['content_id'], order_by=['content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_tag:
            data = Data(index_name='total_devices', date_type=date_type, index_type='TAG')
            data.init_sql(index=["'"+date+"'", "'000000'", 'Lower(tagid)', 'count(distinct device_id||province)'], date=[date],
                          filtering=SqlWhere.COMMON, group_by=['Lower(tagid)'], order_by=['Lower(tagid)'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            first_tags = FIRST_TAGS[:]
            first_tags.append('tagall')
            for tag in first_tags:
                if (tag in TAGS.keys() and TAGS[tag]) or (tag not in TAGS.keys() and tag == 'tagall'):
                    data.init_sql(index=["'"+date+"'", "'000000'", "'"+tag+"'", 'count(distinct device_id||province)'],
                                  date=[date], tag=tag, filtering=SqlWhere.COMMON)
                    data.query()
                    data.trans_to_dict()
                    if insert:
                        data.cursor_execute(cur=cur)
                    else:
                        pprint.pprint(data.dicts)

            tag_cross_area = ['uhd']
            for tag in tag_cross_area:
                data.init_sql(index=["'"+date+"'", 'province', "'"+tag+"'", 'count(distinct device_id||province)'],
                              date=[date], tag=tag, filtering=SqlWhere.COMMON, group_by=['province'], order_by=['province'])
                data.query()
                data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_area:
            data = Data(index_name='total_devices', date_type=date_type, index_type='AREA')
            data.init_sql(index=["'"+date+"'", 'province', "'tagall'", 'count(distinct device_id||province)'], date=[date],
                          filtering=SqlWhere.COMMON, group_by=['province'], order_by=['province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)
        print 'Total devices', date, 'finished...'

        if count >= 300 or date == dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


def watch_time(start_date, end_date, date_type, query_pro=True, query_tag=True, insert=False):
    """
    收视时长查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：hour everyday
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    if date_type == 'hour':
        start_dates, end_dates = split_dates(start_date, end_date, step=24, date_type=date_type)
    elif date_type == 'everyday':
        start_dates, end_dates = split_dates(start_date, end_date, step=30, date_type=date_type)
    else:
        start_dates, end_dates = [start_date], [end_date]
    date_index = 'ymd' if date_type != 'hour' else 'ymdh'
    conn, cur = mysql_connect()
    count = 1
    for sdate, edate in zip(start_dates, end_dates):
        if query_pro:
            data = Data(index_name='watch_time', date_type=date_type, index_type='PRO')
            data.init_sql(index=[date_index, 'province', 'content_id', 'sum(allwatchtime)'], date=[sdate, edate],
                          filtering=SqlWhere.COMMON, group_by=[date_index, 'province', 'content_id'],
                          order_by=[date_index, 'province', 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_tag:
            data = Data(index_name='watch_time', date_type=date_type, index_type='TAG')
            data.init_sql(index=[date_index, 'province', 'Lower(tagid)', 'sum(allwatchtime)'], date=[sdate, edate],
                          filtering=SqlWhere.COMMON, group_by=[date_index, 'province', 'Lower(tagid)'],
                          order_by=[date_index, 'province', 'Lower(tagid)'])
            data.query()
            data.trans_to_dict()
            data.revise_index()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if sdate != edate:
            print 'Watch time', sdate+'-'+edate, 'finished...'
        else:
            print 'Watch time', sdate, 'finished...'

        if count >= 50 or edate == end_dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


def timevalid_active_devices(start_date, end_date, date_type, query_pro=True, query_tag=True, insert=False):
    """
    收视时长不为零的活跃用户数查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：hour everyday
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    if date_type == 'hour':
        start_dates, end_dates = split_dates(start_date, end_date, step=24, date_type=date_type)
    elif date_type == 'everyday':
        start_dates, end_dates = split_dates(start_date, end_date, step=30, date_type=date_type)
    else:
        start_dates, end_dates = [start_date], [end_date]
    date_index = 'ymd' if date_type != 'hour' else 'ymdh'
    conn, cur = mysql_connect()
    count = 1
    for sdate, edate in zip(start_dates, end_dates):
        if query_pro:
            data = Data(index_name='timevalid_active_devices', date_type=date_type, index_type='PRO')
            data.init_sql(index=[date_index, 'province', 'content_id', 'count(distinct device_id||province)'], date=[sdate, edate],
                          filtering=SqlWhere.TOTAL_WATCHTIME_FLAG, group_by=[date_index, 'province', 'content_id'],
                          order_by=[date_index, 'province', 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", 'content_id', 'count(distinct device_id||province)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCHTIME_FLAG,
                          group_by=[date_index, 'content_id'],
                          order_by=[date_index, 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_tag:
            data = Data(index_name='timevalid_active_devices', date_type=date_type, index_type='TAG')
            data.init_sql(index=[date_index, 'province', 'Lower(tagid)', 'count(distinct device_id||province)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCHTIME_FLAG,
                          group_by=[date_index, 'province', 'Lower(tagid)'],
                          order_by=[date_index, 'province', 'Lower(tagid)'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", 'Lower(tagid)', 'count(distinct device_id||province)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCHTIME_FLAG,
                          group_by=[date_index, 'Lower(tagid)'], order_by=[date_index, 'Lower(tagid)'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            first_tags = FIRST_TAGS[:]
            first_tags.append('tagall')
            for tag in first_tags:
                if (tag in TAGS.keys() and TAGS[tag]) or (tag not in TAGS.keys() and tag == 'tagall'):
                    data.init_sql(index=[date_index, 'province', "'"+tag+"'", 'count(distinct device_id||province)'],
                                  date=[sdate, edate], tag=tag, filtering=SqlWhere.TOTAL_WATCHTIME_FLAG,
                                  group_by=[date_index, 'province'], order_by=[date_index, 'province'])
                    data.query()
                    data.trans_to_dict()
                    if insert:
                        data.cursor_execute(cur=cur)
                    else:
                        pprint.pprint(data.dicts)

            for tag in first_tags:
                if (tag in TAGS.keys() and TAGS[tag]) or (tag not in TAGS.keys() and tag == 'tagall'):
                    data.init_sql(index=[date_index, "'000000'", "'"+tag+"'", 'count(distinct device_id||province)'],
                                  date=[sdate, edate], tag=tag, filtering=SqlWhere.TOTAL_WATCHTIME_FLAG,
                                  group_by=[date_index], order_by=[date_index])
                    data.query()
                    data.trans_to_dict()
                    if insert:
                        data.cursor_execute(cur=cur)
                    else:
                        pprint.pprint(data.dicts)

        if sdate != edate:
            print 'Time-valid active devices', sdate+'-'+edate, 'finished...'
        else:
            print 'Time-valid active devices', sdate, 'finished...'

        if count >= 40 or edate == end_dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


# Basic Several-Day Index
def active_devices_days(start_date, end_date, date_type, query_pro=True, query_tag=True, insert=False):
    """
    活跃用户数多天查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：7days 30days month
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    if date_type == '7days':
        start_dates = get_days(max(get_date(start_date, -6), START_YMD), get_date(end_date, -6))
        end_dates = get_days(max(get_date(START_YMD, 6), start_date), end_date)
    elif date_type == '30days':
        start_dates = get_days(max(get_date(start_date, -29), START_YMD), get_date(end_date, -29))
        end_dates = get_days(max(get_date(START_YMD, 29), start_date), end_date)
    elif date_type == 'month':
        months = get_months(start_date, end_date, ledge=True, redge=False)
        start_dates, end_dates = [item[0] for item in months], [item[1] for item in months]
    else:
        start_dates, end_dates = [start_date], [end_date]
    conn, cur = mysql_connect()
    count = 1
    for sdate, edate in zip(start_dates, end_dates):
        if query_pro:
            data = Data(index_name='active_devices', date_type=date_type, index_type='PRO')
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', 'content_id', 'count(distinct device_id||province)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['province', 'content_id'],
                          order_by=['province', 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", 'content_id', 'count(distinct device_id||province)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['content_id'],
                          order_by=['content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_tag:
            data = Data(index_name='active_devices', date_type=date_type, index_type='TAG')
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', 'Lower(tagid)', 'count(distinct device_id||province)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['province', 'Lower(tagid)'],
                          order_by=['province', 'Lower(tagid)'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", 'Lower(tagid)', 'count(distinct device_id||province)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['Lower(tagid)'], order_by=['Lower(tagid)'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            first_tags = FIRST_TAGS[:]
            first_tags.append('tagall')
            for tag in first_tags:
                if (tag in TAGS.keys() and TAGS[tag]) or (tag not in TAGS.keys() and tag == 'tagall'):
                    data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', "'"+tag+"'", 'count(distinct device_id||province)'],
                                  date=[sdate, edate], tag=tag, filtering=SqlWhere.COMMON, group_by=['province'])
                    data.query()
                    data.trans_to_dict()
                    if insert:
                        data.cursor_execute(cur=cur)
                    else:
                        pprint.pprint(data.dicts)

            for tag in first_tags:
                if (tag in TAGS.keys() and TAGS[tag]) or (tag not in TAGS.keys() and tag == 'tagall'):
                    data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", "'"+tag+"'", 'count(distinct device_id||province)'],
                                  date=[sdate, edate], tag=tag, filtering=SqlWhere.COMMON)
                    data.query()
                    data.trans_to_dict()
                    if insert:
                        data.cursor_execute(cur=cur)
                    else:
                        pprint.pprint(data.dicts)

        print 'Active devices '+date_type, sdate + '-' + edate, 'finished...'

        if count >= 40 or edate == end_dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


def timevalid_active_devices_days(start_date, end_date, date_type, query_pro=True, query_tag=True, insert=False):
    """
    收视时长不为零的活跃用户数多天查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：7days 30days month
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    if date_type == '7days':
        start_dates = get_days(max(get_date(start_date, -6), START_YMD), get_date(end_date, -6))
        end_dates = get_days(max(get_date(START_YMD, 6), start_date), end_date)
    elif date_type == '30days':
        start_dates = get_days(max(get_date(start_date, -29), START_YMD), get_date(end_date, -29))
        end_dates = get_days(max(get_date(START_YMD, 29), start_date), end_date)
    elif date_type == 'month':
        months = get_months(start_date, end_date, ledge=True, redge=False)
        start_dates, end_dates = [item[0] for item in months], [item[1] for item in months]
    else:
        start_dates, end_dates = [start_date], [end_date]
    conn, cur = mysql_connect()
    count = 1
    for sdate, edate in zip(start_dates, end_dates):
        if query_pro:
            data = Data(index_name='timevalid_active_devices', date_type=date_type, index_type='PRO')
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', 'content_id', 'count(distinct device_id||province)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCHTIME_FLAG,
                          group_by=['province', 'content_id'], order_by=['province', 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", 'content_id', 'count(distinct device_id||province)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCHTIME_FLAG, group_by=['content_id'],
                          order_by=['content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_tag:
            data = Data(index_name='timevalid_active_devices', date_type=date_type, index_type='TAG')
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', 'Lower(tagid)', 'count(distinct device_id||province)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCHTIME_FLAG,
                          group_by=['province', 'Lower(tagid)'], order_by=['province', 'Lower(tagid)'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", 'Lower(tagid)', 'count(distinct device_id||province)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCHTIME_FLAG, group_by=['Lower(tagid)'],
                          order_by=['Lower(tagid)'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            first_tags = FIRST_TAGS[:]
            first_tags.append('tagall')
            for tag in first_tags:
                if (tag in TAGS.keys() and TAGS[tag]) or (tag not in TAGS.keys() and tag == 'tagall'):
                    data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', "'"+tag+"'", 'count(distinct device_id||province)'],
                                  date=[sdate, edate], tag=tag, filtering=SqlWhere.TOTAL_WATCHTIME_FLAG,
                                  group_by=['province'])
                    data.query()
                    data.trans_to_dict()
                    if insert:
                        data.cursor_execute(cur=cur)
                    else:
                        pprint.pprint(data.dicts)

            for tag in first_tags:
                if (tag in TAGS.keys() and TAGS[tag]) or (tag not in TAGS.keys() and tag == 'tagall'):
                    data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", "'"+tag+"'", 'count(distinct device_id||province)'],
                                  date=[sdate, edate], tag=tag, filtering=SqlWhere.TOTAL_WATCHTIME_FLAG)
                    data.query()
                    data.trans_to_dict()
                    if insert:
                        data.cursor_execute(cur=cur)
                    else:
                        pprint.pprint(data.dicts)

        print 'Time-valid active devices '+date_type, sdate + '-' + edate, 'finished...'

        if count >= 40 or edate == end_dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


# Specific Index
def watchtime_dist(start_date, end_date, date_type, query_tag=True, insert=False):
    """
    不同收视时长的用户数分布查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：everyday history
    :param query_tag: 是否查询频道
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    data = Data(index_name='watchtime_dist', date_type=date_type, index_type='TAG')
    if date_type == 'everyday':
        start_dates, end_dates = split_dates(start_date, end_date, step=1, date_type=date_type)
    elif date_type == 'history':
        start_dates, end_dates = [START_YMD] * len(get_days(start_date, end_date)), get_days(start_date, end_date)
    else:
        start_dates, end_dates = [start_date], [end_date]

    conn, cur = mysql_connect()
    count = 1
    for sdate, edate in zip(start_dates, end_dates):
        if query_tag:
            data.init_sql(index=['Lower(tagid) as tag', 'province', 'device_id', 'sum(allwatchtime) as allwatchtime'], date=[sdate, edate],
                          filtering=SqlWhere.COMMON, group_by=['tag', 'province', 'device_id'],
                          order_by=['tag', 'province', 'device_id'])
            subsql = data.sql
            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", 'a.tag', 'a.province', 'a.allwatchtime', 'count(distinct a.device_id)'],
                          table='(' + subsql + ') a', group_by=['a.tag', 'a.province', 'a.allwatchtime'],
                          order_by=['a.tag', 'a.province', 'a.allwatchtime'])
            data.query()
            data.org_index()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=['Lower(tagid) as tag', "'000000' as province", 'device_id', 'sum(allwatchtime) as allwatchtime'], date=[sdate, edate],
                          filtering=SqlWhere.COMMON, group_by=['tag', 'device_id'], order_by=['tag', 'device_id'])
            subsql = data.sql
            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", 'a.tag', 'a.province', 'a.allwatchtime', 'count(distinct a.device_id)'],
                          table='(' + subsql + ') a', group_by=['a.tag', 'a.province', 'a.allwatchtime'],
                          order_by=['a.tag', 'a.province', 'a.allwatchtime'])
            data.query()
            data.org_index()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            first_tags = FIRST_TAGS[:]
            first_tags.append('tagall')
            for tag in first_tags:
                if (tag in TAGS.keys() and TAGS[tag]) or (tag not in TAGS.keys() and tag == 'tagall'):
                    data.init_sql(index=["'" + tag + "' as tagid", 'province', 'device_id', 'sum(allwatchtime) as allwatchtime'],
                                  date=[sdate, edate], tag=tag, filtering=SqlWhere.COMMON, group_by=['province', 'device_id'],
                                  order_by=['province', 'device_id'])
                    subsql = data.sql
                    data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", 'a.tagid', 'a.province', 'a.allwatchtime', 'count(distinct a.device_id)'],
                                  table='(' + subsql + ') a', group_by=['a.tagid', 'a.province', 'a.allwatchtime'],
                                  order_by=['a.tagid', 'a.province', 'a.allwatchtime'])
                    data.query()
                    data.org_index()
                    data.trans_to_dict()
                    if insert:
                        data.cursor_execute(cur=cur)
                    else:
                        pprint.pprint(data.dicts)

                    data.init_sql(index=["'" + tag + "' as tagid", "'000000' as province", 'device_id', 'sum(allwatchtime) as allwatchtime'],
                                  date=[sdate, edate], tag=tag, filtering=SqlWhere.COMMON, group_by=['device_id'], order_by=['device_id'])
                    subsql = data.sql
                    data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", 'a.tagid', 'a.province', 'a.allwatchtime', 'count(distinct a.device_id)'],
                                  table='(' + subsql + ') a', group_by=['a.tagid', 'a.province', 'a.allwatchtime'], order_by=['a.tagid', 'a.province', 'a.allwatchtime'])
                    print data.sql
                    data.query()
                    data.org_index()
                    data.trans_to_dict()
                    if insert:
                        data.cursor_execute(cur=cur)
                    else:
                        pprint.pprint(data.dicts)

        if sdate != edate:
            print 'Watch time distribution '+date_type, sdate+'-'+edate, 'finished...'
        else:
            print 'Watch time distribution '+date_type, sdate, 'finished...'

        if count >= 5 or edate == end_dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


def timevalid_total_devices(end_hour, query_tag=True, insert=False):
    """
    收视时长不为零的累积用户数查询

    :param end_hour: 截止小时
    :param query_tag: 是否查询频道
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    conn, cur = mysql_connect()
    if query_tag:
        data = Data(index_name='timevalid_total_devices', date_type='history', index_type='TAG')
        data.init_sql(index=["'"+'2016091800'+"'", "'"+end_hour+"'", 'province', 'Lower(tagid)', 'count(distinct device_id||province)'],
                      date=[end_hour], filtering=SqlWhere.TOTAL_WATCHTIME_FLAG, group_by=['province', 'Lower(tagid)'],
                      order_by=['province', 'Lower(tagid)'])
        data.query()
        data.trans_to_dict()
        if insert:
            data.cursor_execute(cur=cur)
        else:
            pprint.pprint(data.dicts)

        data.init_sql(index=["'"+'2016091800'+"'", "'"+end_hour+"'", "'000000'", 'Lower(tagid)', 'count(distinct device_id||province)'],
                      date=[end_hour], filtering=SqlWhere.TOTAL_WATCHTIME_FLAG, group_by=['Lower(tagid)'],
                      order_by=['Lower(tagid)'])
        data.query()
        data.trans_to_dict()
        if insert:
            data.cursor_execute(cur=cur)
        else:
            pprint.pprint(data.dicts)

        first_tags = FIRST_TAGS[:]
        first_tags.append('tagall')
        for tag in first_tags:
            if (tag in TAGS.keys() and TAGS[tag]) or (tag not in TAGS.keys() and tag == 'tagall'):
                data.init_sql(index=["'"+'2016091800'+"'", "'"+end_hour+"'", 'province', "'" + tag + "'", 'count(distinct device_id||province)'],
                              date=[end_hour], tag=tag, filtering=SqlWhere.TOTAL_WATCHTIME_FLAG, group_by=['province'],
                              order_by=['province'])
                data.query()
                data.trans_to_dict()
                if insert:
                    data.cursor_execute(cur=cur)
                else:
                    pprint.pprint(data.dicts)

        for tag in first_tags:
            if (tag in TAGS.keys() and TAGS[tag]) or (tag not in TAGS.keys() and tag == 'tagall'):
                data.init_sql(index=["'"+'2016091800'+"'", "'"+end_hour+"'", "'000000'", "'" + tag + "'", 'count(distinct device_id||province)'],
                              date=[end_hour], tag=tag, filtering=SqlWhere.TOTAL_WATCHTIME_FLAG)
                data.query()
                data.trans_to_dict()
                if insert:
                    data.cursor_execute(cur=cur)
                else:
                    pprint.pprint(data.dicts)

        if insert:
            conn.commit()

        print 'Time-valid total devices', end_hour, 'finished...'
    mysql_close(conn, cur)


# Real-time Port
def pro_total_devices(end_hour, program_id, area_id):
    """
    某个节目在某个地区的累计用户查询

    :param end_hour: 截止小时
    :param program_id: 节目id
    :param area_id: 地区id
    :return: 累计用户数
    """
    data = Data('pro_total_devices', date_type='hour', index_type='Pro')
    data.init_sql(index=['count(distinct device_id||province)'], date=[end_hour], program=program_id, area=area_id,
                  filtering=SqlWhere.COMMON)
    data.query()
    return int(data.data[0][0])
