# coding: utf-8
import pprint

from config import START_YMD
from method import *
from model import *

# 涉及频道时如何处理点击数据和没有采集到频道信息的收视数据？
# 统计中是否包含全国和全部频道的组合？


# Basic Every-Date Index
def play_times(start_date, end_date, date_type, query_pro=True, query_tag=True, query_dev=True, insert=False):
    """
    收视次数查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：hour everyday
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param query_dev: 是否查询设备
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
                          filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'province', 'content_id'],
                          order_by=[date_index, 'province', 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_tag:
            data = Data(index_name='play_times', date_type=date_type, index_type='TAG')
            data.init_sql(index=[date_index, 'province', 'tagid', "'all'", "'all'", "'all'", 'count(*)'],
                          date=[sdate, edate], filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'province', 'tagid'],
                          order_by=[date_index, 'province', 'tagid'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', 'tagid2', "'all'", "'all'", "'all'", 'count(*)'],
                          date=[sdate, edate], filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'province', 'tagid2'],
                          order_by=[date_index, 'province', 'tagid2'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', "'tagother'", "'all'", "'all'", "'all'", 'count(*)'],
                          date=[sdate, edate], tag='tagother', filtering=SqlWhere.PROGRAM_FLAG,
                          group_by=[date_index, 'province'], order_by=[date_index, 'province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_dev:
            data = Data(index_name='play_times', date_type=date_type, index_type='DEV')
            data.init_sql(index=[date_index, "'000000'", "'tagall'", 'app_key', "'all'", "'all'", 'count(*)'],
                          date=[sdate, edate], filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'app_key'],
                          order_by=[date_index, 'app_key'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data = Data(index_name='play_times', date_type=date_type, index_type='DEV')
            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", 'app_version_name', "'all'", 'count(*)'],
                          date=[sdate, edate], filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'app_version_name'],
                          order_by=[date_index, 'app_version_name'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data = Data(index_name='play_times', date_type=date_type, index_type='DEV')
            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", "'all'", 'channel', 'count(*)'],
                          date=[sdate, edate], filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'channel'],
                          order_by=[date_index, 'channel'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if sdate != edate:
            print 'Play times', sdate+'-'+edate, 'finished...'
        else:
            print 'Play times', sdate, 'finished...'

        if count >= 50 or edate == end_dates[-1]:  # 查询一段时间commit一次
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


def clicks(start_date, end_date, date_type, query_pro=True, query_tag=True, query_dev=True, insert=False):
    """
    点击量查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：hour everyday
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param query_dev: 是否查询设备
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
            data = Data(index_name='clicks', date_type=date_type, index_type='PRO')
            data.init_sql(index=[date_index, 'province', 'content_id', 'count(*)'], date=[sdate, edate],
                          filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'province', 'content_id'],
                          order_by=[date_index, 'province', 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_tag:  # 对于某一频道，收视数和点击数应该相等，因为点击数据中均频道均为空，其它频道的点击数和收视数应有差别
            data = Data(index_name='clicks', date_type=date_type, index_type='TAG')
            data.init_sql(index=[date_index, 'province', 'tagid', "'all'", "'all'", "'all'", 'count(*)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'province', 'tagid'],
                          order_by=[date_index, 'province', 'tagid'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', 'tagid2', "'all'", "'all'", "'all'", 'count(*)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'province', 'tagid2'],
                          order_by=[date_index, 'province', 'tagid2'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', "'tagother'", "'all'", "'all'", "'all'", 'count(*)'],
                          date=[sdate, edate], tag='tagother', filtering=SqlWhere.COMMON,
                          group_by=[date_index, 'province'], order_by=[date_index, 'province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_dev:
            data = Data(index_name='clicks', date_type=date_type, index_type='DEV')
            data.init_sql(index=[date_index, "'000000'", "'tagall'", 'app_key', "'all'", "'all'", 'count(*)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'app_key'],
                          order_by=[date_index, 'app_key'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", 'app_version_name', "'all'", 'count(*)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'app_version_name'],
                          order_by=[date_index, 'app_version_name'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", "'all'", 'channel', 'count(*)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'channel'],
                          order_by=[date_index, 'channel'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if sdate != edate:
            print 'Clicks', sdate+'-'+edate, 'finished...'
        else:
            print 'Clicks', sdate, 'finished...'

        if count >= 50 or edate == end_dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


def active_devices(start_date, end_date, date_type, query_pro=True, query_tag=True, query_dev=True, insert=False):
    """
    活跃用户数查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：hour everyday
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param query_dev: 是否查询设备
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
            data.init_sql(index=[date_index, 'province', 'content_id', 'count(distinct device_id)'], date=[sdate, edate],
                          filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'province', 'content_id'],
                          order_by=[date_index, 'province', 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", 'content_id', 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'content_id'],
                          order_by=[date_index, 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_tag:
            data = Data(index_name='active_devices', date_type=date_type, index_type='TAG')
            data.init_sql(index=[date_index, 'province', 'tagid', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'province', 'tagid'],
                          order_by=[date_index, 'province', 'tagid'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", 'tagid', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'tagid'],
                          order_by=[date_index, 'tagid'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', 'tagid2', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'province', 'tagid2'],
                          order_by=[date_index, 'province', 'tagid2'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", 'tagid2', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'tagid2'],
                          order_by=[date_index, 'tagid2'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', "'tagother'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], tag='tagother', filtering=SqlWhere.COMMON, group_by=[date_index, 'province'],
                          order_by=[date_index, 'province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagother'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], tag='tagother', filtering=SqlWhere.COMMON, group_by=[date_index],
                          order_by=[date_index])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', "'tagall'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'province'],
                          order_by=[date_index, 'province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index], order_by=[date_index])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_dev:
            data = Data(index_name='active_devices', date_type=date_type, index_type='DEV')
            data.init_sql(index=[date_index, "'000000'", "'tagall'", 'app_key', "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'app_key'],
                          order_by=[date_index, 'app_key'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", 'app_version_name', "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'app_version_name'],
                          order_by=[date_index, 'app_version_name'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", "'all'", 'channel', 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'channel'],
                          order_by=[date_index, 'channel'])
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


def total_devices(start_date, end_date, date_type, query_pro=True, query_tag=True, query_area=True, query_dev=True, insert=False):
    """
    累积用户数查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：hour everyday
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param query_area: 是否查询地区
    :param query_dev: 是否查询设备
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    dates = get_days(start_date, end_date) if date_type != 'hour' else get_hours(start_date, end_date)

    if date_type == 'hour':
        dates = [item for item in dates if item[-2:] == '23']  # total_devices只按天统计

    conn, cur = mysql_connect()
    count = 1
    for date in dates:  # filtering应与active_devices相同
        if query_pro:
            data = Data(index_name='total_devices', date_type=date_type, index_type='PRO')
            data.init_sql(index=["'"+date+"'", "'000000'", 'content_id', 'count(distinct device_id)'], date=[date],
                          filtering=SqlWhere.PROGRAM_FLAG, group_by=['content_id'], order_by=['content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_tag:
            data = Data(index_name='total_devices', date_type=date_type, index_type='TAG')
            data.init_sql(index=["'"+date+"'", "'000000'", 'tagid', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[date], filtering=SqlWhere.COMMON, group_by=['tagid'], order_by=['tagid'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'" + date + "'", "'000000'", 'tagid2', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[date], filtering=SqlWhere.COMMON, group_by=['tagid2'], order_by=['tagid2'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'" + date + "'", "'000000'", "'tagother'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[date], tag='tagother', filtering=SqlWhere.COMMON)
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'" + date + "'", "'000000'", "'tagall'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[date], filtering=SqlWhere.COMMON)
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_area:
            data = Data(index_name='total_devices', date_type=date_type, index_type='AREA')
            data.init_sql(index=["'"+date+"'", 'province', "'tagall'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[date], filtering=SqlWhere.COMMON, group_by=['province'], order_by=['province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_dev:
            data = Data(index_name='total_devices', date_type=date_type, index_type='DEV')
            data.init_sql(index=["'" + date + "'", "'000000'", "'tagall'", 'app_key', "'all'", "'all'", 'count(distinct device_id)'],
                          date=[date], filtering=SqlWhere.COMMON, group_by=['app_key'], order_by=['app_key'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'" + date + "'", "'000000'", "'tagall'", "'all'", 'app_version_name', "'all'", 'count(distinct device_id)'],
                          date=[date], filtering=SqlWhere.COMMON, group_by=['app_version_name'], order_by=['app_version_name'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'" + date + "'", "'000000'", "'tagall'", "'all'", "'all'", 'channel', 'count(distinct device_id)'],
                          date=[date], filtering=SqlWhere.COMMON, group_by=['channel'], order_by=['channel'])
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


def watch_time(start_date, end_date, date_type, query_pro=True, query_tag=True, query_dev=True, insert=False):
    """
    收视时长查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：hour everyday
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param query_dev: 是否查询设备
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
    for sdate, edate in zip(start_dates, end_dates):  # filtering应与play_times相同
        if query_pro:
            data = Data(index_name='watch_time', date_type=date_type, index_type='PRO')
            data.init_sql(index=[date_index, 'province', 'content_id', 'sum(allwatchtime)'], date=[sdate, edate],
                          filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'province', 'content_id'],
                          order_by=[date_index, 'province', 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_tag:
            data = Data(index_name='watch_time', date_type=date_type, index_type='TAG')
            data.init_sql(index=[date_index, 'province', 'tagid', "'all'", "'all'", "'all'", 'sum(allwatchtime)'],
                          date=[sdate, edate], filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'province', 'tagid'],
                          order_by=[date_index, 'province', 'tagid'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', 'tagid2', "'all'", "'all'", "'all'", 'sum(allwatchtime)'],
                          date=[sdate, edate], filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'province', 'tagid2'],
                          order_by=[date_index, 'province', 'tagid2'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', "'tagother'", "'all'", "'all'", "'all'", 'sum(allwatchtime)'],
                          date=[sdate, edate], tag='tagother', filtering=SqlWhere.PROGRAM_FLAG,
                          group_by=[date_index, 'province'], order_by=[date_index, 'province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_dev:
            data = Data(index_name='watch_time', date_type=date_type, index_type='DEV')
            data.init_sql(index=[date_index, "'000000'", "'tagall'", 'app_key', "'all'", "'all'", 'sum(allwatchtime)'],
                          date=[sdate, edate], filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'app_key'],
                          order_by=[date_index, 'app_key'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", 'app_version_name', "'all'", 'sum(allwatchtime)'],
                          date=[sdate, edate], filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'app_version_name'],
                          order_by=[date_index, 'app_version_name'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", "'all'", 'channel', 'sum(allwatchtime)'],
                          date=[sdate, edate], filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'channel'],
                          order_by=[date_index, 'channel'])
            data.query()
            data.trans_to_dict()
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


def use_time(start_date, end_date, date_type, query_tag=True, query_dev=True, insert=False):
    """
    使用时长查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：hour everyday
    :param query_tag: 是否查询频道
    :param query_dev: 是否查询设备
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
    for sdate, edate in zip(start_dates, end_dates):  # filtering应与clicks相同
        if query_tag:
            data = Data(index_name='use_time', date_type=date_type, index_type='TAG')
            data.init_sql(index=[date_index, 'province', 'tagid', "'all'", "'all'", "'all'", 'sum(allwatchtime)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'province', 'tagid'],
                          order_by=[date_index, 'province', 'tagid'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', 'tagid2', "'all'", "'all'", "'all'", 'sum(allwatchtime)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'province', 'tagid2'],
                          order_by=[date_index, 'province', 'tagid2'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', "'tagother'", "'all'", "'all'", "'all'", 'sum(allwatchtime)'],
                          date=[sdate, edate], tag='tagother', filtering=SqlWhere.COMMON,
                          group_by=[date_index, 'province'], order_by=[date_index, 'province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_dev:
            data = Data(index_name='use_time', date_type=date_type, index_type='DEV')
            data.init_sql(index=[date_index, "'000000'", "'tagall'", 'app_key', "'all'", "'all'", 'sum(allwatchtime)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'app_key'],
                          order_by=[date_index, 'app_key'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", 'app_version_name', "'all'", 'sum(allwatchtime)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'app_version_name'],
                          order_by=[date_index, 'app_version_name'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", "'all'", 'channel', 'sum(allwatchtime)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'channel'],
                          order_by=[date_index, 'channel'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if sdate != edate:
            print 'Use time', sdate+'-'+edate, 'finished...'
        else:
            print 'Use time', sdate, 'finished...'

        if count >= 50 or edate == end_dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


def launch_times(start_date, end_date, date_type, query_area=True, query_dev=True, insert=False):
    """
    启动次数查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：hour everyday
    :param query_area: 是否查询地区
    :param query_dev: 是否查询设备
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
    for sdate, edate in zip(start_dates, end_dates):  # filtering应与active_devices相同
        if query_area:
            data = Data(index_name='launch_times', date_type=date_type, index_type='AREA')
            data.init_sql(index=[date_index, 'province', "'tagall'", "'all'", "'all'", "'all'", 'count(distinct session_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'province'],
                          order_by=[date_index, 'province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", "'all'", "'all'", 'count(distinct session_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index], order_by=[date_index])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_dev:
            data = Data(index_name='launch_times', date_type=date_type, index_type='DEV')
            data.init_sql(index=[date_index, "'000000'", "'tagall'", 'app_key', "'all'", "'all'", 'count(distinct session_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'app_key'],
                          order_by=[date_index, 'app_key'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", 'app_version_name', "'all'", 'count(distinct session_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'app_version_name'],
                          order_by=[date_index, 'app_version_name'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", "'all'", 'channel', 'count(distinct session_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=[date_index, 'channel'],
                          order_by=[date_index, 'channel'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if sdate != edate:
            print 'Launch times', sdate+'-'+edate, 'finished...'
        else:
            print 'Launch times', sdate, 'finished...'

        if count >= 40 or edate == end_dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


def timevalid_active_devices(start_date, end_date, date_type, query_pro=True, query_tag=True, query_dev=True, insert=False):
    """
    收视时长不为零的活跃用户数查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：hour everyday
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param query_dev: 是否查询设备
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
    for sdate, edate in zip(start_dates, end_dates):  # 此指标主要用于计算人均收视时长，故其filtering应只比watch_time多加一条allwatchtime>0的条件
        if query_pro:
            data = Data(index_name='timevalid_active_devices', date_type=date_type, index_type='PRO')
            data.init_sql(index=[date_index, 'province', 'content_id', 'count(distinct device_id)'], date=[sdate, edate],
                          filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=[date_index, 'province', 'content_id'],
                          order_by=[date_index, 'province', 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", 'content_id', 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG,
                          group_by=[date_index, 'content_id'], order_by=[date_index, 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_tag:
            data = Data(index_name='timevalid_active_devices', date_type=date_type, index_type='TAG')
            data.init_sql(index=[date_index, 'province', 'tagid', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=[date_index, 'province', 'tagid'],
                          order_by=[date_index, 'province', 'tagid'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", 'tagid', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=[date_index, 'tagid'],
                          order_by=[date_index, 'tagid'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', 'tagid2', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=[date_index, 'province', 'tagid2'],
                          order_by=[date_index, 'province', 'tagid2'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", 'tagid2', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=[date_index, 'tagid2'],
                          order_by=[date_index, 'tagid2'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', "'tagother'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], tag='tagother', filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=[date_index, 'province'],
                          order_by=[date_index, 'province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagother'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], tag='tagother', filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=[date_index],
                          order_by=[date_index])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', "'tagall'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=[date_index, 'province'],
                          order_by=[date_index, 'province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=[date_index],
                          order_by=[date_index])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_dev:
            data = Data(index_name='timevalid_active_devices', date_type=date_type, index_type='DEV')
            data.init_sql(index=[date_index, "'000000'", "'tagall'", 'app_key', "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=[date_index, 'app_key'],
                          order_by=[date_index, 'app_key'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", 'app_version_name', "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=[date_index, 'app_version_name'],
                          order_by=[date_index, 'app_version_name'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagall'", "'all'", "'all'", 'channel', 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=[date_index, 'channel'],
                          order_by=[date_index, 'channel'])
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
def active_devices_days(start_date, end_date, date_type, query_pro=True, query_tag=True, query_dev=True, insert=False):
    """
    活跃用户数多天查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：7days 30days month
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param query_dev: 是否查询设备
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    if date_type == '7days':
        start_dates = get_days(max(get_date(start_date, -6), START_YMD), get_date(end_date, -6))  # 要考虑到更新
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
    for sdate, edate in zip(start_dates, end_dates):  # filtering应与active_devices相同
        if query_pro:
            data = Data(index_name='active_devices', date_type=date_type, index_type='PRO')
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', 'content_id', 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.PROGRAM_FLAG, group_by=['province', 'content_id'],
                          order_by=['province', 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", 'content_id', 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.PROGRAM_FLAG, group_by=['content_id'],
                          order_by=['content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_tag:
            data = Data(index_name='active_devices', date_type=date_type, index_type='TAG')
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', 'tagid', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['province', 'tagid'],
                          order_by=['province', 'tagid'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", 'tagid', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['tagid'], order_by=['tagid'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', 'tagid2', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['province', 'tagid2'],
                          order_by=['province', 'tagid2'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", 'tagid2', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['tagid2'], order_by=['tagid2'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", 'province', "'tagother'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], tag='tagother', filtering=SqlWhere.COMMON, group_by=['province'], order_by=['province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", "'000000'", "'tagother'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], tag='tagother', filtering=SqlWhere.COMMON)
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', "'tagall'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['province'], order_by=['province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", "'tagall'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON)
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_dev:
            data = Data(index_name='active_devices', date_type=date_type, index_type='DEV')
            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", "'000000'", "'tagall'", 'app_key', "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['app_key'], order_by=['app_key'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", "'000000'", "'tagall'", "'all'", 'app_version_name', "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['app_version_name'], order_by=['app_version_name'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", "'000000'", "'tagall'", "'all'", "'all'", 'channel', 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['channel'], order_by=['channel'])
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


def timevalid_active_devices_days(start_date, end_date, date_type, query_pro=True, query_tag=True, query_dev=True, insert=False):
    """
    收视时长不为零的活跃用户数多天查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：7days 30days month
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param query_dev: 是否查询设备
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
    for sdate, edate in zip(start_dates, end_dates):  # 用于计算人均收视时长，filtering应与timevalid_active_devices相同
        if query_pro:
            data = Data(index_name='timevalid_active_devices', date_type=date_type, index_type='PRO')
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', 'content_id', 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['province', 'content_id'],
                          order_by=['province', 'content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", 'content_id', 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['content_id'],
                          order_by=['content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_tag:
            data = Data(index_name='timevalid_active_devices', date_type=date_type, index_type='TAG')
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', 'tagid', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['province', 'tagid'],
                          order_by=['province', 'tagid'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", 'tagid', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['tagid'], order_by=['tagid'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', 'tagid2', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['province', 'tagid2'],
                          order_by=['province', 'tagid2'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", 'tagid2', "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['tagid2'], order_by=['tagid2'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", 'province', "'tagother'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], tag='tagother', filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['province'],
                          order_by=['province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", "'000000'", "'tagother'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], tag='tagother', filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG)
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', "'tagall'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['province'], order_by=['province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", "'tagall'", "'all'", "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG)
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_dev:
            data = Data(index_name='timevalid_active_devices', date_type=date_type, index_type='DEV')
            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", "'000000'", "'tagall'", 'app_key', "'all'", "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['app_key'], order_by=['app_key'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", "'000000'", "'tagall'", "'all'", 'app_version_name', "'all'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['app_version_name'], order_by=['app_version_name'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", "'000000'", "'tagall'", "'all'", "'all'", 'channel', 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['channel'], order_by=['channel'])
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


def launch_times_days(start_date, end_date, date_type, query_area=True, query_dev=True, insert=False):
    """
    启动次数多天查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：7days 30days month
    :param query_area: 是否查询地区
    :param query_dev: 是否查询设备
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
    for sdate, edate in zip(start_dates, end_dates):  # 所有filtering应与launch_times_days相同
        if query_area:
            data = Data(index_name='launch_times', date_type=date_type, index_type='AREA')
            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", 'province', "'tagall'", "'all'", "'all'", "'all'", 'count(distinct session_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['province'], order_by=['province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", "'000000'", "'tagall'", "'all'", "'all'", "'all'", 'count(distinct session_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON)
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_dev:
            data = Data(index_name='launch_times', date_type=date_type, index_type='DEV')
            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", "'000000'", "'tagall'", 'app_key', "'all'", "'all'", 'count(distinct session_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['app_key'], order_by=['app_key'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", "'000000'", "'tagall'", "'all'", 'app_version_name', "'all'", 'count(distinct session_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['app_version_name'], order_by=['app_version_name'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", "'000000'", "'tagall'", "'all'", "'all'", 'channel', 'count(distinct session_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['channel'], order_by=['channel'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        print 'Launch times '+date_type, sdate + '-' + edate, 'finished...'

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
            data.init_sql(index=['tagid', 'province', 'device_id', 'sum(allwatchtime) as allwatchtime'], date=[sdate, edate],
                          filtering=SqlWhere.PROGRAM_FLAG, group_by=['tagid', 'province', 'device_id'], order_by=['tagid', 'province', 'device_id'])
            subsql = data.sql
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'a.tagid', 'a.province', 'a.allwatchtime', 'count(distinct a.device_id)'],
                          table='('+subsql+') a', group_by=['a.tagid', 'a.province', 'a.allwatchtime'], order_by=['a.tagid', 'a.province', 'a.allwatchtime'])
            data.query()
            data.org_index()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=['tagid', "'000000' as province", 'device_id', 'sum(allwatchtime) as allwatchtime'], date=[sdate, edate],
                          filtering=SqlWhere.PROGRAM_FLAG, group_by=['tagid', 'device_id'], order_by=['tagid', 'device_id'])
            subsql = data.sql
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'a.tagid', 'a.province', 'a.allwatchtime', 'count(distinct a.device_id)'],
                          table='('+subsql+') a', group_by=['a.tagid', 'a.province', 'a.allwatchtime'], order_by=['a.tagid', 'a.province', 'a.allwatchtime'])
            data.query()
            data.org_index()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=['tagid2', 'province', 'device_id', 'sum(allwatchtime) as allwatchtime'], date=[sdate, edate],
                          filtering=SqlWhere.PROGRAM_FLAG, group_by=['tagid2', 'province', 'device_id'], order_by=['tagid2', 'province', 'device_id'])
            subsql = data.sql
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'a.tagid2', 'a.province', 'a.allwatchtime', 'count(distinct a.device_id)'],
                          table='('+subsql+') a', group_by=['a.tagid2', 'a.province', 'a.allwatchtime'], order_by=['a.tagid2', 'a.province', 'a.allwatchtime'])
            data.query()
            data.org_index()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=['tagid2', "'000000' as province", 'device_id', 'sum(allwatchtime) as allwatchtime'], date=[sdate, edate],
                          filtering=SqlWhere.PROGRAM_FLAG, group_by=['tagid2', 'device_id'], order_by=['tagid2', 'device_id'])
            subsql = data.sql
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'a.tagid2', 'a.province', 'a.allwatchtime', 'count(distinct a.device_id)'],
                          table='('+subsql+') a', group_by=['a.tagid2', 'a.province', 'a.allwatchtime'], order_by=['a.tagid2', 'a.province', 'a.allwatchtime'])
            data.query()
            data.org_index()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'tagall' as tagid", 'province', 'device_id', 'sum(allwatchtime) as allwatchtime'], date=[sdate, edate],
                          filtering=SqlWhere.PROGRAM_FLAG, group_by=['province', 'device_id'], order_by=['province', 'device_id'])
            subsql = data.sql
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'a.tagid', 'a.province', 'a.allwatchtime', 'count(distinct a.device_id)'],
                          table='('+subsql+') a', group_by=['a.tagid', 'a.province', 'a.allwatchtime'], order_by=['a.tagid', 'a.province', 'a.allwatchtime'])
            data.query()
            data.org_index()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'tagall' as tagid", "'000000' as province", 'device_id', 'sum(allwatchtime) as allwatchtime'], date=[sdate, edate],
                          filtering=SqlWhere.PROGRAM_FLAG, group_by=['device_id'], order_by=['device_id'])
            subsql = data.sql
            data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", 'a.tagid', 'a.province', 'a.allwatchtime', 'count(distinct a.device_id)'],
                          table='(' + subsql + ') a', group_by=['a.tagid', 'a.province', 'a.allwatchtime'], order_by=['a.tagid', 'a.province', 'a.allwatchtime'])
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


def usetime_dist(start_date, end_date, date_type, query_tag=True, insert=False):
    """
    不同使用时长的用户数分布查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：everyday history
    :param query_tag: 是否查询频道
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    data = Data(index_name='usetime_dist', date_type=date_type, index_type='TAG')
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
            data.init_sql(index=['tagid', 'province', 'device_id', 'sum(allwatchtime) as allwatchtime'], date=[sdate, edate],
                          filtering=SqlWhere.COMMON, group_by=['tagid', 'province', 'device_id'], order_by=['tagid', 'province', 'device_id'])
            subsql = data.sql
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'a.tagid', 'a.province', 'a.allwatchtime', 'count(distinct a.device_id)'],
                          table='('+subsql+') a', group_by=['a.tagid', 'a.province', 'a.allwatchtime'], order_by=['a.tagid', 'a.province', 'a.allwatchtime'])
            data.query()
            data.org_index()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=['tagid', "'000000' as province", 'device_id', 'sum(allwatchtime) as allwatchtime'], date=[sdate, edate],
                          filtering=SqlWhere.COMMON, group_by=['tagid', 'device_id'], order_by=['tagid', 'device_id'])
            subsql = data.sql
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'a.tagid', 'a.province', 'a.allwatchtime', 'count(distinct a.device_id)'],
                          table='('+subsql+') a', group_by=['a.tagid', 'a.province', 'a.allwatchtime'], order_by=['a.tagid', 'a.province', 'a.allwatchtime'])
            data.query()
            data.org_index()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=['tagid2', 'province', 'device_id', 'sum(allwatchtime) as allwatchtime'], date=[sdate, edate],
                          filtering=SqlWhere.COMMON, group_by=['tagid2', 'province', 'device_id'], order_by=['tagid2', 'province', 'device_id'])
            subsql = data.sql
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'a.tagid2', 'a.province', 'a.allwatchtime', 'count(distinct a.device_id)'],
                          table='('+subsql+') a', group_by=['a.tagid2', 'a.province', 'a.allwatchtime'], order_by=['a.tagid2', 'a.province', 'a.allwatchtime'])
            data.query()
            data.org_index()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=['tagid2', "'000000' as province", 'device_id', 'sum(allwatchtime) as allwatchtime'], date=[sdate, edate],
                          filtering=SqlWhere.COMMON, group_by=['tagid2', 'device_id'], order_by=['tagid2', 'device_id'])
            subsql = data.sql
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'a.tagid2', 'a.province', 'a.allwatchtime', 'count(distinct a.device_id)'],
                          table='('+subsql+') a', group_by=['a.tagid2', 'a.province', 'a.allwatchtime'], order_by=['a.tagid2', 'a.province', 'a.allwatchtime'])
            data.query()
            data.org_index()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'tagall' as tagid", 'province', 'device_id', 'sum(allwatchtime) as allwatchtime'], date=[sdate, edate],
                          filtering=SqlWhere.COMMON, group_by=['province', 'device_id'], order_by=['province', 'device_id'])
            subsql = data.sql
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'a.tagid', 'a.province', 'a.allwatchtime', 'count(distinct a.device_id)'],
                          table='('+subsql+') a', group_by=['a.tagid', 'a.province', 'a.allwatchtime'], order_by=['a.tagid', 'a.province', 'a.allwatchtime'])
            data.query()
            data.org_index()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'tagall' as tagid", "'000000' as province", 'device_id', 'sum(allwatchtime) as allwatchtime'], date=[sdate, edate],
                          filtering=SqlWhere.COMMON, group_by=['device_id'], order_by=['device_id'])
            subsql = data.sql
            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'a.tagid', 'a.province', 'a.allwatchtime', 'count(distinct a.device_id)'],
                          table='('+subsql+') a', group_by=['a.tagid', 'a.province', 'a.allwatchtime'], order_by=['a.tagid', 'a.province', 'a.allwatchtime'])
            data.query()
            data.org_index()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if sdate != edate:
            print 'Use time distribution '+date_type, sdate+'-'+edate, 'finished...'
        else:
            print 'Use time distribution '+date_type, sdate, 'finished...'

        if count >= 5 or edate == end_dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


def new_retention(start_date, end_date, date_type, insert=False):
    """
    新增用户留存查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：day week month
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    data = Data(index_name='new_retention', date_type=date_type, index_type='TAG')
    dates = []
    if date_type == 'day':
        end_dates = get_days(start_date, end_date)
        for edate in end_dates:
            start_dates = get_days(get_date(edate, -7), get_date(edate, -1))
            for sdate in start_dates:
                dates.append(((sdate, sdate), (edate, edate)))
    elif date_type == 'week':
        end_weeks = get_weeks(start_date, end_date, ledge=True, redge=False)
        for eweek in end_weeks:
            start_weeks = get_weeks(get_date(eweek[0], -7*7), get_date(eweek[1], -1*7))
            for sweek in start_weeks:
                dates.append((sweek, eweek))
    elif date_type == 'month':
        end_months = get_months(start_date, end_date, ledge=True, redge=False)
        for emonth in end_months:
            start_months = get_months(get_date(emonth[0][:6], -7)+'01', get_date(emonth[1][:6], -1)+'01', ledge=False, redge=True)
            for smonth in start_months:
                dates.append((smonth, emonth))
    else:
        pass

    conn, cur = mysql_connect()
    count = 1
    for sdate, edate in dates:  # 嵌套语句
        data.init_sql(index=['distinct device_id'], date=[edate[0], edate[1]])
        subsql = data.sql
        data.init_sql(index=['device_id', 'min(ymd)'], date=[sdate[1]], device_in=subsql, group_by=['device_id'], having='min(ymd)>={sdate1} and min(ymd)<={sdate2}'.format(sdate1=sdate[0], sdate2=sdate[1]))
        data.query()
        data.data = [[sdate[0], edate[0], 'tagall', '000000', len(data.data)]]
        data.trans_to_dict()
        if insert:
            data.cursor_execute(cur=cur)
        else:
            pprint.pprint(data.dicts)

        print 'New device retention of', sdate[0], 'in', edate[0], date_type, 'finished...'

        if count >= 30 or (sdate, edate) == dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


def active_retention(start_date, end_date, date_type, insert=False):
    """
    活跃用户留存查询

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：day week month
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    data = Data(index_name='active_retention', date_type=date_type, index_type='TAG')
    dates = []
    if date_type == 'day':
        end_dates = get_days(start_date, end_date)
        for edate in end_dates:
            start_dates = get_days(get_date(edate, -7), get_date(edate, -1))
            for sdate in start_dates:
                dates.append(((sdate, sdate), (edate, edate)))
    elif date_type == 'week':
        end_weeks = get_weeks(start_date, end_date, ledge=True, redge=False)
        for eweek in end_weeks:
            start_weeks = get_weeks(get_date(eweek[0], -7*7), get_date(eweek[1], -1*7))
            for sweek in start_weeks:
                dates.append((sweek, eweek))
    elif date_type == 'month':
        end_months = get_months(start_date, end_date, ledge=True, redge=False)
        for emonth in end_months:
            start_months = get_months(get_date(emonth[0][:6], -7)+'01', get_date(emonth[1][:6], -1)+'01', ledge=False, redge=True)
            for smonth in start_months:
                dates.append((smonth, emonth))
    else:
        pass

    conn, cur = mysql_connect()
    count = 1
    for sdate, edate in dates:  # 嵌套语句
        data.init_sql(index=['distinct device_id'], date=[edate[0], edate[1]])
        subsql = data.sql
        data.init_sql(index=["'"+str(sdate[0])+"'", "'"+str(edate[0])+"'", "'tagall'", "'000000'", 'count(distinct device_id)'],
                      date=[sdate[0], sdate[1]], device_in=subsql)
        data.query()
        data.trans_to_dict()
        if insert:
            data.cursor_execute(cur=cur)
        else:
            pprint.pprint(data.dicts)

        print 'Active device retention of', sdate[0], 'in', edate[0], date_type, 'finished...'

        if count >= 30 or (sdate, edate) == dates[-1]:
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
    if query_tag:  # 用于计算历史累计的人均收视时长，filtering应只比watch_time多allwathctime>0
        data = Data(index_name='timevalid_total_devices', date_type='history', index_type='TAG')
        data.init_sql(index=["'"+'2016091800'+"'", "'"+end_hour+"'", 'province', 'tagid', 'count(distinct device_id)'],
                      date=[end_hour], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['province', 'tagid'],
                      order_by=['province', 'tagid'])
        data.query()
        data.trans_to_dict()
        if insert:
            data.cursor_execute(cur=cur)
        else:
            pprint.pprint(data.dicts)

        data.init_sql(index=["'"+'2016091800'+"'", "'"+end_hour+"'", "'000000'", 'tagid', 'count(distinct device_id)'],
                      date=[end_hour], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['tagid'],
                      order_by=['tagid'])
        data.query()
        data.trans_to_dict()
        if insert:
            data.cursor_execute(cur=cur)
        else:
            pprint.pprint(data.dicts)

        data.init_sql(index=["'"+'2016091800'+"'", "'"+end_hour+"'", 'province', 'tagid2', 'count(distinct device_id)'],
                      date=[end_hour], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['province', 'tagid2'],
                      order_by=['province', 'tagid2'])
        data.query()
        data.trans_to_dict()
        if insert:
            data.cursor_execute(cur=cur)
        else:
            pprint.pprint(data.dicts)

        data.init_sql(index=["'"+'2016091800'+"'", "'"+end_hour+"'", "'000000'", 'tagid2', 'count(distinct device_id)'],
                      date=[end_hour], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['tagid2'],
                      order_by=['tagid2'])
        data.query()
        data.trans_to_dict()
        if insert:
            data.cursor_execute(cur=cur)
        else:
            pprint.pprint(data.dicts)

        if insert:
            conn.commit()

        data.init_sql(index=["'" + '2016091800' + "'", "'" + end_hour + "'", 'province', "'tagother'", 'count(distinct device_id)'],
                      date=[end_hour], tag='tagother', filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['province'],
                      order_by=['province'])
        data.query()
        data.trans_to_dict()
        if insert:
            data.cursor_execute(cur=cur)
        else:
            pprint.pprint(data.dicts)

        if insert:
            conn.commit()

        data.init_sql(
            index=["'" + '2016091800' + "'", "'" + end_hour + "'", "'000000'", "'tagother'", 'count(distinct device_id)'],
            date=[end_hour], tag='tagother', filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG)
        data.query()
        data.trans_to_dict()
        if insert:
            data.cursor_execute(cur=cur)
        else:
            pprint.pprint(data.dicts)

        if insert:
            conn.commit()

        data.init_sql(index=["'"+'2016091800'+"'", "'"+end_hour+"'", 'province', "'tagall'", 'count(distinct device_id)'],
                      date=[end_hour], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG, group_by=['province'],
                      order_by=['province'])
        data.query()
        data.trans_to_dict()
        if insert:
            data.cursor_execute(cur=cur)
        else:
            pprint.pprint(data.dicts)

        if insert:
            conn.commit()

        data.init_sql(index=["'"+'2016091800'+"'", "'"+end_hour+"'", "'000000'", "'tagall'", 'count(distinct device_id)'],
                      date=[end_hour], filtering=SqlWhere.TOTAL_WATCH_PRO_FLAG)
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
    data.init_sql(index=['count(distinct device_id)'], date=[end_hour], program=program_id, area=area_id,
                  filtering=SqlWhere.COMMON)
    data.query()
    return int(data.data[0][0])
