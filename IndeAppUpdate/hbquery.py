# coding: utf-8
import pprint

from config import CCTV5_START_YMD, FINANCE_START_YMD, CHILDREN_START_YMD, MUSIC_START_YMD
from method import *
from model import *


# Basic Every-Date Index
def play_times(source, start_date, end_date, date_type, query_pro=True, query_tag=True, insert=False):
    """
    收视次数查询

    :param source: 收据来源
    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：hour everyday
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    if date_type == 'everyday':
        start_dates, end_dates = split_dates(start_date, end_date, step=30, date_type=date_type)
    else:
        start_dates, end_dates = [start_date], [end_date]
    date_index = 'ymd' if date_type != 'hour' else 'ymdh'
    conn, cur = mysql_connect()
    count = 1
    for sdate, edate in zip(start_dates, end_dates):
        if query_pro:
            data = Data(source=source, index_name='play_times', date_type=date_type, index_type='PRO')
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
            data = Data(source=source, index_name='play_times', date_type=date_type, index_type='TAG')
            # data.init_sql(index=[date_index, 'province', 'tagid', 'count(*)'], date=[sdate, edate],
            #               filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'province', 'tagid'],
            #               order_by=[date_index, 'province', 'tagid'])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)
            #
            # data.init_sql(index=[date_index, 'province', 'tagid2', 'count(*)'], date=[sdate, edate],
            #               filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'province', 'tagid2'],
            #               order_by=[date_index, 'province', 'tagid2'])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', "'tagall'", 'count(*)'], date=[sdate, edate],
                          tag='tagother', filtering=SqlWhere.PROGRAM_FLAG, group_by=[date_index, 'province'],
                          order_by=[date_index, 'province'])  # 独立app的tag全为null，故直接统计tagall
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if sdate != edate:
            print source.capitalize(), 'Play times', sdate+'-'+edate, 'finished...'
        else:
            print source.capitalize(), 'Play times', sdate, 'finished...'

        if count >= 50 or edate == end_dates[-1]:  # 查询一段时间commit一次
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


def clicks(source, start_date, end_date, date_type, query_pro=True, query_tag=True, insert=False):
    """
    点击量查询

    :param source: 收据来源
    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：hour everyday
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    if date_type == 'everyday':
        start_dates, end_dates = split_dates(start_date, end_date, step=30, date_type=date_type)
    else:
        start_dates, end_dates = [start_date], [end_date]
    date_index = 'ymd' if date_type != 'hour' else 'ymdh'
    conn, cur = mysql_connect()
    count = 1
    for sdate, edate in zip(start_dates, end_dates):
        if query_pro:
            data = Data(source=source, index_name='clicks', date_type=date_type, index_type='PRO')
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
            data = Data(source=source, index_name='clicks', date_type=date_type, index_type='TAG')
            # data.init_sql(index=[date_index, 'province', 'tagid', 'count(*)'], date=[sdate, edate],
            #               filtering=SqlWhere.COMMON, group_by=[date_index, 'province', 'tagid'],
            #               order_by=[date_index, 'province', 'tagid'])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)
            #
            # data.init_sql(index=[date_index, 'province', 'tagid2', 'count(*)'], date=[sdate, edate],
            #               filtering=SqlWhere.COMMON, group_by=[date_index, 'province', 'tagid2'],
            #               order_by=[date_index, 'province', 'tagid2'])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', "'tagall'", 'count(*)'], date=[sdate, edate],
                          tag='tagother', filtering=SqlWhere.COMMON, group_by=[date_index, 'province'],
                          order_by=[date_index, 'province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if sdate != edate:
            print source.capitalize(), 'Clicks', sdate+'-'+edate, 'finished...'
        else:
            print source.capitalize(), 'Clicks', sdate, 'finished...'

        if count >= 50 or edate == end_dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


def active_devices(source, start_date, end_date, date_type, query_pro=True, query_tag=True, insert=False):
    """
    活跃用户数查询

    :param source: 收据来源
    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：hour everyday
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    if date_type == 'everyday':
        start_dates, end_dates = split_dates(start_date, end_date, step=30, date_type=date_type)
    else:
        start_dates, end_dates = [start_date], [end_date]
    date_index = 'ymd' if date_type != 'hour' else 'ymdh'
    conn, cur = mysql_connect()
    count = 1
    for sdate, edate in zip(start_dates, end_dates):
        if query_pro:
            data = Data(source, index_name='active_devices', date_type=date_type, index_type='PRO')
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
            data = Data(source=source, index_name='active_devices', date_type=date_type, index_type='TAG')
            # data.init_sql(index=[date_index, 'province', 'tagid', 'count(distinct device_id)'], date=[sdate, edate],
            #               filtering=SqlWhere.COMMON, group_by=[date_index, 'province', 'tagid'],
            #               order_by=[date_index, 'province', 'tagid'])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)
            #
            # data.init_sql(index=[date_index, "'000000'", 'tagid', 'count(distinct device_id)'], date=[sdate, edate],
            #               filtering=SqlWhere.COMMON, group_by=[date_index, 'tagid'], order_by=[date_index, 'tagid'])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)
            #
            # data.init_sql(index=[date_index, 'province', 'tagid2', 'count(distinct device_id)'], date=[sdate, edate],
            #               filtering=SqlWhere.COMMON, group_by=[date_index, 'province', 'tagid2'],
            #               order_by=[date_index, 'province', 'tagid2'])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)
            #
            # data.init_sql(index=[date_index, "'000000'", 'tagid2', 'count(distinct device_id)'], date=[sdate, edate],
            #               filtering=SqlWhere.COMMON, group_by=[date_index, 'tagid2'], order_by=[date_index, 'tagid2'])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)
            #
            # data.init_sql(index=[date_index, 'province', "'tagother'", 'count(distinct device_id)'], date=[sdate, edate],
            #               tag='tagother', filtering=SqlWhere.COMMON, group_by=[date_index, 'province'],
            #               order_by=[date_index, 'province'])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)
            #
            # data.init_sql(index=[date_index, "'000000'", "'tagother'", 'count(distinct device_id)'], date=[sdate, edate],
            #               tag='tagother', filtering=SqlWhere.COMMON, group_by=[date_index], order_by=[date_index])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, 'province', "'tagall'", 'count(distinct device_id)'], date=[sdate, edate],
                          filtering=SqlWhere.COMMON, group_by=[date_index, 'province'], order_by=[date_index, 'province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=[date_index, "'000000'", "'tagall'", 'count(distinct device_id)'], date=[sdate, edate],
                          filtering=SqlWhere.COMMON, group_by=[date_index], order_by=[date_index])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if sdate != edate:
            print source.capitalize(), 'Active devices', sdate+'-'+edate, 'finished...'
        else:
            print source.capitalize(), 'Active devices', sdate, 'finished...'

        if count >= 40 or edate == end_dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)


def total_devices(source, start_date, end_date, date_type, query_pro=True, query_tag=True, query_area=True, insert=False):
    """
    累积用户数查询

    :param source: 收据来源
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
    for date in dates:  # filtering应与active_devices相同
        if query_pro:
            data = Data(source=source, index_name='total_devices', date_type=date_type, index_type='PRO')
            data.init_sql(index=["'"+date+"'", "'000000'", 'content_id', 'count(distinct device_id)'], date=[date],
                          filtering=SqlWhere.PROGRAM_FLAG, group_by=['content_id'], order_by=['content_id'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_tag:
            data = Data(source=source, index_name='total_devices', date_type=date_type, index_type='TAG')
            # data.init_sql(index=["'"+date+"'", "'000000'", 'tagid', 'count(distinct device_id)'], date=[date],
            #               filtering=SqlWhere.COMMON, group_by=['tagid'], order_by=['tagid'])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)
            #
            # data.init_sql(index=["'" + date + "'", "'000000'", 'tagid2', 'count(distinct device_id)'], date=[date],
            #               filtering=SqlWhere.COMMON, group_by=['tagid2'], order_by=['tagid2'])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)
            #
            # data.init_sql(index=["'" + date + "'", "'000000'", "'tagother'", 'count(distinct device_id)'], date=[date],
            #               tag='tagother', filtering=SqlWhere.COMMON)
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)

            data.init_sql(index=["'" + date + "'", "'000000'", "'tagall'", 'count(distinct device_id)'], date=[date],
                          filtering=SqlWhere.COMMON)
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        if query_area:
            data = Data(source=source, index_name='total_devices', date_type=date_type, index_type='AREA')
            data.init_sql(index=["'"+date+"'", 'province', "'tagall'", 'count(distinct device_id)'], date=[date],
                          filtering=SqlWhere.COMMON, group_by=['province'], order_by=['province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        print source.capitalize(), 'Total devices', date, 'finished...'

        if count >= 10 or date == dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1

        if source == 'cctv5':
            time.sleep(40)
    mysql_close(conn, cur)


# Basic Several-Day Index
def active_devices_days(source, start_date, end_date, date_type, query_pro=True, query_tag=True, insert=False):
    """
    活跃用户数多天查询

    :param source: 收据来源
    :param start_date: 起始时间
    :param end_date: 截止时间
    :param date_type: 时间类型 例：7days 30days month
    :param query_pro: 是否查询节目
    :param query_tag: 是否查询频道
    :param insert: 是否插入数据库，False则将结果打印出来
    """
    if date_type == 'week':
        weeks = get_weeks(start_date, end_date, ledge=True, redge=False)
        start_dates, end_dates = [item[0] for item in weeks], [item[1] for item in weeks]
    else:
        start_dates, end_dates = [start_date], [end_date]

    conn, cur = mysql_connect()
    count = 1
    for sdate, edate in zip(start_dates, end_dates):  # filtering应与active_devices相同
        if query_pro:
            data = Data(source=source, index_name='active_devices', date_type=date_type, index_type='PRO')
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
            data = Data(source=source, index_name='active_devices', date_type=date_type, index_type='TAG')
            # data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', 'tagid', 'count(distinct device_id)'],
            #               date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['province', 'tagid'],
            #               order_by=['province', 'tagid'])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)
            #
            # data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", 'tagid', 'count(distinct device_id)'],
            #               date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['tagid'], order_by=['tagid'])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)
            #
            # data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', 'tagid2', 'count(distinct device_id)'],
            #               date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['province', 'tagid2'],
            #               order_by=['province', 'tagid2'])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)
            #
            # data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", 'tagid2', 'count(distinct device_id)'],
            #               date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['tagid2'], order_by=['tagid2'])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)
            #
            # data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", 'province', "'tagother'", 'count(distinct device_id)'],
            #               date=[sdate, edate], tag='tagother', filtering=SqlWhere.COMMON, group_by=['province'], order_by=['province'])
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)
            #
            # data.init_sql(index=["'" + sdate + "'", "'" + edate + "'", "'000000'", "'tagother'", 'count(distinct device_id)'],
            #               date=[sdate, edate], tag='tagother', filtering=SqlWhere.COMMON)
            # data.query()
            # data.trans_to_dict()
            # if insert:
            #     data.cursor_execute(cur=cur)
            # else:
            #     pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", 'province', "'tagall'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON, group_by=['province'], order_by=['province'])
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

            data.init_sql(index=["'"+sdate+"'", "'"+edate+"'", "'000000'", "'tagall'", 'count(distinct device_id)'],
                          date=[sdate, edate], filtering=SqlWhere.COMMON)
            data.query()
            data.trans_to_dict()
            if insert:
                data.cursor_execute(cur=cur)
            else:
                pprint.pprint(data.dicts)

        print source.capitalize(), 'Active devices '+date_type, sdate + '-' + edate, 'finished...'

        if count >= 40 or edate == end_dates[-1]:
            if insert:
                conn.commit()
            count = 1
        else:
            count += 1
    mysql_close(conn, cur)
