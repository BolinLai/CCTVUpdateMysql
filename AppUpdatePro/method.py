# coding: utf-8
import time
import datetime
import MySQLdb
import phoenixdb
import phoenixdb.cursor

from config import *


def mysql_connect(host=HbaseMysqlConfig.MYSQL_HOST,
                  port=HbaseMysqlConfig.MYSQL_PORT,
                  user=HbaseMysqlConfig.MYSQL_USER,
                  passwd=HbaseMysqlConfig.MYSQL_PASSWD,
                  charset=HbaseMysqlConfig.MYSQL_CHARSET):
    """
    连接数据库

    :param host: mysql 地址
    :param port: mysql 端口
    :param user: mysql 用户名
    :param passwd: mysql 密码
    :param charset: mysql 编码方式
    :return: 连接mysql的connection和cursor
    """
    conn = MySQLdb.connect(host=host,
                           port=port,
                           user=user,
                           passwd=passwd,
                           charset=charset)
    cur = conn.cursor()
    return conn, cur


def mysql_close(conn, cur):
    """
    关闭数据库

    :param conn: MySQLdb的connect
    :param cur: connect对应的cursor
    """
    cur.close()
    conn.close()


def insert_update_time(update_dicts):
    """
    插入或更新各个表的更新日期。

    :param update_dicts: 各个表的更新日期。 格式：{table1:{date_type1: date1, date_type2: date2, ……}, table2: ……}
    """
    conn, cur = mysql_connect()
    for table in update_dicts.keys():
        for dtype in update_dicts[table].keys():
            mysql_str = "insert into {table}(table_name, date_type, update_date) values(%s, %s, %s) on duplicate key update update_date=values(update_date)".format(table=MysqlTable.TABLE_INFO)
            cur.execute(mysql_str, [table, dtype, update_dicts[table][dtype]])
    conn.commit()
    mysql_close(conn, cur)


def file_output(path, date_type):
    """
    将更新日期输出到文件。

    :param path: 输出路径
    :param date_type: 需要输出的更新日期的类型。（hour或其它）
    """
    conn, cur = mysql_connect()
    mysql_str = "select table_name, date_type, update_date from {table} where table_name not like '%specific%'".format(table=MysqlTable.TABLE_INFO)
    cur.execute(mysql_str)
    update_dates = cur.fetchall()
    mysql_close(conn, cur)

    if date_type == 'hour':
        date = min([int(item[2]) for item in update_dates if item[1] == 'hour'])
    else:
        date = min([int(item[2]) for item in update_dates if item[1] != 'hour'])

    with open(path, 'w') as f:
        f.write(str(date))
    f.close()


def get_update_ymd(table, date_type, x=0):
    """
    获取日期。

    :param table: 数据库.数据表名
    :param date_type: 日期类型，例如：everyday，hour
    :param x: 延迟的天数
    :return: 在数据库最后一次更新时间的基础上，提前days_ago天的日期。若数据库无更新时间，则返回None。
    """
    conn, cur = mysql_connect()
    mysql_str = "select update_date from {table} where table_name='{table_name}' and date_type='{date_type}'".format(table=MysqlTable.TABLE_INFO, table_name=table, date_type=date_type)
    cur.execute(mysql_str)
    update_ymd = cur.fetchone()
    mysql_close(conn, cur)

    ymd = str(get_date(str(update_ymd[0]), x)) if update_ymd[0] else None
    return ymd


def get_update_ymdh(table, date_type, x=0):
    """
    获取时间。

    :param table: 数据库.数据表名
    :param x: 延迟的小时数
    :param date_type: 日期类型，例如：everyday，hour
    :return: 在数据库最后一次更新时间的基础上，提前hours_ago小时的时间。若数据库无更新时间，则返回None。
    """
    conn, cur = mysql_connect()
    mysql_str = "select update_date from {table} where table_name='{table_name}' and date_type='{date_type}'".format(table=MysqlTable.TABLE_INFO, table_name=table, date_type=date_type)
    cur.execute(mysql_str)
    update_ymdh = cur.fetchone()
    mysql_close(conn, cur)

    ymdh = str(get_date(str(update_ymdh[0]), x)) if update_ymdh[0] else None
    return ymdh


def get_tag():
    """
    获取所有tag。

    :return: 所有tag及tag之间的父子关系。
    """
    conn, cur = mysql_connect()

    mysql_str = "select tag_id, parenttag from {table};".format(table=MysqlTable.TAG)
    cur.execute(mysql_str)
    data = cur.fetchall()

    count = 0
    tag_list = {}
    for tag, parenttag in data:
        if parenttag == 'Tag':
            count += 1
            tag_list.setdefault(tag, [])
        else:
            tag_list.setdefault(parenttag, []).append(tag)
    mysql_close(conn, cur)

    return tag_list


TAGS = get_tag()
FIRST_TAGS = TAGS.keys()


def get_totaltag():
    """
    获得所有的tag

    :return: 包括所有一二级tag的list
    """
    total_tags = FIRST_TAGS[:]
    for item in TAGS.values():
        total_tags.extend(item)
    return total_tags


TOTAL_TAGS = get_totaltag()


def get_subtag(tag_id):
    """
    获得子tag

    :param tag_id: 一级tag
    :return: 输入tag_id下的子tag
    """
    if tag_id == 'tag':
        results = FIRST_TAGS
    elif tag_id in TAGS and TAGS[tag_id]:
        results = TAGS[tag_id]
    else:
        results = []
    return results


def get_node():
    """
    获取所有节点。

    :return: 所有节点及对应名称。
    """
    conn, cur = mysql_connect()

    mysql_str = "select * from {table};".format(table=MysqlTable.NODE)
    cur.execute(mysql_str)
    data = cur.fetchall()
    node = {}
    for item in data:
        node[str(item[0])] = item[1]

    mysql_close(conn, cur)
    return node


NODES = get_node()


def get_days(start_ymd, end_ymd):
    """
    获得中间日期。

    :param start_ymd: 起始时间
    :param end_ymd: 截止时间
    :return: 起始时间与截止时间之间所有的日期（ymd）。（含起始与截止时间）
    """
    start_timestamp = time.mktime(time.strptime(str(start_ymd), '%Y%m%d'))
    end_timestamp = time.mktime(time.strptime(str(end_ymd), '%Y%m%d'))

    days = []
    mid_timestamp = start_timestamp
    while mid_timestamp <= end_timestamp:
        mid_ymd = time.strftime('%Y%m%d', time.localtime(mid_timestamp))
        days.append(mid_ymd)
        mid_timestamp += 3600 * 24
    return days


def get_hours(start_ymdh, end_ymdh):
    """
    获得中间小时。

    :param start_ymdh: 起始时间
    :param end_ymdh: 截止时间
    :return: 起始时间与截止时间之间所有的小时（ymdh）。（含起始与截止时间）
    """
    start_timestamp = time.mktime(time.strptime(str(start_ymdh), '%Y%m%d%H'))
    end_timestamp = time.mktime(time.strptime(str(end_ymdh), '%Y%m%d%H'))

    hours = []
    mid_timestamp = start_timestamp
    while mid_timestamp <= end_timestamp:
        mid_ymd = time.strftime('%Y%m%d%H', time.localtime(mid_timestamp))
        hours.append(mid_ymd)
        mid_timestamp += 3600
    return hours


def get_weeks(start_ymd, end_ymd, ledge=False, redge=False):
    """
    获得中间的周。

    :param start_ymd: 起始日期
    :param end_ymd: 截止日期
    :param ledge: 是否包含起始日期所在周
    :param redge: 是否包含截止日期所在周
    :return: 起始时间和截止时间之间所有的周。例：[[MON_1, SUN_1], [MON_2, SUN_2], ……]
    """
    start_ymd, end_ymd = str(start_ymd), str(end_ymd)
    weeks = []
    s_ymd, e_ymd = datetime.datetime.strptime(start_ymd, '%Y%m%d'), datetime.datetime.strptime(end_ymd, '%Y%m%d')
    if ledge:
        s_ymd = s_ymd - datetime.timedelta(days=s_ymd.weekday()) if s_ymd.weekday() != 0 else s_ymd
    else:
        s_ymd = s_ymd + datetime.timedelta(days=7 - s_ymd.weekday()) if s_ymd.weekday() != 0 else s_ymd

    if redge:
        e_ymd = e_ymd + datetime.timedelta(days=7 - e_ymd.weekday()) if e_ymd.weekday() != 6 else e_ymd + datetime.timedelta(days=1)
    else:
        e_ymd = e_ymd - datetime.timedelta(days=e_ymd.weekday()) if e_ymd.weekday() != 6 else e_ymd + datetime.timedelta(days=1)

    while s_ymd < e_ymd:
        weeks.append([s_ymd.strftime('%Y%m%d'), (s_ymd + datetime.timedelta(days=6)).strftime('%Y%m%d')])
        s_ymd += datetime.timedelta(days=7)
    return weeks


def get_months(start_ymd, end_ymd, ledge=False, redge=False):
    """
    获得中间的月。

    :param start_ymd: 起始时间
    :param end_ymd: 截止时间
    :param ledge: 是否包含起始时间所在的月
    :param redge: 是否包含截止时间所在的月
    :return: 起始时间和截止时间之间所有的月。例：[['20170101', '20170131'], ['20170201', '20170228'], ……]
    """
    start_ymd, end_ymd = str(start_ymd), str(end_ymd)
    s_months = []
    start_date = start_ymd[0:6] + '01'
    s_months.append(start_date)
    while int(start_date) <= int(end_ymd):
        date = datetime.datetime.strptime(start_date, '%Y%m%d')
        month = date.month + 1
        year = date.year
        if month > 12:
            year += 1
            month = 1
        start_date = datetime.datetime.strftime(datetime.datetime(year=year, month=month, day=date.day, hour=date.hour), '%Y%m%d')
        s_months.append(start_date)
    months = [[s_months[i], get_date(s_months[i+1], -1)] for i in xrange(len(s_months[:-1]))]
    if not ledge:
        if months and months[0][0] != start_ymd:
            months = months[1:]
    if not redge:
        if months and months[-1][-1] != end_ymd:
            months = months[:-1]
    return months


def split_dates(start_date, end_date, step, date_type):
    """
    从起始时间到截止时间按step来划分。

    :param start_date: 起始时间
    :param end_date: 截止时间
    :param step: 步长
    :param date_type: 日期类型（小时和非小时）
    :return: 按照步长划分的时间段的起始日期和终止日期。例：([start_time_1, start_time_2, ……], [end_time_1, end_time_2, ……])
    """
    dates = get_days(str(start_date), str(end_date)) if date_type != 'hour' else get_hours(str(start_date), str(end_date))
    startdates, enddates = dates[::step], dates[step-1:][::step]
    if dates[-1] not in enddates:
        enddates.append(dates[-1])
    return startdates, enddates


def get_date(date, x):
    """
    获得date之后x小时/天/月的日期。

    :param date: 指定日期。例：小时 2017010112 天：20170101 月：201701
    :param x: 延后小时/天/月数
    :return: 延后的日期。
    """
    date = str(date)
    if len(date) == 6:
        new_date = datetime.datetime.strptime(date, '%Y%m')
        month = new_date.month + x
        year = new_date.year
        if month > 12 or month < 1:
            year += month / 12
            month = month % 12
        if month == 0:
            year -= 1
            month = 12
        new_date = datetime.datetime.strftime(datetime.datetime(year=year, month=month, day=new_date.day, hour=new_date.hour), '%Y%m')

    elif len(date) == 8:
        new_date = (datetime.datetime.strptime(date, '%Y%m%d') + datetime.timedelta(days=x)).strftime('%Y%m%d')
    elif len(date) == 10:
        new_date = (datetime.datetime.strptime(date, '%Y%m%d%H') + datetime.timedelta(hours=x)).strftime('%Y%m%d%H')
    else:
        new_date = ''

    return new_date


def isdataready(date):
    """
    判断某个时间的数据是否全部到达。

    :param date: 判断的日期。（可为小时或天）
    :return: 数据是否已经到达
    """
    date = str(date) + '23' if len(str(date)) == 8 else str(date)
    conn = phoenixdb.connect(HbaseMysqlConfig.HBASE_IP, autocommit=True)
    cursor = conn.cursor()

    sql = "select count(*) from {table} where ymdh={ymdh}".format(table=HbaseMysqlConfig.HBASE_TABLE, ymdh=date)
    cursor.execute(sql)
    result = cursor.fetchall()

    cursor.close()
    conn.close()

    if int(result[0][0]) > 1000:
        return True
    else:
        return False


def add_partition(table, date):
    """
    为数据表添加partition。

    :param table: 数据表
    :param date: 添加日期
    """
    conn, cur = mysql_connect()
    mysql_str = "use {db}".format(db=HbaseMysqlConfig.MYSQL_DB)
    cur.execute(mysql_str)
    mysql_str = "select max(partition_description) from information_schema.partitions where table_schema=schema() and table_name='{table}'".format(table=table.split('.')[1])
    cur.execute(mysql_str)
    maxpartition = cur.fetchone()[0]

    if int(date) < int(maxpartition):
        pass
    else:
        months = get_months(maxpartition[0:8], str(date)[0:8], ledge=True, redge=True)
        s_months = [item[0] for item in months]
        s_months.append(get_date(months[-1][-1], 1))
        for s1, s2 in zip(s_months[:-1], s_months[1:]):
            if table == MysqlTable.BASIC_INDEX_HOUR or table == MysqlTable.BASIC_INDEX_HOUR_PRO or table == MysqlTable.SPECIFIC_INDEX_HOUR:
                mysql_str = "alter table {table} add partition (partition p{partiton_name} values less than ({partition_date}))".format(table=table, partiton_name=s1[:6], partition_date=s2+'00')
            else:
                mysql_str = "alter table {table} add partition (partition p{partiton_name} values less than ({partition_date}))".format(table=table, partiton_name=s1[:6], partition_date=s2)
            cur.execute(mysql_str)
        conn.commit()
    mysql_close(conn, cur)
