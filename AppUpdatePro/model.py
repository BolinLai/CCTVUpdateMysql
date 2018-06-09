# coding: utf-8
import time
import pprint
import phoenixdb
import phoenixdb.cursor

from method import TOTAL_TAGS, mysql_connect, mysql_close, get_date
from config import *


class SqlWhere:
    """
    过滤条件的语句
    """
    COMMON = "flag=1"
    TOTAL_WATCHTIME = "allwatchtime>0"
    PROGRAM = "content_id is not null and content_id<>'null'"
    SUBTAG = "tagid2 is not null"
    FIRST_LAST_DEVICE = "device_id is not null"

    TOTAL_WATCH_FLAG = TOTAL_WATCHTIME + " and " + COMMON
    TOTAL_WATCH_PRO_FLAG = TOTAL_WATCHTIME + " and " + PROGRAM + " and " + COMMON
    TOTAL_WATCH_PRO_SUBTAG_FLAG = TOTAL_WATCHTIME + " and " + PROGRAM + " and " + SUBTAG + " and " + COMMON
    PROGRAM_FLAG = PROGRAM + " and " + COMMON
    PROGRAM_SUBTAG_FLAG = PROGRAM + " and " + SUBTAG + " and " + COMMON
    SUBTAG_FLAG = SUBTAG + " and " + COMMON

    def __init__(self):
        pass


class Data:
    """
    做hbase查询的数据类
    """
    def __init__(self, index_name, date_type, index_type):
        """
        初始化

        :param index_name: 指标名称
        :param date_type: 查询时间类型 例：hour everyday 7days 30days month history day week
        :param index_type: 指标类型 例：PRO TAG AREA DEV
        """
        self.sql = ''
        self.data = None
        self.dicts = []
        self.index_name = index_name
        self.date_type = date_type
        self.index_type = index_type

    def init_sql(self, index, table=HbaseMysqlConfig.HBASE_TABLE, date=None, device=None, device_in=None, program=None,
                 tag=None, area=None, filtering=None, group_by=None, having=None, order_by=None, order='asc', limit=None):
        """
        生成sql语句，并将其赋予self.sql。

        :param index: 查询内容，介于select和where之间的部分，用list表示，例：['ymd', 'count(*)']
        :param table: 查询的数据表，默认使用config中的hbase表
        :param date: 时间范围，用list表示
                     若有两个时间，则表示介于这两个节点之间（含节点）例：[date1, date2]
                     若只有一个，则表示截止到这个时间（含节点）例：[date1]
        :param device: 添加用户id的条件
        :param device_in: 使用in语句查多个用户
        :param program: 添加节目id的条件
        :param tag: 添加栏目id的条件
        :param area: 添加地区的条件
        :param filtering: 过滤语句
        :param group_by: group by语句，用list表示，例：['ymd']
        :param having: having语句
        :param order_by: order by语句，用list表示
        :param order: 排序方式，默认为顺序
        :param limit: limit语句
        """
        # 生成select后要查询的指标
        index_str = ','.join(index)

        # 生成时间范围
        if self.date_type == 'hour':
            if not date:
                date_str = ''
            elif len(date) == 1:
                date_str = 'ymd<{date} and ymdh<={dateh}'.format(date=get_date(date[0][:8], 1), dateh=date[0])
            else:
                date_str = 'ymd>{start_date} and ymd<{end_date} and ymdh>={start_dateh} and ymdh<={end_dateh}'.format(start_date=get_date(date[0][:8], -1), end_date=get_date(date[1][:8], 1), start_dateh=date[0], end_dateh=date[1])
        else:
            if not date:
                date_str = ''
            elif len(date) == 1:
                date_str = 'ymd<={date}'.format(date=date[0])
            else:
                date_str = 'ymd>={start_date} and ymd<={end_date}'.format(start_date=date[0], end_date=date[1])

        # 分别生成关于device_id，program_id的语句
        device_str = "device_id='{device_id}'".format(device_id=device) if device else ''
        devicein_str = 'device_id in ({device_id})'.format(device_id=device_in) if device_in else ''
        program_str = "content_id='{content_id}'".format(content_id=program) if program else ''

        # 生成关于tag_id的语句
        if tag and tag != 'tagall' and tag != 'tagother':
            tag_str = "(tagid='{tagid}' or tagid2='{tagid}')".format(tagid=tag)
        elif tag and tag == 'tagother':
            tag_str = "(tagid is null or tagid='null')"
        else:
            tag_str = ''

        # 生成关于area_id的语句
        area_str = "province={province}".format(province=area) if area and area != '000000' else ''

        # 生成过滤条件
        filter_str = '' if not filtering else filtering

        # 生成group by和order by的语句
        group_by_str = 'group by ' + ','.join(group_by) if group_by else ''
        having_str = 'having ' + having if having else ''
        order_by_str = 'order by ' + ','.join(order_by) + ' ' + order if order_by else ''

        # 生成limit语句
        limit_str = '' if not limit else "limit " + str(limit)

        # 生成最终语句
        self.sql = "select " + index_str + " from {table}".format(table=table)
        condition = [string for string in [date_str, device_str, devicein_str, program_str, tag_str, area_str, filter_str] if string]
        auxi_str = [string for string in [group_by_str, having_str, order_by_str, limit_str]]
        if condition:
            self.sql += " where " + ' and '.join(condition)
        if auxi_str:
            self.sql += ' ' + ' '.join(auxi_str)

    def query(self):
        """
        查询函数，查询sql并将结果赋予self.data。
        """
        conn = phoenixdb.connect(HbaseMysqlConfig.HBASE_IP, autocommit=True)
        cursor = conn.cursor()

        cursor.execute(self.sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()

        self.data = result

    def org_index(self):
        """
        对某些指标进行reorganize，使self.data的list格式可以进行字典转换，转换结果赋予self.data
        """
        if self.index_name in ['watchtime_dist', 'usetime_dist']:
            data_dict = {}
            for item in self.data:
                data_dict.setdefault((item[0], item[1], item[2], item[3]), []).append([int(item[4]), int(item[5])])
            self.data = [[item[0], item[1], item[2], item[3], str(data_dict[item])] for item in data_dict.keys()]
        else:
            pass

    def trans_to_dict(self):
        """
        将查询到的list转换为dict，并赋予self.dict。
        """
        if self.index_name in ['play_times', 'clicks', 'active_devices', 'total_devices', 'watch_time', 'use_time', 'launch_times', 'timevalid_active_devices']:
            if self.date_type == 'everyday' or self.date_type == 'hour':
                if self.index_type == 'PRO':
                    self.dicts = [{'date': item[0], 'type_id': item[2], 'area_id': item[1], 'value': int(item[3])} for item in self.data]
                elif self.index_type == 'TAG' or self.index_type == 'AREA' or self.index_type == 'DEV':
                    self.dicts = [{'date': item[0], 'type_id': item[2], 'area_id': item[1], 'app_key': item[3], 'app_version_code': item[4], 'channel': item[5], 'value': int(item[6])} for item in self.data if item[2] in TOTAL_TAGS or item[2] == 'tagall' or item[2] == 'tagother']
                else:
                    pass

            elif self.date_type == '7days' or self.date_type == '30days' or self.date_type == 'month':
                if self.index_type == 'PRO':
                    self.dicts = [{'start_date': item[0], 'end_date': item[1], 'date_type': self.date_type, 'type_id': item[3], 'area_id': item[2], 'value': int(item[4])} for item in self.data]
                elif self.index_type == 'TAG' or self.index_type == 'AREA' or self.index_type == 'DEV':
                    self.dicts = [{'start_date': item[0], 'end_date': item[1], 'date_type': self.date_type, 'type_id': item[3], 'area_id': item[2], 'app_key': item[4], 'app_version_code': item[5], 'channel': item[6], 'value': int(item[7])} for item in self.data if item[3] in TOTAL_TAGS or item[3] == 'tagall' or item[3] == 'tagother']
                else:
                    pass

        elif self.index_name == 'timevalid_total_devices':
            if self.date_type == 'history':
                if self.index_type == 'TAG':
                    self.dicts = [{'start_date': item[0], 'end_date': item[1], 'date_type': self.date_type, 'type_id': item[3], 'area_id': item[2], 'value': int(item[4])} for item in self.data if item[3] in TOTAL_TAGS or item[3] == 'tagall' or item[3] == 'tagother']

        elif self.index_name in ['watchtime_dist', 'usetime_dist', 'new_retention', 'active_retention']:
            self.dicts = [{'start_date': item[0], 'end_date': item[1], 'date_type': self.date_type, 'type_id': item[2], 'area_id': item[3], 'value': item[4]} for item in self.data if item[2] in TOTAL_TAGS or item[2] == 'tagall' or item[2] == 'tagother']

        for item in self.dicts:  # app的province除了正确的id外只有None这一种情况，如果还有其它的则这种处理方法不能将二者相加起来
            try:
                int(item['area_id'])
            except:
                item['area_id'] = -1

    def cursor_execute(self, cur):
        """
        插入数据表

        :param cur: 插入数据表对应的cursor
        """
        if self.index_name in ['play_times', 'clicks', 'active_devices', 'total_devices', 'watch_time', 'use_time', 'launch_times', 'timevalid_active_devices']:
            if self.date_type == 'hour' and self.index_type == 'PRO':
                table = MysqlTable.BASIC_INDEX_HOUR_PRO
            elif self.date_type == 'hour' and (self.index_type == 'TAG' or self.index_type == 'AREA' or self.index_type == 'DEV'):
                table = MysqlTable.BASIC_INDEX_HOUR
            elif self.date_type == 'everyday' and self.index_type == 'PRO':
                table = MysqlTable.BASIC_INDEX_EVERYDAY_PRO
            elif self.date_type == 'everyday' and (self.index_type == 'TAG' or self.index_type == 'AREA' or self.index_type == 'DEV'):
                table = MysqlTable.BASIC_INDEX_EVERYDAY
            elif self.date_type in ['7days', '30days', 'month'] and self.index_type == 'PRO':
                table = MysqlTable.BASIC_INDEX_DAYS_PRO
            elif self.date_type in ['7days', '30days', 'month'] and (self.index_type == 'TAG' or self.index_type == 'AREA' or self.index_type == 'DEV'):
                table = MysqlTable.BASIC_INDEX_DAYS
            else:
                table = ''

            if self.date_type == 'hour' or self.date_type == 'everyday':
                if self.index_type == 'PRO':
                    mysql_str = 'insert into {table}(name, date, type_id, area_id, value) values(%s, %s, %s, %s, %s) on duplicate key update value=values(value)'.format(table=table)
                    for item in self.dicts:
                        if item['value'] != 0 and item['value'] != '0':
                            cur.execute(mysql_str, [self.index_name, item['date'], item['type_id'], item['area_id'], item['value']])
                else:
                    mysql_str = 'insert into {table}(name, date, type_id, area_id, app_key, app_version_code, app_channel, value) values(%s, %s, %s, %s, %s, %s, %s, %s) on duplicate key update value=values(value)'.format(table=table)
                    for item in self.dicts:
                        if item['value'] != 0 and item['value'] != '0':
                            cur.execute(mysql_str, [self.index_name, item['date'], item['type_id'], item['area_id'], item['app_key'], item['app_version_code'], item['channel'], item['value']])
            elif self.date_type == '7days' or self.date_type == '30days' or self.date_type == 'month':
                if self.index_type == "PRO":
                    mysql_str = "insert into {table}(name, start_date, end_date, date_type, type_id, area_id, value) values(%s, %s, %s, %s, %s, %s, %s) on duplicate key update value=values(value)".format(table=table)
                    for item in self.dicts:
                        if item['value'] != 0 and item['value'] != '0':
                            cur.execute(mysql_str, [self.index_name, item['start_date'], item['end_date'], self.date_type, item['type_id'], item['area_id'], item['value']])
                else:
                    mysql_str = "insert into {table}(name, start_date, end_date, date_type, type_id, area_id, app_key, app_version_code, app_channel, value) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) on duplicate key update value=values(value)".format(table=table)
                    for item in self.dicts:
                        if item['value'] != 0 and item['value'] != '0':
                            cur.execute(mysql_str, [self.index_name, item['start_date'], item['end_date'], self.date_type, item['type_id'], item['area_id'], item['app_key'], item['app_version_code'], item['channel'], item['value']])

        elif self.index_name in ['watchtime_dist', 'usetime_dist', 'new_retention', 'active_retention']:
            table = MysqlTable.SPECIFIC_INDEX if not self.date_type == 'hour' else MysqlTable.SPECIFIC_INDEX_HOUR
            mysql_str = 'insert into {table}(name, start_date, end_date, date_type, type_id, area_id, value) values(%s, %s, %s, %s, %s, %s, %s) on duplicate key update value=values(value)'.format(table=table)
            for item in self.dicts:
                if item['value'] != '[]' and item['value'] != 0 and item['value'] != '0':
                    cur.execute(mysql_str, [self.index_name, item['start_date'], item['end_date'], self.date_type, item['type_id'], item['area_id'], item['value']])

        elif self.index_name == 'timevalid_total_devices':
            table = MysqlTable.SPECIFIC_INDEX_HOUR
            conn_tmp, cur_tmp = mysql_connect()
            for item in self.dicts:
                if item['value'] != 0 and item['value'] != '0':
                    mysql_str = "select * from {table} where name='{name}' and type_id='{type_id}' and area_id='{area_id}'".format(table=table, name=self.index_name, type_id=item['type_id'], area_id=item['area_id'])
                    cur_tmp.execute(mysql_str)
                    insert = False if cur_tmp.fetchall() else True
                    if insert:
                        mysql_str = "insert into {table}(name, start_date, end_date, date_type, type_id, area_id, value) values(%s, %s, %s, %s, %s, %s, %s) on duplicate key update value=values(value)".format(table=table)
                        cur.execute(mysql_str, [self.index_name, item['start_date'], int(item['end_date']), self.date_type, item['type_id'], item['area_id'], item['value']])
                    else:
                        mysql_str = "update {table} set end_date=(%s), value=(%s) where name='{name}' and type_id='{type_id}' and area_id='{area_id}'".format(table=table, name=self.index_name, type_id=item['type_id'], area_id=item['area_id'])
                        cur.execute(mysql_str, [item['end_date'], item['value']])
            mysql_close(conn_tmp, cur_tmp)
