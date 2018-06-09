# coding: utf-8
class HbaseMysqlConfig:
    """
    Config of Hbase and Mysql
    """
    HBASE_IP = "http://202.120.39.239:8760/"  # http://172.16.8.150:8760/  http://202.120.39.239:8760/  http://10.110.142.33:8765
    CCTV5_HBASE_TABLE = "appdata.cctv52"
    FINANCE_HBASE_TABLE = "appdata.finance2"
    CHILDREN_HBASE_TABLE = "appdata.children2"
    MUSIC_HBASE_TABLE = "appdata.music2"

    MYSQL_HOST = "202.120.39.153"  # "202.120.39.153"/"10.110.142.3"
    MYSQL_PORT = 3901  # 3901/3306
    MYSQL_USER = "root"
    MYSQL_PASSWD = "Rootdata_200"
    MYSQL_CHARSET = "utf8"
    MYSQL_DB = "appdata"

    def __init__(self):
        pass


class MysqlTable:
    """
    Mysql
    """
    TABLE_INFO = HbaseMysqlConfig.MYSQL_DB + ".hbase_inde_table_info"
    BASIC_INDEX_EVERYDAY_PRO = HbaseMysqlConfig.MYSQL_DB + ".hbase_inde_basicindex_everydaypro"
    BASIC_INDEX_EVERYDAY = HbaseMysqlConfig.MYSQL_DB + ".hbase_inde_basicindex_everyday"
    BASIC_INDEX_DAYS = HbaseMysqlConfig.MYSQL_DB + ".hbase_inde_basicindex_days"

    def __init__(self):
        pass


CCTV5_START_YMD = '20160901'
FINANCE_START_YMD = '20170802'
CHILDREN_START_YMD = '20160901'
MUSIC_START_YMD = '20170802'

START_UPDATE_DICTS = {
    MysqlTable.BASIC_INDEX_EVERYDAY_PRO: {'CCTV5': CCTV5_START_YMD,
                                          'FINANCE': FINANCE_START_YMD,
                                          'CHILDREN': CHILDREN_START_YMD,
                                          'MUSIC': MUSIC_START_YMD},

    MysqlTable.BASIC_INDEX_EVERYDAY: {'CCTV5': CCTV5_START_YMD,
                                      'FINANCE': FINANCE_START_YMD,
                                      'CHILDREN': CHILDREN_START_YMD,
                                      'MUSIC': MUSIC_START_YMD},

    MysqlTable.BASIC_INDEX_DAYS: {'CCTV5': CCTV5_START_YMD,
                                  'FINANCE': FINANCE_START_YMD,
                                  'CHILDREN': CHILDREN_START_YMD,
                                  'MUSIC': MUSIC_START_YMD}
}

"""
创建数据表 APP
CREATE TABLE `hbase_inde_basicindex_everydaypro` (
  `id` bigint(50) NOT NULL AUTO_INCREMENT,
  `name` char(40) NOT NULL,
  `source` char(15) NOT NULL,
  `date` int(8) NOT NULL,
  `type_id` char(200) NOT NULL,
  `area_id` char(30) NOT NULL,
  `value` int(20) NOT NULL,
  PRIMARY KEY (`id`, `date`),
  UNIQUE KEY `DSNTA` (`date`,`source`,`name`,`type_id`,`area_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC
PARTITION BY RANGE COLUMNS(`date`) 
(
    PARTITION p201608 VALUES LESS THAN (20160901)
);

CREATE TABLE `hbase_inde_basicindex_everyday` (
  `id` int(15) NOT NULL AUTO_INCREMENT,
  `name` char(40) NOT NULL,
  `source` char(15) NOT NULL,
  `date` int(8) NOT NULL,
  `type_id` char(200) NOT NULL,
  `area_id` char(30) NOT NULL,
  `value` int(20) NOT NULL,
  PRIMARY KEY (`id`, `date`),
  UNIQUE KEY `DSNTA` (`date`,`source`,`name`,`type_id`,`area_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC
PARTITION BY RANGE COLUMNS(`date`) 
(
    PARTITION p201608 VALUES LESS THAN (20160901)
);

CREATE TABLE `hbase_inde_basicindex_days` (
  `id` int(15) NOT NULL AUTO_INCREMENT,
  `name` char(40) NOT NULL,
  `source` char(15) NOT NULL,
  `start_date` int(8) NOT NULL,
  `end_date` int(8) NOT NULL,
  `date_type` char(10) NOT NULL,
  `type_id` char(200) NOT NULL,
  `area_id` char(30) NOT NULL,
  `value` int(20) NOT NULL,
  PRIMARY KEY (`id`, `end_date`),
  UNIQUE KEY `SNSETAD` (`start_date`,`name`,`source`,`end_date`,`type_id`,`area_id`,`date_type`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC
PARTITION BY RANGE COLUMNS(`end_date`) 
(
    PARTITION p201608 VALUES LESS THAN (20160901)
);

查看表分区：
select partition_name, partition_expression, partition_description, table_rows from information_schema.partitions where table_schema=schema() and table_name='hbase_basicindex_hourpro';

查看表大小：
SELECT TABLE_NAME,DATA_LENGTH+INDEX_LENGTH,TABLE_ROWS FROM information_schema.tables WHERE TABLE_SCHEMA='areadata_dev' AND TABLE_NAME='hbase_basicindex_hourpro';


CREATE TABLE `hbase_inde_table_info` (
  `id` int(3) NOT NULL AUTO_INCREMENT,
  `table_name` char(80) NOT NULL,
  `source` char(15) NOT NULL,
  `update_date` int(10) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `NAME_TYPE` (`table_name`,`source`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;
"""
