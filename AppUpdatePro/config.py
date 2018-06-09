# coding: utf-8
class HbaseMysqlConfig:
    """
    Config of Hbase and Mysql
    """
    HBASE_IP = "http://202.120.39.239:8760/"  # http://172.16.8.150:8760/  http://202.120.39.239:8760/  http://10.110.142.33:8765
    HBASE_TABLE = "appdata.info2"  # appdata.info2 / appdata.info_201801

    MYSQL_HOST = "202.120.39.153"  # "202.120.39.153"/"10.110.142.3"
    MYSQL_PORT = 3901  # 3901/3306
    MYSQL_USER = "root"
    MYSQL_PASSWD = "Rootdata_200"
    MYSQL_CHARSET = "utf8"
    MYSQL_DB = "appdata"  # appdata_dev/appdata

    def __init__(self):
        pass


class MysqlTable:
    """
    Mysql
    """
    TAG = HbaseMysqlConfig.MYSQL_DB + ".appdata_tag_list"
    NODE = HbaseMysqlConfig.MYSQL_DB + ".province"

    TABLE_INFO = HbaseMysqlConfig.MYSQL_DB + ".hbase_table_info"
    BASIC_INDEX_HOUR_PRO = HbaseMysqlConfig.MYSQL_DB + ".hbase_basicindex_hourpro"
    BASIC_INDEX_HOUR = HbaseMysqlConfig.MYSQL_DB + ".hbase_basicindex_hour"
    BASIC_INDEX_EVERYDAY_PRO = HbaseMysqlConfig.MYSQL_DB + ".hbase_basicindex_everydaypro"
    BASIC_INDEX_EVERYDAY = HbaseMysqlConfig.MYSQL_DB + ".hbase_basicindex_everyday"
    BASIC_INDEX_DAYS_PRO = HbaseMysqlConfig.MYSQL_DB + ".hbase_basicindex_dayspro"
    BASIC_INDEX_DAYS = HbaseMysqlConfig.MYSQL_DB + ".hbase_basicindex_days"
    SPECIFIC_INDEX = HbaseMysqlConfig.MYSQL_DB + ".hbase_specific_index"
    SPECIFIC_INDEX_HOUR = HbaseMysqlConfig.MYSQL_DB + ".hbase_specific_index_hour"

    def __init__(self):
        pass


START_YMD = '20170118'
START_YMDH = '2017011800'


START_UPDATE_DICTS = {
    MysqlTable.BASIC_INDEX_HOUR_PRO: {'hour': START_YMDH},
    MysqlTable.BASIC_INDEX_HOUR: {'hour': START_YMDH},
    MysqlTable.BASIC_INDEX_EVERYDAY_PRO: {'everyday': START_YMD},
    MysqlTable.BASIC_INDEX_EVERYDAY: {'everyday': START_YMD},
    MysqlTable.BASIC_INDEX_DAYS_PRO: {'7days': START_YMD, '30days': START_YMD, 'month': START_YMD},
    MysqlTable.BASIC_INDEX_DAYS: {'7days': START_YMD, '30days': START_YMD, 'month': START_YMD},
    MysqlTable.SPECIFIC_INDEX: {'None': START_YMD},
    MysqlTable.SPECIFIC_INDEX_HOUR: {'None': START_YMDH}
}


"""
创建数据表 APP
CREATE TABLE `hbase_basicindex_hourpro` (
  `id` bigint(50) NOT NULL AUTO_INCREMENT,
  `name` char(40) NOT NULL,
  `date` int(10) NOT NULL,
  `type_id` char(200) NOT NULL,
  `area_id` char(30) NOT NULL,
  `value` int(20) NOT NULL,
  PRIMARY KEY (`id`, `date`),
  UNIQUE KEY `DNTA` (`date`,`name`,`type_id`,`area_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC
PARTITION BY RANGE COLUMNS(`date`) 
(
    PARTITION p201701 VALUES LESS THAN (2017020100)
);

策略：
按月分区，每到一个新的月份，记得先新建一个分区：
ALTER TABLE hbase_basic_index_test ADD PARTITION (PARTITION s201705 VALUES LESS THAN (20170600));

CREATE TABLE `hbase_basicindex_hour` (
  `id` int(15) NOT NULL AUTO_INCREMENT,
  `name` char(40) NOT NULL,
  `date` int(10) NOT NULL,
  `type_id` char(200) NOT NULL,
  `area_id` char(30) NOT NULL,
  `app_key` char(40) NOT NULL,
  `app_version_code` char(15) NOT NULL,
  `app_channel` char(30) NOT NULL, 
  `value` int(20) NOT NULL,
  PRIMARY KEY (`id`, `date`),
  UNIQUE KEY `DNTAKVC` (`date`, `name`, `type_id`,`area_id`, `app_key`, `app_version_code`, `app_channel`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC
PARTITION BY RANGE COLUMNS(`date`) 
(
    PARTITION p201701 VALUES LESS THAN (2017020100)
);


CREATE TABLE `hbase_basicindex_everydaypro` (
  `id` bigint(50) NOT NULL AUTO_INCREMENT,
  `name` char(40) NOT NULL,
  `date` int(8) NOT NULL,
  `type_id` char(200) NOT NULL,
  `area_id` char(30) NOT NULL,
  `value` int(20) NOT NULL,
  PRIMARY KEY (`id`, `date`),
  UNIQUE KEY `DNTA` (`date`,`name`,`type_id`,`area_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC
PARTITION BY RANGE COLUMNS(`date`) 
(
    PARTITION p201701 VALUES LESS THAN (20170201)
);


CREATE TABLE `hbase_basicindex_everyday` (
  `id` int(15) NOT NULL AUTO_INCREMENT,
  `name` char(40) NOT NULL,
  `date` int(8) NOT NULL,
  `type_id` char(200) NOT NULL,
  `area_id` char(30) NOT NULL,
  `app_key` char(40) NOT NULL,
  `app_version_code` char(15) NOT NULL,
  `app_channel` char(30) NOT NULL, 
  `value` int(20) NOT NULL,
  PRIMARY KEY (`id`, `date`),
  UNIQUE KEY `DNTAKVC` (`date`,`name`,`type_id`,`area_id`,`app_key`, `app_version_code`, `app_channel`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC
PARTITION BY RANGE COLUMNS(`date`) 
(
    PARTITION p201701 VALUES LESS THAN (20170201)
);


CREATE TABLE `hbase_basicindex_dayspro` (
  `id` bigint(50) NOT NULL AUTO_INCREMENT,
  `name` char(40) NOT NULL,
  `start_date` int(8) NOT NULL,
  `end_date` int(8) NOT NULL,
  `date_type` char(10) NOT NULL,
  `type_id` char(200) NOT NULL,
  `area_id` char(30) NOT NULL,
  `value` int(20) NOT NULL,
  PRIMARY KEY (`id`, `end_date`),
  UNIQUE KEY `SNDETA` (`start_date`,`name`,`date_type`,`end_date`,`type_id`,`area_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC
PARTITION BY RANGE COLUMNS(`end_date`) 
(
    PARTITION p201701 VALUES LESS THAN (20170201)
);


CREATE TABLE `hbase_basicindex_days` (
  `id` int(15) NOT NULL AUTO_INCREMENT,
  `name` char(40) NOT NULL,
  `start_date` int(8) NOT NULL,
  `end_date` int(8) NOT NULL,
  `date_type` char(10) NOT NULL,
  `type_id` char(200) NOT NULL,
  `area_id` char(30) NOT NULL,
  `app_key` char(40) NOT NULL,
  `app_version_code` char(15) NOT NULL,
  `app_channel` char(30) NOT NULL, 
  `value` int(20) NOT NULL,
  PRIMARY KEY (`id`, `end_date`),
  UNIQUE KEY `SNDETAKVC` (`start_date`,`name`,`date_type`,`end_date`,`type_id`,`area_id`,`app_key`, `app_version_code`, `app_channel`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC
PARTITION BY RANGE COLUMNS(`end_date`) 
(
    PARTITION p201701 VALUES LESS THAN (20170201)
);


CREATE TABLE `hbase_specific_index_hour` (
  `id` int(15) NOT NULL AUTO_INCREMENT,
  `name` char(40) NOT NULL,
  `start_date` int(10) NOT NULL,
  `end_date` int(10) NOT NULL,
  `date_type` char(10) NOT NULL,
  `type_id` char(200) NOT NULL,
  `area_id` char(30) NOT NULL,
  `value` mediumtext NOT NULL,
  PRIMARY KEY (`id`, `end_date`),
  UNIQUE KEY `SNDETA` (`start_date`,`name`,`date_type`,`end_date`,`type_id`,`area_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC
PARTITION BY RANGE COLUMNS(`end_date`) 
(
    PARTITION p201701 VALUES LESS THAN (2017020100)
);


CREATE TABLE `hbase_specific_index` (
  `id` int(15) NOT NULL AUTO_INCREMENT,
  `name` char(40) NOT NULL,
  `start_date` int(8) NOT NULL,
  `end_date` int(8) NOT NULL,
  `date_type` char(10) NOT NULL,
  `type_id` char(200) NOT NULL,
  `area_id` char(30) NOT NULL,
  `value` mediumtext NOT NULL,
  PRIMARY KEY (`id`, `end_date`),
  UNIQUE KEY `SNDETA` (`start_date`,`name`,`date_type`,`end_date`,`type_id`,`area_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC
PARTITION BY RANGE COLUMNS(`end_date`) 
(
    PARTITION p201701 VALUES LESS THAN (20170201)
);

查看表分区：
select partition_name, partition_expression, partition_description, table_rows from information_schema.partitions where table_schema=schema() and table_name='hbase_basicindex_hourpro';

查看表大小：
SELECT TABLE_NAME,DATA_LENGTH+INDEX_LENGTH,TABLE_ROWS FROM information_schema.tables WHERE TABLE_SCHEMA='areadata_dev' AND TABLE_NAME='hbase_basicindex_hourpro';


CREATE TABLE `hbase_table_info` (
  `id` int(3) NOT NULL AUTO_INCREMENT,
  `table_name` char(80) NOT NULL,
  `date_type` char(10) NOT NULL,
  `update_date` int(10) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `NAME_TYPE` (`table_name`,`date_type`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;
"""
