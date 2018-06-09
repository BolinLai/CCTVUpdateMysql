import phoenixdb
import phoenixdb.cursor
import pprint
import time

from config import HbaseMysqlConfig

begin_time = time.time()
database_url = HbaseMysqlConfig.HBASE_IP
conn = phoenixdb.connect(database_url, autocommit=True)

cursor = conn.cursor()

# cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
# cursor.execute("SELECT * FROM users WHERE id=1")
# print cursor.fetchone()['USERNAME']


# sql_str = "SELECT count(*) FROM appdata.info"
# sql_str = "SELECT id, session_id, y, m, d from appdata.info limit 100"
# sql_str = "SELECT id, session_id, y, m, d from appdata.info where cast(y as char(4))||cast(m as char(2))='201801' limit 10"
# sql_str = "SELECT * from appdata.info2 limit 5"

# sql_str = "select tagid, count(*) from appdata.music2 where flag=1 group by tagid order by tagid"
# sql_str = "select tagid2, count(*) from appdata.info2 where flag=1 group by tagid2 order by tagid2"
# sql_str = "select province, count(*) from appdata.info2 where flag=1 group by province order by province"
# sql_str = "select channel, count(*) from appdata.info2 where flag=1 group by channel order by channel"
# sql_str = "select count(*) from appdata.info2 where content_id is not null and (tagid is null or tagid = 'null')"
# sql_str = "select count(*) from appdata.info2 where (tagid is not null or tagid <> 'null') and (tagid2 is null or tagid2 = 'null')"
# sql_str = "select count(*) from appdata.info2 where session_id is null or session_id='null'
sql_str = "select ymd, count(*) from appdata.cctv52 where flag=1 and ymd<=20161001 group by ymd order by ymd desc"
# sql_str = "select device_id, min(ymd) from appdata.info2 where device_id in (select distinct device_id from appdata.info2 where ymd>=20170304 and ymd<=20170304) group by device_id having min(ymd)=20170302"
# sql_str = "SELECT PROVINCE, COUNT(*) FROM (SELECT DISTINCT DEVICE_ID, ymd, PROVINCE FROM areadata.areadata_201801 where y<2017 and flag=1) a GROUP BY PROVINCE"
# sql_str = "select device_id, min(ymd) as ymd from appdata.info2 group by device_id having ymd>=20170601 and ymd<=20170604 order by ymd"
# sql_str = "select tagid, province, allwatchtime,count(*) from appdata.info2 where ymd>=20170401 and ymd<=20170401 and content_id is not null and content_id<>'null' and flag=1 group by tagid, province, allwatchtime order by tagid, province, allwatchtime asc"
# sql_str = "select ymd,province,content_id,count(*) from appdata.finance2 where ymd>=20170401 and ymd<=20170402 and content_id is not null and content_id<>'null' and flag=1 group by ymd,province,content_id  order by ymd,province,content_id asc"
# sql_str = "select count(*) from appdata.finance2 where ymd>=20170401 and ymd<=20170402 "


cursor.execute(sql_str)
pprint.pprint(cursor.fetchall())
cursor.close()
conn.close()
