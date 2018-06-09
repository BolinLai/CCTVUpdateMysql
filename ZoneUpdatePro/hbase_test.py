import phoenixdb
import phoenixdb.cursor
import pprint
import time

begin_time = time.time()
# database_url = 'http://172.16.8.150:8760/'
database_url = 'http://202.120.39.239:8760/'
conn = phoenixdb.connect(database_url, autocommit=True)

cursor = conn.cursor()

# cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
# cursor.execute("SELECT * FROM users WHERE id=1")
# print cursor.fetchone()['USERNAME']

# sql_str = "select Lower(tagid) as tag,province,device_id,sum(allwatchtime) as allwatchtime from areadata.areadata2 where ymd>=20170218 and ymd<=20170218 and flag=1 group by tag,province,device_id  order by tag,province,device_id asc"
# sql_str = "select tagid, count(*) from areadata.areadata2 where flag=1 group by tagid order by tagid"
# sql_str = "select province, count(*) from areadata.areadata2 where flag=1 group by province order by province"
# sql_str = "select count(*) from areadata.areadata2 where content_id is null or content_id='null' or content_id='Null' or content_id='NULL' or content_id='None' or content_id='none' or content_id='NONE'"
# sql_str = "select ymd, count(*) from areadata.areadata2 where flag=1 group by ymd order by ymd"
# sql_str = "select device_id, count(*) from areadata.areadata2 where flag=1 group by device_id order by count(*) desc limit 10"
# sql_str = "select content_id, count(*) from areadata.areadata2 where flag=1 and ymd=20170901 group by content_id order by count(*) desc limit10"


cursor.execute(sql_str)
pprint.pprint(cursor.fetchall())
cursor.close()
conn.close()

print time.time() - begin_time


