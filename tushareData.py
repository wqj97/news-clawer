import tushare as ts

import pymysql
import time

conn = pymysql.Connect(
    host='localhost',
    user='root',
    password='wqj9705',
    db='ynQuant',
    port=3306,
    charset='utf8'
)
cursor = conn.cursor()

while True:
    print("wakeing")
    data = ts.get_latest_news(80, True)
    for index, val in data.iterrows():
        sql = "INSERT INTO `news`(`classify`,`title`,`url`,`content`, `created_at`, `updated_at`) VALUES(%s,%s,%s,%s, now(), now())"
        try:
            cursor.execute(sql, (val['classify'], val['title'], val['url'], val['content']))
            conn.commit()
        except Exception as e:
            continue
    print("sleeping")
    time.sleep(60)
