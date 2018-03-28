#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tushare as ts
import pymysql
import time

while True:
    data = ts.get_latest_news(top=20)
    currentTime = time.ctime()
    for index, val in data.iterrows():
        content = ts.latest_content(val['url'])
        # content = latest_content(url)
        conn = pymysql.Connect(
            host='localhost',
            user='root',
            password='root',
            db='ynQuart',
            port=3306,
            charset='utf8'
        )
        try:
            cursor = conn.cursor()
            sql = "INSERT INTO `news`(`classify`,`title`,`newsTime`,`url`,`content`) VALUES(%s,%s,%s,%s,%s)"
            cursor.execute(sql, (val['classify'], val['title'], val['time'], val['url'], content))
            conn.commit()
        except pymysql.err.IntegrityError:
            print(pymysql.err.IntegrityError)
            print("%s : 数据已经插入，暂未更新" % currentTime)
        finally:
            conn.close()
    time.sleep(6000)
