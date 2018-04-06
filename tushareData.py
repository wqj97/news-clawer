import tushare as ts

import pymysql
import time
from QcloudApi.qcloudapi import QcloudApi
import json

Wenzhi_config = {
    'Region': 'bj',
    'secretId': 'AKID99jjLdep9fbPZP96VmYfRkjORy3gocKV',
    'secretKey': 'rNSnqdmBIqB65Je73GoJIwCulXdXp9Pt',
    'method': 'POST',
    'SignatureMethod': 'HmacSHA1'
}

Wenzhi_action = 'TextSentiment'
Wenzhi_module = 'wenzhi'
service = QcloudApi(Wenzhi_module, Wenzhi_config)

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
    data = ts.get_latest_news(100, True)
    for index, val in data.iterrows():
        print('processing {}/{}'.format(index + 1, len(data)))
        sql = "SELECT EXISTS(SELECT id FROM news WHERE url = '%s')".format(val['url'])
        cursor.execute(sql)
        if cursor.fetchone()[0] == 1:
            continue
        analysis = service.call(Wenzhi_action, {'content': val['content']}).decode('utf-8')
        sql = "INSERT INTO news (title, content, news_time, classify,url,analysis, created_at, updated_at) VALUES ('{}', '{}', date_format(concat(year(now()), '-{}'), '%Y-%m-%d %H:%i'), '{}', '{}', '{}' , now(), now())".format(
            val['title'], val['content'], val['time'], val['classify'], val['url'], analysis)
        try:
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            print(e)
            continue
    print("sleeping")
    time.sleep(60)
