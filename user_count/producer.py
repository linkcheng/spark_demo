#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zheng.long@shoufuyou.com
@module: producer 
@date: 11/30/18 
"""
import time
import json
import random
from datetime import datetime, timedelta, date

from pykafka import KafkaClient
import pymysql
from pymysql.cursors import DictCursor

from config.DB import DB_CONFIG


HOSTS = "192.168.30.141:6667,192.168.30.140:6667,192.168.30.139:6667"
TOPIC = "shoufuyou_v2.User"
FMT = '%Y-%m-%d %H:%M:%S'


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime(FMT)
        elif isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)


class DBHelper(object):
    def __init__(self, **kwargs):
        self.conn = pymysql.connect(**kwargs)
        self.cr = self.conn.cursor(DictCursor)

    def __del__(self):
        self.close()

    def rollback(self):
        self.conn.rollback()

    def commit(self):
        self.conn.commit()

    def close(self):
        if not self.conn._closed:
            self.cr.close()
            self.conn.close()


class User:
    def __init__(self, **config):
        self.db = DBHelper(**config)
        self.cursor = self.db.cr

    def get(self):
        now = datetime.now()
        start_str = (now - timedelta(minutes=5)).strftime(FMT)
        end_str = now.strftime(FMT)
        keys = ['app_source', 'created_time']
        sql = """SELECT %s FROM shoufuyou_v2.User 
            WHERE created_time>=%s and created_time<%s"""

        self.cursor.execute(sql, (','.join(keys), start_str, end_str))
        values = self.cursor.fetchall() or []
        for value in values:
            yield json.dumps(value, cls=DateEncoder)

    def __del__(self):
        self.db.close()


def produce_from_db():
    client = KafkaClient(hosts=HOSTS)
    topic = client.topics[TOPIC]
    u = User(**DB_CONFIG)

    with topic.get_sync_producer() as prod:
        for user in u.get():
            prod.produce(user)


def produce():
    client = KafkaClient(hosts=HOSTS)
    topic = client.topics[TOPIC]

    app_sources = ['ios', 'android']
    with topic.get_sync_producer() as prod:
        for i in range(1000):
            user = {
                'created_time': datetime.now().strftime(FMT),
                'app_source': app_sources[random.randint(0, 1)],
            }
            print(user)
            time.sleep(random.randint(1, 10))
            prod.produce(json.dumps(user))


if __name__ == '__main__':
    produce()
