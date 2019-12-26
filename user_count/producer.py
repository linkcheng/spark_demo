#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zheng.long@sfy.com
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

# from config.DB import DB_CONFIG

DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '12341234',
    'db': 'v2',
    'charset': 'utf8',
}

# HOSTS = "192.168.30.141:6667,192.168.30.140:6667,192.168.30.139:6667"
HOSTS = "127.0.0.1:9092"
TOPIC = "v2.User"
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
        sql = """SELECT %s FROM sfy_v2.User 
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


def generate():
    null = None
    false = False

    def get(index, created_time, updated_time=None):
        if not updated_time:
            updated_time = created_time

        data = [
            {
                "data": [
                    {
                        "id": "1111111401",
                        "name": "XYF20806056",
                        "password_digest": "994ead90845bd930afdf48b3b9ff334e",
                        "mobile": "18212341234",
                        "email": null,
                        "created_time": created_time,
                        "created_ip": "11.122.68.106",
                        "last_login_time": "2019-01-02 15:39:52",
                        "last_login_ip": "11.122.68.106",
                        "old_id": "0",
                        "weixin_open_id": null,
                        "gender": "3",
                        "person_id": "1943564",
                        "utm_source": "信用飞APP",
                        "order_cnt": "0",
                        "bill_status": "1",
                        "console_remark": "",
                        "updated_time": updated_time,
                        "app_source": "autoupdate",
                        "is_get_authorize": "0",
                        "user_address_list": null,
                        "bankNumber": null,
                        "biz_event_status": null,
                        "biz_event_time": null,
                        "biz_event_data": null,
                        "invitation_code": null,
                        "used_invitation_code": null
                    },
                    {
                        "id": "1111111402",
                        "name": null,
                        "password_digest": "",
                        "mobile": "15112341234",
                        "email": null,
                        "created_time": created_time,
                        "created_ip": "22.104.147.244",
                        "last_login_time": "2019-01-02 15:39:52",
                        "last_login_ip": "22.104.147.244",
                        "old_id": "0",
                        "weixin_open_id": null,
                        "gender": "3",
                        "person_id": "0",
                        "utm_source": "小白信用分",
                        "order_cnt": "0",
                        "bill_status": "1",
                        "console_remark": "",
                        "updated_time": updated_time,
                        "app_source": "",
                        "is_get_authorize": "0",
                        "user_address_list": null,
                        "bankNumber": null,
                        "biz_event_status": null,
                        "biz_event_time": null,
                        "biz_event_data": null,
                        "invitation_code": null,
                        "used_invitation_code": null
                    }
                ],
                "database": "sfy_v2",
                "es": 1546415113000,
                "id": 119,
                "isDdl": false,
                "mysqlType": {
                    "id": "bigint(20) unsigned",
                    "name": "varchar(63)",
                    "password_digest": "varchar(63)",
                    "mobile": "varchar(63)",
                    "email": "varchar(127)",
                    "created_time": "datetime",
                    "created_ip": "varchar(63)",
                    "last_login_time": "datetime",
                    "last_login_ip": "varchar(63)",
                    "old_id": "bigint(20) unsigned",
                    "weixin_open_id": "varchar(63)",
                    "gender": "enum('male','female','other')",
                    "person_id": "bigint(20) unsigned",
                    "utm_source": "varchar(127)",
                    "order_cnt": "smallint(5) unsigned",
                    "bill_status": "enum('none','has_bill','has_overdue','overdue')",
                    "console_remark": "varchar(512)",
                    "updated_time": "timestamp",
                    "app_source": "varchar(63)",
                    "is_get_authorize": "tinyint(1)",
                    "user_address_list": "varchar(500)",
                    "bankNumber": "varchar(36)",
                    "biz_event_status": "varchar(32)",
                    "biz_event_time": "datetime",
                    "biz_event_data": "varchar(500)",
                    "invitation_code": "varchar(16)",
                    "used_invitation_code": "varchar(16)"
                },
                "old": null,
                "sql": "",
                "sqlType": {
                    "id": -5,
                    "name": 12,
                    "password_digest": 12,
                    "mobile": 12,
                    "email": 12,
                    "created_time": 93,
                    "created_ip": 12,
                    "last_login_time": 93,
                    "last_login_ip": 12,
                    "old_id": -5,
                    "weixin_open_id": 12,
                    "gender": 4,
                    "person_id": -5,
                    "utm_source": 12,
                    "order_cnt": 5,
                    "bill_status": 4,
                    "console_remark": 12,
                    "updated_time": 93,
                    "app_source": 12,
                    "is_get_authorize": -7,
                    "user_address_list": 12,
                    "bankNumber": 12,
                    "biz_event_status": 12,
                    "biz_event_time": 93,
                    "biz_event_data": 12,
                    "invitation_code": 12,
                    "used_invitation_code": 12
                },
                "table": "User",
                "ts": 1546415113859,
                "type": "INSERT"
            },

            {
                "data": [
                    {
                        "id": "1111111349",
                        "name": "张三",
                        "password_digest": "957f8fcecdc4bc8199d8c52c6d998719",
                        "mobile": "13512341234",
                        "email": "",
                        "created_time": created_time,
                        "created_ip": "11.11.11.11",
                        "last_login_time": "2018-11-08 22:59:19",
                        "last_login_ip": "22.22.22.22",
                        "old_id": "3056",
                        "weixin_open_id": null,
                        "gender": "3",
                        "person_id": "53633",
                        "utm_source": "",
                        "order_cnt": "3",
                        "bill_status": "2",
                        "console_remark": "",
                        "updated_time": updated_time,
                        "app_source": null,
                        "is_get_authorize": "0",
                        "user_address_list": null,
                        "bankNumber": null,
                        "biz_event_status": null,
                        "biz_event_time": null,
                        "biz_event_data": null,
                        "invitation_code": null,
                        "used_invitation_code": null
                    }
                ],
                "database": "test",
                "es": 1545805527000,
                "id": 19,
                "isDdl": false,
                "mysqlType": {
                    "id": "bigint(20) unsigned",
                    "name": "varchar(63)",
                    "password_digest": "varchar(63)",
                    "mobile": "varchar(63)",
                    "email": "varchar(127)",
                    "created_time": "datetime",
                    "created_ip": "varchar(63)",
                    "last_login_time": "datetime",
                    "last_login_ip": "varchar(63)",
                    "old_id": "bigint(20) unsigned",
                    "weixin_open_id": "varchar(63)",
                    "gender": "enum('male','female','other')",
                    "person_id": "bigint(20) unsigned",
                    "utm_source": "varchar(127)",
                    "order_cnt": "smallint(5) unsigned",
                    "bill_status": "enum('none','has_bill','has_overdue','overdue')",
                    "console_remark": "varchar(512)",
                    "updated_time": "timestamp",
                    "app_source": "varchar(63)",
                    "is_get_authorize": "tinyint(1)",
                    "user_address_list": "varchar(500)",
                    "bankNumber": "varchar(36)",
                    "biz_event_status": "varchar(32)",
                    "biz_event_time": "datetime",
                    "biz_event_data": "varchar(500)",
                    "invitation_code": "varchar(16)",
                    "used_invitation_code": "varchar(16)"
                },
                "old": null,
                "sql": "",
                "sqlType": {
                    "id": -5,
                    "name": 12,
                    "password_digest": 12,
                    "mobile": 12,
                    "email": 12,
                    "created_time": 93,
                    "created_ip": 12,
                    "last_login_time": 93,
                    "last_login_ip": 12,
                    "old_id": -5,
                    "weixin_open_id": 12,
                    "gender": 4,
                    "person_id": -5,
                    "utm_source": 12,
                    "order_cnt": 5,
                    "bill_status": 4,
                    "console_remark": 12,
                    "updated_time": 93,
                    "app_source": 12,
                    "is_get_authorize": -7,
                    "user_address_list": 12,
                    "bankNumber": 12,
                    "biz_event_status": 12,
                    "biz_event_time": 93,
                    "biz_event_data": 12,
                    "invitation_code": 12,
                    "used_invitation_code": 12
                },
                "table": "User",
                "ts": 1545805527438,
                "type": "INSERT"
            },

            {
                "data": [
                    {
                        "id": "1111111403",
                        "name": "XYF20806056",
                        "password_digest": "994ead90845bd930afdf48b3b9ff334e",
                        "mobile": "18212341234",
                        "email": null,
                        "created_time": created_time,
                        "created_ip": "11.122.68.106",
                        "last_login_time": "2019-01-02 15:39:52",
                        "last_login_ip": "11.122.68.106",
                        "old_id": "0",
                        "weixin_open_id": null,
                        "gender": "3",
                        "person_id": "1943564",
                        "utm_source": "信用飞APP",
                        "order_cnt": "0",
                        "bill_status": "1",
                        "console_remark": "",
                        "updated_time": updated_time,
                        "app_source": "autoupdate",
                        "is_get_authorize": "0",
                        "user_address_list": null,
                        "bankNumber": null,
                        "biz_event_status": null,
                        "biz_event_time": null,
                        "biz_event_data": null,
                        "invitation_code": null,
                        "used_invitation_code": null
                    }
                ],
                "database": "sfy_v2",
                "es": 1546415154000,
                "id": 121,
                "isDdl": false,
                "mysqlType": {
                    "id": "bigint(20) unsigned",
                    "name": "varchar(63)",
                    "password_digest": "varchar(63)",
                    "mobile": "varchar(63)",
                    "email": "varchar(127)",
                    "created_time": "datetime",
                    "created_ip": "varchar(63)",
                    "last_login_time": "datetime",
                    "last_login_ip": "varchar(63)",
                    "old_id": "bigint(20) unsigned",
                    "weixin_open_id": "varchar(63)",
                    "gender": "enum('male','female','other')",
                    "person_id": "bigint(20) unsigned",
                    "utm_source": "varchar(127)",
                    "order_cnt": "smallint(5) unsigned",
                    "bill_status": "enum('none','has_bill','has_overdue','overdue')",
                    "console_remark": "varchar(512)",
                    "updated_time": "timestamp",
                    "app_source": "varchar(63)",
                    "is_get_authorize": "tinyint(1)",
                    "user_address_list": "varchar(500)",
                    "bankNumber": "varchar(36)",
                    "biz_event_status": "varchar(32)",
                    "biz_event_time": "datetime",
                    "biz_event_data": "varchar(500)",
                    "invitation_code": "varchar(16)",
                    "used_invitation_code": "varchar(16)"
                },
                "old": null,
                "sql": "",
                "sqlType": {
                    "id": -5,
                    "name": 12,
                    "password_digest": 12,
                    "mobile": 12,
                    "email": 12,
                    "created_time": 93,
                    "created_ip": 12,
                    "last_login_time": 93,
                    "last_login_ip": 12,
                    "old_id": -5,
                    "weixin_open_id": 12,
                    "gender": 4,
                    "person_id": -5,
                    "utm_source": 12,
                    "order_cnt": 5,
                    "bill_status": 4,
                    "console_remark": 12,
                    "updated_time": 93,
                    "app_source": 12,
                    "is_get_authorize": -7,
                    "user_address_list": 12,
                    "bankNumber": 12,
                    "biz_event_status": 12,
                    "biz_event_time": 93,
                    "biz_event_data": 12,
                    "invitation_code": 12,
                    "used_invitation_code": 12
                },
                "table": "User",
                "ts": 1546415154516,
                "type": "INSERT"
            },

            {
                "data": [
                    {
                        "id": "1111111404",
                        "name": "XYF20806056",
                        "password_digest": "994ead90845bd930afdf48b3b9ff334e",
                        "mobile": "18212341234",
                        "email": null,
                        "created_time": created_time,
                        "created_ip": "11.122.68.106",
                        "last_login_time": "2019-01-02 15:39:52",
                        "last_login_ip": "11.122.68.106",
                        "old_id": "0",
                        "weixin_open_id": null,
                        "gender": "3",
                        "person_id": "1943564",
                        "utm_source": "信用飞APP",
                        "order_cnt": "0",
                        "bill_status": "1",
                        "console_remark": "",
                        "updated_time": updated_time,
                        "app_source": "autoupdate",
                        "is_get_authorize": "0",
                        "user_address_list": null,
                        "bankNumber": null,
                        "biz_event_status": null,
                        "biz_event_time": null,
                        "biz_event_data": null,
                        "invitation_code": null,
                        "used_invitation_code": null
                    }
                ],
                "database": "sfy_v2",
                "es": 1546415154000,
                "id": 122,
                "isDdl": false,
                "mysqlType": {
                    "id": "bigint(20) unsigned",
                    "name": "varchar(63)",
                    "password_digest": "varchar(63)",
                    "mobile": "varchar(63)",
                    "email": "varchar(127)",
                    "created_time": "datetime",
                    "created_ip": "varchar(63)",
                    "last_login_time": "datetime",
                    "last_login_ip": "varchar(63)",
                    "old_id": "bigint(20) unsigned",
                    "weixin_open_id": "varchar(63)",
                    "gender": "enum('male','female','other')",
                    "person_id": "bigint(20) unsigned",
                    "utm_source": "varchar(127)",
                    "order_cnt": "smallint(5) unsigned",
                    "bill_status": "enum('none','has_bill','has_overdue','overdue')",
                    "console_remark": "varchar(512)",
                    "updated_time": "timestamp",
                    "app_source": "varchar(63)",
                    "is_get_authorize": "tinyint(1)",
                    "user_address_list": "varchar(500)",
                    "bankNumber": "varchar(36)",
                    "biz_event_status": "varchar(32)",
                    "biz_event_time": "datetime",
                    "biz_event_data": "varchar(500)",
                    "invitation_code": "varchar(16)",
                    "used_invitation_code": "varchar(16)"
                },
                "old": null,
                "sql": "",
                "sqlType": {
                    "id": -5,
                    "name": 12,
                    "password_digest": 12,
                    "mobile": 12,
                    "email": 12,
                    "created_time": 93,
                    "created_ip": 12,
                    "last_login_time": 93,
                    "last_login_ip": 12,
                    "old_id": -5,
                    "weixin_open_id": 12,
                    "gender": 4,
                    "person_id": -5,
                    "utm_source": 12,
                    "order_cnt": 5,
                    "bill_status": 4,
                    "console_remark": 12,
                    "updated_time": 93,
                    "app_source": 12,
                    "is_get_authorize": -7,
                    "user_address_list": 12,
                    "bankNumber": 12,
                    "biz_event_status": 12,
                    "biz_event_time": 93,
                    "biz_event_data": 12,
                    "invitation_code": 12,
                    "used_invitation_code": 12
                },
                "table": "User",
                "ts": 1546415155021,
                "type": "INSERT"
            },

            {
                "data": [
                    {
                        "id": "1111111405",
                        "name": "刘邦",
                        "password_digest": "",
                        "mobile": "17712341234",
                        "email": "",
                        "created_time": created_time,
                        "created_ip": "11.34.203.168",
                        "last_login_time": "2019-01-02 17:09:58",
                        "last_login_ip": "11.34.203.168",
                        "old_id": "0",
                        "weixin_open_id": null,
                        "gender": "3",
                        "person_id": "0",
                        "utm_source": "SDK_iGola",
                        "order_cnt": "0",
                        "bill_status": "1",
                        "console_remark": "",
                        "updated_time": updated_time,
                        "app_source": "",
                        "is_get_authorize": "0",
                        "user_address_list": null,
                        "bankNumber": null,
                        "biz_event_status": null,
                        "biz_event_time": null,
                        "biz_event_data": null,
                        "invitation_code": null,
                        "used_invitation_code": null
                    }
                ],
                "database": "sfy_v2",
                "es": 1546420198000,
                "id": 362,
                "isDdl": false,
                "mysqlType": {
                    "id": "bigint(20) unsigned",
                    "name": "varchar(63)",
                    "password_digest": "varchar(63)",
                    "mobile": "varchar(63)",
                    "email": "varchar(127)",
                    "created_time": "datetime",
                    "created_ip": "varchar(63)",
                    "last_login_time": "datetime",
                    "last_login_ip": "varchar(63)",
                    "old_id": "bigint(20) unsigned",
                    "weixin_open_id": "varchar(63)",
                    "gender": "enum('male','female','other')",
                    "person_id": "bigint(20) unsigned",
                    "utm_source": "varchar(127)",
                    "order_cnt": "smallint(5) unsigned",
                    "bill_status": "enum('none','has_bill','has_overdue','overdue')",
                    "console_remark": "varchar(512)",
                    "updated_time": "timestamp",
                    "app_source": "varchar(63)",
                    "is_get_authorize": "tinyint(1)",
                    "user_address_list": "varchar(500)",
                    "bankNumber": "varchar(36)",
                    "biz_event_status": "varchar(32)",
                    "biz_event_time": "datetime",
                    "biz_event_data": "varchar(500)",
                    "invitation_code": "varchar(16)",
                    "used_invitation_code": "varchar(16)"
                },
                "old": null,
                "sql": "",
                "sqlType": {
                    "id": -5,
                    "name": 12,
                    "password_digest": 12,
                    "mobile": 12,
                    "email": 12,
                    "created_time": 93,
                    "created_ip": 12,
                    "last_login_time": 93,
                    "last_login_ip": 12,
                    "old_id": -5,
                    "weixin_open_id": 12,
                    "gender": 4,
                    "person_id": -5,
                    "utm_source": 12,
                    "order_cnt": 5,
                    "bill_status": 4,
                    "console_remark": 12,
                    "updated_time": 93,
                    "app_source": 12,
                    "is_get_authorize": -7,
                    "user_address_list": 12,
                    "bankNumber": 12,
                    "biz_event_status": 12,
                    "biz_event_time": 93,
                    "biz_event_data": 12,
                    "invitation_code": 12,
                    "used_invitation_code": 12
                },
                "table": "User",
                "ts": 1546420198108,
                "type": "INSERT"
            },

            {
                "data": [
                    {
                        "id": "1111111405",
                        "name": "刘邦",
                        "password_digest": "",
                        "mobile": "17712341234",
                        "email": "",
                        "created_time": created_time,
                        "created_ip": "12.34.203.168",
                        "last_login_time": "2019-01-02 17:09:58",
                        "last_login_ip": "12.34.203.168",
                        "old_id": "0",
                        "weixin_open_id": null,
                        "gender": "3",
                        "person_id": "9030",
                        "utm_source": "SDK_iGola",
                        "order_cnt": "0",
                        "bill_status": "1",
                        "console_remark": "",
                        "updated_time": updated_time,
                        "app_source": "",
                        "is_get_authorize": "0",
                        "user_address_list": null,
                        "bankNumber": null,
                        "biz_event_status": null,
                        "biz_event_time": null,
                        "biz_event_data": null,
                        "invitation_code": null,
                        "used_invitation_code": null
                    }
                ],
                "database": "sfy_v2",
                "es": 1546420198000,
                "id": 363,
                "isDdl": false,
                "mysqlType": {
                    "id": "bigint(20) unsigned",
                    "name": "varchar(63)",
                    "password_digest": "varchar(63)",
                    "mobile": "varchar(63)",
                    "email": "varchar(127)",
                    "created_time": "datetime",
                    "created_ip": "varchar(63)",
                    "last_login_time": "datetime",
                    "last_login_ip": "varchar(63)",
                    "old_id": "bigint(20) unsigned",
                    "weixin_open_id": "varchar(63)",
                    "gender": "enum('male','female','other')",
                    "person_id": "bigint(20) unsigned",
                    "utm_source": "varchar(127)",
                    "order_cnt": "smallint(5) unsigned",
                    "bill_status": "enum('none','has_bill','has_overdue','overdue')",
                    "console_remark": "varchar(512)",
                    "updated_time": "timestamp",
                    "app_source": "varchar(63)",
                    "is_get_authorize": "tinyint(1)",
                    "user_address_list": "varchar(500)",
                    "bankNumber": "varchar(36)",
                    "biz_event_status": "varchar(32)",
                    "biz_event_time": "datetime",
                    "biz_event_data": "varchar(500)",
                    "invitation_code": "varchar(16)",
                    "used_invitation_code": "varchar(16)"
                },
                "old": [
                    {
                        "person_id": "0"
                    }
                ],
                "sql": "",
                "sqlType": {
                    "id": -5,
                    "name": 12,
                    "password_digest": 12,
                    "mobile": 12,
                    "email": 12,
                    "created_time": 93,
                    "created_ip": 12,
                    "last_login_time": 93,
                    "last_login_ip": 12,
                    "old_id": -5,
                    "weixin_open_id": 12,
                    "gender": 4,
                    "person_id": -5,
                    "utm_source": 12,
                    "order_cnt": 5,
                    "bill_status": 4,
                    "console_remark": 12,
                    "updated_time": 93,
                    "app_source": 12,
                    "is_get_authorize": -7,
                    "user_address_list": 12,
                    "bankNumber": 12,
                    "biz_event_status": 12,
                    "biz_event_time": 93,
                    "biz_event_data": 12,
                    "invitation_code": 12,
                    "used_invitation_code": 12
                },
                "table": "User",
                "ts": 1546420199014,
                "type": "UPDATE"
            },

        ]

        return data[index % len(data)]

    for i in range(1000):
        ct = datetime.now().strftime(FMT)
        yield get(i, ct)


def produce_example():
    topic = "example"
    client = KafkaClient(hosts=HOSTS)
    topic = client.topics[topic]

    with topic.get_sync_producer() as prod:
        for user in generate():
            print(user)
            time.sleep(random.randint(5, 10))
            prod.produce(json.dumps(user).encode())


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
    produce_example()
