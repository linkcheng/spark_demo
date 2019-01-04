#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zheng.long@shoufuyou.com
@module: data 
@date: 2019-01-02
bin/kafka-console-consumer.sh --bootstrap-server  192.168.30.141:6667,192.168.30.140:6667,192.168.30.139:6667 --topic example
"""
null = None
false = False
true = True

data = {
    "data": [
        {
            "id": "1111111349",
            "name": "张三",
            "password_digest": "957f8fcecdc4bc8199d8c52c6d998719",
            "mobile": "13512341234",
            "email": "",
            "created_time": "2015-08-27 23:54:08",
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
            "updated_time": "2018-12-26 14:25:27",
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
}
