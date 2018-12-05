#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zheng.long@shoufuyou.com
@module: consumer 
@date: 11/30/18 
"""
from pykafka import KafkaClient


HOSTS = "192.168.30.141:6667,192.168.30.140:6667,192.168.30.139:6667"
TOPIC = 'UserCount'
ZOOKEEPER_CONNECT = 'hh001:2181,hh002:2181,hh0013:2181'


def consume_into_db():
    client = KafkaClient(hosts=HOSTS)
    topic = client.topics[TOPIC]

    balanced_consumer = topic.get_balanced_consumer(
        auto_commit_enable=False,
        zookeeper_connect=ZOOKEEPER_CONNECT
    )

    for message in balanced_consumer:
        # print message
        if message is not None:
            pass


def consume():
    client = KafkaClient(hosts=HOSTS)
    topic = client.topics[TOPIC]

    balanced_consumer = topic.get_balanced_consumer(
        auto_commit_enable=False,
        zookeeper_connect=ZOOKEEPER_CONNECT
    )

    for message in balanced_consumer:
        # print message
        if message is not None:
            # 打印接收到的消息体的偏移个数和值
            print(message.offset, message.value)


if __name__ == '__main__':
    consume()
