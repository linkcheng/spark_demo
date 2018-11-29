#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zheng.long@shoufuyou.com
@module: spark_demo
@date: 11/15/2018 
"""
import time
import json
import random
from datetime import datetime, timedelta, date

import pymysql
from pymysql.cursors import DictCursor
from pykafka import KafkaClient

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql.functions import from_json, window

from config.DB import driver, url, user, password, dbtable_v2_user, DB_CONFIG

FMT = '%Y-%m-%d %H:%M:%S'


def word_count():
    path = ('hdfs:///data/bi/stg/stg_shoufuyou_v2_user_day/'
            'p_day_id=20180927/delta_0000001_0000001_0000/user.txt')
    # sc.getConf().getAll()
    sc = SparkContext('yarn', 'test')
    text_file = sc.textFile(path)
    counts = text_file.flatMap(lambda line: line.split("$")) \
                 .map(lambda word: (word, 1)) \
                 .reduceByKey(lambda a, b: a + b)

    counts.collect()


def read_mysql():
    spark = SparkSession.builder.appName('PySpark_SQL_example').getOrCreate()
    df = spark.read.format('jdbc') \
        .options(
            url=url,
            driver=driver,
            dbtable=dbtable_v2_user,
            user=user,
            password=password
        )\
        .load()
    df.printSchema()
    counts_by_app_source = df.groupBy("app_source").count()
    counts_by_app_source.show()


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


def get_users():
    db = DBHelper(**DB_CONFIG)
    cursor = db.cr

    _starts = [
        '2018-11-01 14:18:00',
        '2018-11-05 14:18:00',
        '2018-11-07 14:18:00',
        '2018-11-09 14:18:00',
        '2018-11-12 14:18:00',
        '2018-11-13 15:48:00'
    ]
    starts = _starts[::-1]

    keys = [
        'id', 'mobile', 'name', 'gender', 'app_source', 'created_time'
    ]
    # while True:
    for start in starts:
        # start = datetime.now()
        time.sleep(10)
        sql = """SELECT * FROM shoufuyou_v2.User WHERE created_time>=%s"""
        cursor.execute(sql, (start, ))
        values = cursor.fetchall() or []

        for value in values:
            # yield json.dumps(value)
            yield '$'.join(map(str, [value[key] for key in keys]))


class User:
    def __init__(self, **config):
        self.db = DBHelper(**config)
        self.cursor = self.db.cr

    def get(self):
        now = datetime.now()
        start_str = (now - timedelta(minutes=5)).strftime(FMT)
        end_str = now.strftime(FMT)
        keys = ['mobile', 'name', 'created_time']
        sql = """SELECT %s FROM shoufuyou_v2.User 
            WHERE created_time>=%s and created_time<%s"""

        self.cursor.execute(sql, (','.join(keys), start_str, end_str))
        values = self.cursor.fetchall() or []
        for value in values:
            yield json.dumps(value, cls=DateEncoder)

    def __del__(self):
        self.db.close()


def produce_users_from_db():
    hosts = "192.168.30.141:6667,192.168.30.140:6667,192.168.30.139:6667"
    topic = "shoufuyou_v2.User1"

    client = KafkaClient(hosts=hosts)
    # 选择一个topic
    topic = client.topics[topic]
    # 连接 User 表
    u = User(**DB_CONFIG)

    with topic.get_sync_producer() as prod:
        for user in u.get():
            prod.produce(user)


def produce_users():
    hosts = "192.168.30.141:6667,192.168.30.140:6667,192.168.30.139:6667"
    topic = "shoufuyou_v2.User1"

    client = KafkaClient(hosts=hosts)
    # 选择一个topic
    topic = client.topics[topic]

    with topic.get_sync_producer() as prod:
        for i in range(1000):
            user = {
                'mobile': 'mobile' + str(i),
                'name': 'name' + str(i),
                'created_time': datetime.now().strftime(FMT)
            }
            print(user)
            time.sleep(random.randint(1, 10))
            prod.produce(json.dumps(user))


def read_kafka():
    """
    ./bin/pyspark --queue default --master yarn --deploy-mode client
    """
    hosts = "192.168.30.141:6667,192.168.30.140:6667,192.168.30.139:6667"
    topic = "shoufuyou_v2.User"

    spark = SparkSession.builder.master('yarn').appName("GetUsers").getOrCreate()

    events = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", hosts) \
        .option("startingOffsets", "earliest") \
        .option("subscribe", topic) \
        .option("failOnDataLoss", False) \
        .load()
    events = events.selectExpr("CAST(value AS STRING)")
    schema = StructType([
        StructField("mobile", StringType(), True),
        StructField("name", StringType(), True),
        StructField("created_time", TimestampType(), True),
    ])

    data = events.select(from_json(events.value, schema).alias("User")) \
        .selectExpr("User.mobile", "User.name", "User.created_time")
    data.createOrReplaceTempView("NewUser")
    user_count = data.withWatermark("created_time", "10 minutes") \
        .groupBy(window("created_time", "5 minutes", "5 minutes")).count() \
        .selectExpr("window.start", "window.end", "count")

    # spark.sql("select * from User4").show()

    # query1 = user_count \
    #     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    #     .writeStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", hosts) \
    #     .option("topic", "user_count") \
    #     .option("checkpointLocation", "/data/spark/checkpoint") \
    #     .outputMode("update") \
    #     .trigger(processingTime='5 minutes') \
    #     .queryName("user_count") \
    #     .start()

    # query1 = user_count \
    #     .writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .trigger(processingTime='5 minutes') \
    #     .queryName("User1") \
    #     .start()

    query1 = user_count \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .trigger(processingTime='5 minutes') \
        .queryName("User1") \
        .start()

    query1.awaitTermination()
    query1.stop()


def tf_demo():
    import tensorflow as tf

    with tf.Session() as sess:
        with tf.device('/cpu:0'):
            matrix1 = tf.constant([[3, 3]])
            matrix2 = tf.constant([[2], [2]])
            product = tf.matmul(matrix1, matrix2)
            result = sess.run([product])
            print(result)


if __name__ == '__main__':
    produce_users()
