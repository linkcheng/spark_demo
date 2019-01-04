#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zheng.long@shoufuyou.com
@module: counter
@date: 11/30/18 
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, window, concat_ws, decode
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    TimestampType,
    ArrayType,
    LongType,
    IntegerType,
    ShortType,
    ByteType,
    BooleanType,
    NullType
)

# HOSTS = "192.168.30.141:6667,192.168.30.140:6667,192.168.30.139:6667"
HOSTS = "127.0.0.1:9092"


class ForeachWriter:
    def open(self, partition_id, epoch_id):
        """Open connection. This method is optional in Python."""

    def process(self, row):
        """Write row to connection. This method is not optional in Python."""

    def close(self, error):
        """Close the connection. This method in optional in Python."""


class SparkStructuredStreaming(object):
    """
    bin/pyspark --queue default --master yarn --deploy-mode client
    """
    def __init__(self, app_name):
        self.name = app_name
        self.session = None
        self.console_stream = None
        self.writer_stream = None
        self.writer_stream_db = None

    def __del__(self):
        if self.console_stream:
            self.console_stream.stop()
        if self.writer_stream:
            self.writer_stream.stop()
        if self.writer_stream_db:
            self.writer_stream_db.stop()

    def get_spark_session(self):
        """创建SparkSession """
        if not self.session:
            self.session = SparkSession.\
                builder. \
                master("local[*]").\
                appName(self.name).\
                getOrCreate()
        return self

    def read_stream(self, topic, schema=None):
            # .option("failOnDataLoss", False) \
            # .option("startingOffsets", "earliest") \
        df = self.session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", HOSTS) \
            .option("subscribe", topic) \
            .load()

        if schema:
            df = df.select(decode("value", "UTF-8").alias("value")) \
                .selectExpr("CAST(value AS STRING)") \
                .select(from_json("value", schema=schema).alias("_data")) \
                .selectExpr("_data.*")
        return df

    def print_console(self, df, mode="append"):
        self.console_stream = df \
            .writeStream \
            .outputMode(mode) \
            .format("console") \
            .start()
        self.console_stream.awaitTermination()

    def write_stream_to_kafka(self, df, topic,
                              checkpoint="/data/spark/checkpoint",
                              mode="update"):
        # .trigger(processingTime="2 minutes")
        self.writer_stream = df \
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", HOSTS) \
            .option("topic", topic) \
            .option("checkpointLocation", checkpoint) \
            .outputMode(mode) \
            .start()
        self.writer_stream.awaitTermination()

    def write_stream_to_db(self, df,
                           checkpoint="/data/spark/checkpoint",
                           mode="update"):
        self.writer_stream_db = df \
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .option("checkpointLocation", checkpoint) \
            .outputMode(mode) \
            .foreach(ForeachWriter()) \
            .start()
        self.writer_stream_db.awaitTermination()


def handle_example():
    topic = "example"
    schema = StructType([
        StructField("id", LongType()),
        StructField("es", LongType()),
        StructField("database", StringType()),
        StructField("table", StringType()),
        StructField("type", StringType()),
        StructField("isDdl", BooleanType()),
        StructField("data", ArrayType(StructType([
            StructField("id", LongType()),
            StructField("name", StringType()),
            StructField("password_digest", StringType()),
            StructField("mobile", StringType()),
            StructField("email", StringType()),
            StructField("created_time", TimestampType()),
            StructField("created_ip", StringType()),
            StructField("last_login_time", TimestampType()),
            StructField("last_login_ip", StringType()),
            StructField("old_id", LongType()),
            StructField("weixin_open_id", StringType()),
            StructField("gender", StringType()),
            StructField("person_id", LongType()),
            StructField("utm_source", StringType()),
            StructField("order_cnt", ShortType()),
            StructField("bill_status", StringType()),
            StructField("console_remark", StringType()),
            StructField("updated_time", TimestampType()),
            StructField("app_source", StringType()),
            StructField("is_get_authorize", ByteType()),
            StructField("user_address_list", StringType()),
            StructField("bankNumber", StringType()),
            StructField("biz_event_status", StringType()),
            StructField("biz_event_time", TimestampType()),
            StructField("biz_event_data", StringType()),
            StructField("invitation_code", StringType()),
            StructField("used_invitation_code", StringType()),
        ]))),
    ])

    spark = SparkStructuredStreaming("example") \
        .get_spark_session()

    # df = spark.read_stream(topic, schema)
    df = spark.read_stream(topic)
    df = df.select(decode("value", "UTF-8").alias("value")) \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json("value", schema=schema).alias("data"))  # \
        # .selectExpr("data.*")

    spark.print_console(df)
    # data.createOrReplaceTempView("example")

    # user_count = data.withWatermark("created_time", "2 minutes") \
    #     .groupBy(window("created_time", "1 minute", "1 minute"), "app_source") \
    #     .count() \
    #     .selectExpr("window.start", "window.end", "app_source", "count") \
    #     .withColumnRenamed("start", "key") \
    #     .select("key", concat_ws(',', "end", "app_source", "count").alias('value'))

    # spark.print_console(user_count)
    # spark.write_stream(user_count1, "UserCount")


def handle():
    topic1 = "shoufuyou_v2.User2"
    schema1 = StructType([
        StructField("app_source", StringType(), True),
        StructField("created_time", TimestampType(), True),
    ])

    spark1 = SparkStructuredStreaming("GetUsers")
    spark1.get_spark_session()
    data1 = spark1.read_stream(topic1, schema1)

    data1.createOrReplaceTempView("NewUser")

    user_count1 = data1.withWatermark("created_time", "2 minutes") \
        .groupBy(window("created_time", "1 minute", "1 minute"), "app_source") \
        .count() \
        .selectExpr("window.start", "window.end", "app_source", "count") \
        .withColumnRenamed("start", "key") \
        .select("key", concat_ws(',', "end", "app_source", "count").alias('value'))

    spark1.print_console(user_count1)
    # spark.write_stream(user_count1, "UserCount")


if __name__ == "__main__":
    handle_example()

import pandas as pd
data = {}
df = pd.DataFrame(data)
df.dropna()