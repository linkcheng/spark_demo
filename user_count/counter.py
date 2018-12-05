#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zheng.long@shoufuyou.com
@module: counter
@date: 11/30/18 
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql.functions import from_json, window, concat_ws

HOSTS = "192.168.30.141:6667,192.168.30.140:6667,192.168.30.139:6667"


class SparkStructuredStreaming(object):
    """
    bin/pyspark --queue default --master yarn --deploy-mode client
    """
    def __init__(self, app_name):
        self.name = app_name
        self.session = None
        self.console_stream = None
        self.writer_stream = None
    def __del__(self):
        if self.session:
            self.session.stop()
        if self.console_stream:
            self.console_stream.stop()
        if self.writer_stream:
            self.writer_stream.stop()
    def get_spark_session(self):
        """创建SparkSession """
        if not self.session:
            self.session = SparkSession.builder.appName(self.name).getOrCreate()
        return self
    def read_stream(self, topic, schema=None):
        df = self.session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", HOSTS) \
            .option("startingOffsets", "earliest") \
            .option("subscribe", topic) \
            .option("failOnDataLoss", False) \
            .load()
        if schema:
            df = df.selectExpr("CAST(value AS STRING)") \
                .select(from_json("value", schema=schema).alias("data")) \
                .selectExpr("data.*")
        return df
    def print_console(self, df):
        self.console_stream = df \
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()
    def write_stream(self, df, topic, checkpoint="/data/spark/checkpoint"):
        # .trigger(processingTime="2 minutes")
        self.writer_stream = df \
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", HOSTS) \
            .option("topic", topic) \
            .option("checkpointLocation", checkpoint) \
            .outputMode("update") \
            .start()
        self.writer_stream.awaitTermination()


def handle():
    topic = "shoufuyou_v2.User2"
    schema = StructType([
        StructField("app_source", StringType(), True),
        StructField("created_time", TimestampType(), True),
    ])

    spark = SparkStructuredStreaming("GetUsers")
    spark.get_spark_session()
    data = spark.read_stream(topic, schema)

    data.createOrReplaceTempView("NewUser")

    user_count = data.withWatermark("created_time", "2 minutes") \
        .groupBy(window("created_time", "1 minute", "1 minute"), "app_source") \
        .count() \
        .selectExpr("window.start", "window.end", "app_source", "count") \
        .withColumnRenamed("start", "key") \
        .select("key", concat_ws(',', "end", "app_source", "count").alias('value'))

    spark.print_console(user_count)
    # spark.write_stream(user_count, "UserCount")


if __name__ == "__main__":
    handle()

topic = "shoufuyou_v2.User2"
schema = StructType([
    StructField("app_source", StringType(), True),
    StructField("created_time", TimestampType(), True),
])

spark = SparkStructuredStreaming("GetUsers")
spark.get_spark_session()
data = spark.read_stream(topic, schema)

data.createOrReplaceTempView("NewUser")

user_count = data.withWatermark("created_time", "2 minutes") \
    .groupBy(window("created_time", "1 minute", "1 minute"), "app_source") \
    .count() \
    .selectExpr("window.start", "window.end", "app_source", "count") \
    .withColumnRenamed("start", "key") \
    .select("key", concat_ws(',', "end", "app_source", "count").alias('value'))

user_count.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

spark.print_console(user_count)

cs = user_count \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("UC") \
    .start()
