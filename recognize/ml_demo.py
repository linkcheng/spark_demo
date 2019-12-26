#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zheng.long@sfy.com
@module: ml_demo 
@date: 9/20/18 
"""
from pyspark.sql import SparkSession, Row
from pyspark.ml.feature import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.linalg import Vector


def demo1():
    spark = SparkSession.builder.master('local').appName('ml_demo').getOrCreate()
    seq = [
        [0, 'a b c d e spark', 1.0],
        [1, 'b d', 0.0],
        [2, 'spark f g h', 1.0],
        [3, 'hadoop mapreduce', 0.0],
    ]
    training = spark.createDataFrame(seq).toDF('id', 'text', 'label')

    tokenizer = Tokenizer().setInputCol('text').setOutputCol('words')
    hashingTF = HashingTF().setNumFeatures(1000).\
        setInputCol(tokenizer.getOutputCol()).\
        setOutputCol('features')
    lr = LogisticRegression().setMaxIter(10).setRegParam(0.01)

    # 按照具体的处理逻辑有序地组织PipelineStages，并 创建一个Pipeline,
    # 现在构建的Pipeline本质上是一个Estimator
    pipeline = Pipeline().setStages([tokenizer, hashingTF, lr])
    # Estimator，在它的fit() 方法运行之后，
    # 它将产生一个PipelineModel，它是一个 Transformer
    model = pipeline.fit(training)

    # 构建测试数据
    test = spark.createDataFrame([
        (4, 'spark i j k'),
        (5, 'I m n'),
        (6, 'spark a'),
        (7, 'apache hadoop'),
    ]).toDF('id', 'text')
    # 调用之前训练好的PipelineModel的transform()方法，
    # 让测试数据按顺序通过拟合的工作流，生成预测结果
    model.transform(test).select('id', 'text', 'probability', 'prediction').\
        collect()


if __name__ == '__main__':
    pass
