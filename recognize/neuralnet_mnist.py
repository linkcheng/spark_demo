#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zheng.long@shoufuyou.com
@module: neuralnet_mnist 
@date: 2018-12-30 
"""
import pickle
import numpy as np
from recognize.code_recognise import get_data
from recognize.nnetwork import sigmoid


def init_work():
    with open("sample_weight.pkl", "rb") as f:
        nw = pickle.load(f)
    return nw


def predict(network, x):
    WW = ["W1", "W2", "W3"]
    bb = ["b1", "b2", "b3"]
    # 设置网络层数
    layers = len(WW)
    # 初始化输入输出值
    output_value = input_value = x
    for i in range(layers):
        dot_value = np.dot(input_value, network[WW[i]]) + network[bb[i]]
        output_value = sigmoid(dot_value)
        input_value = output_value

    return output_value


if __name__ == '__main__':
    _, (x, t) = get_data()
    network = init_work()
    accuracy_cnt = 0
    length = len(x)
    for i in range(length):
        y = predict(network, x[i])
        p = np.argmax(y)
        if p == t[i]:
            accuracy_cnt += 1

    print(f"Accuracy:{float(accuracy_cnt)/length}")
