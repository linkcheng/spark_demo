#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zheng.long@shoufuyou.com
@module: nnetwork 
@date: 2018-12-23 
"""
import numpy as np


narray = np.array


def step_function(x):
    """阶跃函数"""
    return narray(x > 0, dtype=np.int)


def sigmoid(x):
    """激活函数"""
    return 1 / (1 + np.exp(-x))


def relu(x):
    """Rectified Linear Unit"""
    return np.maximum(0, x)


def identity_function(x):
    """恒等函数"""
    return x


def softmax(x):
    """反应概率"""
    c = np.max(x)
    exp_a = np.exp(x-c)  # 溢出对策
    sum_exp_a = np.sum(exp_a)
    y = exp_a / sum_exp_a
    return y


def init_network():
    """初始化权重 W 与偏置 b"""
    W1 = narray([[0.1, 0.3, 0.5], [0.2, 0.4, 0.6]])
    W2 = narray([[0.1, 0.4], [0.2, 0.5], [0.3, 0.6]])
    W3 = narray([[0.1, 0.3], [0.2, 0.4]])

    b1 = narray([0.1, 0.2, 0.3])
    b2 = narray([0.1, 0.2])
    b3 = narray([0.1, 0.2])

    return {
        'W1': W1,
        'W2': W2,
        'W3': W3,
        'b1': b1,
        'b2': b2,
        'b3': b3,
    }


def forward(network, x):
    """将输入转换为输出，正向传播"""
    W1, W2, W3 = network['W1'],  network['W2'],  network['W3']
    b1, b2, b3 = network['b1'],  network['b2'],  network['b3']

    a1 = np.dot(x, W1) + b1
    z1 = sigmoid(a1)

    a2 = np.dot(z1, W2) + b2
    z2 = sigmoid(a2)

    a3 = np.dot(z2, W3) + b3
    y = identity_function(a3)

    return y


def backward():
    """从输出到输入，反向传播"""
    pass


if __name__ == '__main__':
    network = init_network()
    x = narray([0.1, 0.5])
    y = forward(network, x)
    print(y)

