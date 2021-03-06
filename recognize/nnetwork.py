#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zheng.long@sfy.com
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


def mean_squared_error(y: narray, t: narray):
    """损失函数之均方误差
    :param y: 神经网络的输出，narray
    :param t: 监督数据（标签数据），narray
    :return: float
    """
    return 0.5 * np.sum((y-t)**2)


def cross_entropy_error(y: narray, t: narray):
    """损失函数之交叉熵误差
    :param y: 神经网络的输出，narray
    :param t: 监督数据（标签数据），narray
    :return: float
    :note: log(0) => -inf, 所以添加非常小的数 delta，防止出现 log(0) 的计算
    """
    delta = 1e-7
    if y.ndim == 1:
        t = t.reshape(1, t.size)
        y = y.reshape(1, y.size)

    batch_size = y.shape[0]
    # return -np.sum(t*np.log(y+delta)) / batch_size
    return -np.sum(np.log(y[np.arange(batch_size), t] + delta)) / batch_size


def numerical_diff(f, x):
    """数值微分-中心差分
    :param f:
    :param x:
    :return:
    """
    h = 1e-4
    return (f(x+h) - f(x-h)) / (2*h)


def numerical_gradient(f, x: narray):
    """梯度, 对 x 的各个元素求数值微分
    :param f: 函数
    :param x: np.array
    :return:
    """
    h = 1e-4
    grad = np.zeros_like(x)

    for i in range(x.size):
        tmp = x[i]
        # f(x+h)
        x[i] = tmp + h
        f1 = f(x)
        # f(x-h)
        x[i] = tmp - h
        f2 = f(x)

        grad[i] = (f1 - f2) / 2*h
        x[i] = tmp

    return grad


def gradient_descent(f, init_x, lr=0.01, step_num=100):
    """梯度下降法
    :param f: 进行最优化的函数
    :param init_x: 初始值
    :param lr: 学习率
    :param step_num: 梯度法重复的次数
    :return: 极小值或者最小值
    """
    x = init_x

    for i in range(step_num):
        grad = numerical_gradient(f, x)
        x -= lr * grad

    return x


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
    xx = narray([0.1, 0.5])
    yy = forward(network, xx)
    print(yy)
