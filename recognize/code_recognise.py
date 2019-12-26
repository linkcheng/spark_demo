#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zheng.long@sfy.com
@module: code_recognise
@date: 2018-12-23 
"""
# import sys
# import os
# sys.path.append(os.pardir)

from PIL import Image
import numpy as np
from recognize.dataset.mnist import load_mnist


def img_show(img):
    """展示单张照片"""
    pil_img = Image.fromarray(np.uint8(img))
    pil_img.show()


def get_data(normalize=True, flatten=True, one_hot_label=False):
    """读取数据"""
    (x_train, t_train), (x_test, t_test) = load_mnist(normalize=normalize,
                                                      flatten=flatten,
                                                      one_hot_label=one_hot_label)
    # print(x_train.shape)
    # print(t_train.shape)
    # print(x_test.shape)
    # print(t_test.shape)
    return (x_train, t_train), (x_test, t_test)


def print_data(n):
    """展示训练集中前 n 张照片"""
    (x_train, t_train), (x_test, t_test) = get_data(normalize=False,
                                                    flatten=True)
    print(x_train.shape)  # 60000 张 28*28=784 像素的照片
    print(x_test.shape)
    for i in range(n):
        img = x_train[i]
        label = t_train[i]
        print(label)
        print(img.shape)
        # 改变图片性状，原来是一位数组，图片是二维的
        img = img.reshape(28, 28)
        print(img.shape)
        img_show(img)


if __name__ == '__main__':
    print_data(1)
