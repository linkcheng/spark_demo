#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zheng.long@shoufuyou.com
@module: code_recognise
@date: 2018-12-23 
"""
# import sys
# import os
# sys.path.append(os.pardir)

from PIL import Image
import numpy as np
from dataset.mnist import load_mnist


def img_show(img):
    pil_img = Image.fromarray(np.uint8(img))
    pil_img.show()


def get_data(normalize, flatten):
    (x_train, t_train), (x_test, t_test) = load_mnist(normalize=normalize,
                                                      flatten=flatten)
    # print(x_train.shape)
    # print(t_train.shape)
    # print(x_test.shape)
    # print(t_test.shape)
    return (x_train, t_train), (x_test, t_test)


if __name__ == '__main__':
    (x_train, t_train), (x_test, t_test) = get_data(normalize=False, flatten=True)
    img = x_train[0]
    label = t_train[0]
    print(label)
    print(img.shape)
    img = img.reshape(28, 28)
    print(img.shape)
    img_show(img)
