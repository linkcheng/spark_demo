#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zheng.long@sfy.com
@module: demo 
@date: 9/26/18 
"""
import os
from time import sleep, strftime
from concurrent import futures


def display(*args):
    print(strftime('[%H:%M:%S]'), end=' ')
    print(*args)


def loiter(n):
    # n = 5-n
    msg = '{}loiter({}): doing nothing for {}s...'
    display(msg.format('\t'*n, n, n))
    sleep(n)
    msg = '{}loiter({}): done.'
    display(msg.format('\t'*n, n))
    return n * 10


def main():
    display('Script starting.')
    executor = futures.ThreadPoolExecutor(max_workers=3)
    results = executor.map(loiter, range(1, 6))

    # display('results:', results)
    display('Waiting for individual results:')

    for i, result in enumerate(results):
        display('result {}: {}'.format(i, result))


def batch_rename(path='tests/', key=None, word=''):
    _key = '【吾爱学习论坛www.52studying.com 针对各种考试的学习、交流和共享的平台】'
    if not key:
        key = _key

    files = os.listdir(path)
    for filename in files:
        origin_name = os.path.join(path, filename)
        print(origin_name)
        # 删除相同项
        new_name = os.path.join(path, filename.replace(key, word))
        # 添加相同项
        # new_name = os.path.join(path, key + filename)
        print(new_name)
        os.rename(origin_name, new_name)


if __name__ == '__main__':
    # main()
    # batch_rename('/Users/zhenglong/Downloads/许岑 keynote教程', ' ', '.')
    cpath = os.path.dirname(__file__)
    filename = 'ccc'
    full_name = os.path.join(cpath, filename)
    print(full_name)

