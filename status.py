#coding=utf-8

"""
服务器状态定义类
"""

from json import dumps

__author__ = 'kelezyb'


class Status(object):
    def __init__(self, keys):
        self._status = {}
        for key in keys:
            self._status[key] = 0
    
    def set(self, key, val):
        self._status[key] = val

    def get(self, key):
        return self._status[key]

    def inc(self, key, val=1):
        self._status[key] += val

    def dec(self, key, val=1):
        self._status[key] -= val

    def get_status(self):
        return self._status

    def __repr__(self):
        return dumps(self._status, sort_keys=True ,indent=4)