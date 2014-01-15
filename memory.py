#coding=utf-8

import leveldb
import msgpack
import copy
from base import timeit
import threading

__author__ = 'kelezyb'

FIELD_KEY = 0
FIELD_VALUE = 1
FIELD_EXPIRE = 2
FIELD_FLAG = 3
FIELD_TYPE = 4

SRTING_TYPE = 0
LIST_TYPE = 1
HASH_TYPE = 2
SET_TYPE = 3


class Memory:
    def __init__(self, dbname):
        self.dbname = dbname        # 当前DB名称
        self.db = leveldb.LevelDB(dbname)

        self.caches = {}            # 缓存数据Hash

        self.keys = []              # 修改过的Key, 需要Dump数据
        self.delkeys = []           # 删除过的Key, 需要删除数据

        self._load_db()

    @timeit
    def _load_db(self):
        #print self.db.GetStats()

        self.caches = {key: self.unserialize(val) for (key, val) in self.db.RangeIter()}

    @staticmethod
    def unserialize(val):
        return tuple(msgpack.unpackb(val))

    @timeit
    def dump_db(self):
        mutex = threading.Lock()
        mutex.acquire()
        keys = copy.deepcopy(self.keys)
        delkeys = copy.deepcopy(self.delkeys)
        self.keys = []
        self.delkeys = []
        mutex.release()

        batch = leveldb.WriteBatch()

        [batch.Put(key, msgpack.packb(self.caches[key])) for key in set(keys) if key in self.caches]
        [batch.Delete(key) for key in set(delkeys)]

        self.db.Write(batch, sync=True)

    def set(self, key, val, expire, flag):
        self.keys.append(key)
        self.caches[key] = (key, val, expire, flag, SRTING_TYPE)

    def get(self, key):
        if key in self.caches:
            item = self.caches[key]
            data = item[FIELD_VALUE]
            if data is None:
                self.delete(key)    # 已过期
                return 0, None
            else:
                return 1, data
        else:
            return 0, None

    def delete(self, key):
        self.delkeys.append(key)
        if key in self.caches:
            del self.caches[key]

            return 1
        else:
            return 0

    def lpush(self, key, val):
        if key in self.caches:
            self.keys.append(key)
            if self.caches[key][FIELD_TYPE] == LIST_TYPE:
                self.caches[key][FIELD_VALUE].append(val)
                return 1
            else:
                return 0
        else:
            self.keys.append(key)
            self.caches[key] = (key, [val], 0, 0, LIST_TYPE)
            return 1

    def lpop(self, key):
        if key in self.caches:
            self.keys.append(key)
            if self.caches[key][FIELD_TYPE] == LIST_TYPE:
                try:
                    val = self.caches[key][FIELD_VALUE].pop()
                    return 1, val
                except IndexError:
                    return -1, None
            else:
                return 0, None
        else:
            return 0, None

    def lrange(self, key,  start, stop):
        if key in self.caches:
            if self.caches[key][FIELD_TYPE] == LIST_TYPE:
                if stop == -1:
                    return 1, self.caches[key][FIELD_VALUE][start:]
                else:
                    return 1, self.caches[key][FIELD_VALUE][start:stop]
            else:
                return 2, None
        else:
            return 0, None