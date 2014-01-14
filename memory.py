#coding=utf-8

import time
import leveldb
import msgpack
from base import timeit

__author__ = 'kelezyb'

SRTING_TYPE = 0
LIST_TYPE = 1
HASH_TYPE = 2
SET_TYPE = 3


class Item:
    def __init__(self, key, val, expire, flag, tp=SRTING_TYPE):
        self.key = key
        self.val = val
        self.type = tp
        if expire != 0:
            self.expire = int(time.time()) + expire
        else:
            self.expire = 0

        self.flag = flag

    def get(self):
        if self.expire == 0:
            return self.val
        else:
            if time.time() <= self.expire:
                return self.val
            else:
                return None

    def serialize(self):
        return [self.key, self.val, self.expire, self.flag, self.type]

    @staticmethod
    def unserialize(data):
        #print data
        return Item(*data)


class Memory:
    def __init__(self, db):
        self.caches = {}
        self.keys = set()
        self.delkeys = []
        self.db = leveldb.LevelDB(db)
        self._load_db()

    @timeit
    def _load_db(self):
        for key, val in self.db.RangeIter():
            print key
            data = msgpack.unpackb(val)
            self.caches[key] = Item.unserialize(data)

    @timeit
    def _dump_db(self):
        batch = leveldb.WriteBatch()
        for key in self.keys:
            if key in self.caches:
                batch.Put(key, msgpack.packb(self.caches[key].serialize()))

        for key in set(self.delkeys):
            batch.Delete(key)

        self.db.Write(batch, sync=True)

    def set(self, key, val, expire, flag):
        self.keys.add(key)
        self.caches[key] = Item(key, val, expire, flag)

    def get(self, key):
        if key in self.caches:
            item = self.caches[key]
            data = item.get()
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
        if key in self.caches[key]:
            if self.caches[key].type == LIST_TYPE:
                self.caches[key].append(val)
            else:
                return -1
        else:
            self.caches[key] = [val]

    def lrange(self, key,  start, stop):
        if key in self.caches[key]:
            if self.caches[key].type == LIST_TYPE:
                return 1, self.caches[key][start:stop]
            else:
                return 2
        else:
            return 0

