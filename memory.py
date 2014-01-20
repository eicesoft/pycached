#coding=utf-8

import leveldb
import msgpack
import copy
import time
from base import timeit, lock

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
    """
    Cache内存管理类
    """
    def __init__(self, dbname):
        """
        构造函数
        @param dbname: 数据库地址
        """
        self.dbname = dbname        # 当前DB名称
        self.db = leveldb.LevelDB(dbname)

        self.caches = {}            # 缓存数据Hash

        self.keys = []              # 修改过的Key, 需要Dump数据
        self.delkeys = []           # 删除过的Key, 需要删除数据
        self.expirekeys = set()     # 可以过期的Key

        self.load_db()

    @timeit
    def load_db(self):
        """
        从数据库载入数据
        """
        print self.db.GetStats()
        for key, val in self.db.RangeIter():
            val = self.unserialize(val)
            if val[FIELD_EXPIRE] == 0 or time.time() < val[FIELD_EXPIRE]:
                self.caches[key] = val

    @timeit
    def dump_db(self):
        """
        写入K/V数据到数据库
        """
        keys, delkeys = self.key_process()

        batch = leveldb.WriteBatch()

        # 优化已过期的Key不再写入DB
        for key, val in self.filter_keys(keys):
            batch.Put(key, val)

        map(batch.Delete, set(delkeys))

        self.db.Write(batch, sync=False)
        #print self.caches

    def filter_keys(self, keys):
        """
        过滤失效的Key并序列化数据
        """
        for key in set(keys):
            if key in self.caches and \
                    (self.caches[key][FIELD_EXPIRE] == 0 or time.time() < self.caches[key][FIELD_EXPIRE]):
                yield (key, self.serialize(self.caches[key]))

    @staticmethod
    def unserialize(val):
        """
        反序列化数据
        """
        data = msgpack.unpackb(val)
        return data

    @staticmethod
    def serialize(val):
        """
        序列化数据
        """
        #print 's=>%s' % key
        data = msgpack.packb(val)
        return data

    @lock
    def key_process(self):
        """
        Key处理, 准备Dump
        """
        keys = copy.deepcopy(self.keys)
        self.keys = []

        delkeys = copy.deepcopy(self.delkeys)
        self.delkeys = []

        return keys, delkeys

    def set(self, key, val, expire, flag):
        self.keys.append(key)
        #print val
        if expire == 0:
            self.caches[key] = [key, val, expire, flag, SRTING_TYPE]
        else:
            self.caches[key] = [key, val, 0, flag, SRTING_TYPE]
            self.expire(key, expire)

    def get(self, key):
        if key in self.caches:
            if self.check_expire(key):
                return -1, None

            data = self.caches[key][FIELD_VALUE]

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

    def exists(self, key):
        if key in self.caches:
            return 1
        else:
            return 0

    def expire(self, key, expire):
        try:
            etime = int(time.time()) + expire
            self.caches[key][FIELD_EXPIRE] = etime                       # 过期时间
            self.expirekeys.add(key)                                     # 可以过期的Key标记一下
            
            return 1, etime
        except KeyError:
            return 0, None

    def persist(self, key):
        try:
            self.caches[key][FIELD_EXPIRE] = 0                           # 不过期
            return 1
        except KeyError:
            return 0

    def ttl(self, key):
        try:
            return 1, self.caches[key][FIELD_EXPIRE]
        except KeyError:
            return 0, None

    def rename(self, key, newkey):
        if key in self.caches:
            if newkey not in self.caches:
                self.caches[newkey] = self.caches[key]
                self.delete(key)
            
                return 1
            else:
                return 2
        else:
            return 0

    def check_expire(self, key):
        code, expire = self.ttl(key)
        if code == 1:
            if expire != 0 and time.time() > expire:    # 过期
                self.delete(key)                        # 过期的Key删除
                self.expirekeys.remove(key)             # 可以过期的key列表删除
                return True
            else:
                return False
        else:
            return False

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
            self.caches[key] = [key, [val], 0, 0, LIST_TYPE]
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

    def llen(self, key):
        if key in self.caches:
            if self.caches[key][FIELD_TYPE] == LIST_TYPE:
                return 1, len(self.caches[key][FIELD_VALUE])
            else:
                return 2, None
        else:
            return 0, None

    def lindex(self, key, index):
        if key in self.caches:
            if self.caches[key][FIELD_TYPE] == LIST_TYPE:
                try:
                    return 1, self.caches[key][FIELD_VALUE][index]
                except IndexError:
                    return 3, None
            else:
                return 2, None
        else:
            return 0, None

    def linsert(self, key, index, val):
        if key in self.caches:
            self.keys.append(key)
            if self.caches[key][FIELD_TYPE] == LIST_TYPE:
                self.caches[key][FIELD_VALUE].insert(index, val)
                return 1, None
            else:
                return 2, None
        else:
            return 0, None

    def hmset(self, key, values):
        self.keys.append(key)
        if key in self.caches:
            if self.caches[key][FIELD_TYPE] == HASH_TYPE:
                self.caches[key][FIELD_VALUE].update(values)

                return 1, None
            else:
                return 0, None
        else:
            self.caches[key] = [key, values, 0, 0, HASH_TYPE]
            return 1, None

    def hset(self, key, field, val):
        if key in self.caches:
            self.keys.append(key)
            if self.caches[key][FIELD_TYPE] == HASH_TYPE:
                self.caches[key][FIELD_VALUE][field] = val

                return 1, None
            else:
                return 2, None
        else:
            return 0, None

    def hget(self, key, fields):
        if key in self.caches:
            self.keys.append(key)
        else:
            return 0, None

