#coding=utf-8

import time
import copy
import multiprocessing
import leveldb
import msgpack

__author__ = 'kelezyb'

"""
抛弃的模块, 太消耗内存了
"""

class Storage(multiprocessing.Process):
    def __init__(self, dbname, caches, keys, delkeys):
        multiprocessing.Process.__init__(self)
        self.db = leveldb.LevelDB(dbname)
        self.caches = caches
        self.keys = keys
        self.delkeys = delkeys

    def key_process(self):
        keys = copy.deepcopy(self.keys)
        self.keys = []

        delkeys = copy.deepcopy(self.delkeys)
        self.delkeys = []

        return keys, delkeys

    @staticmethod
    def serialize(batch, key, val):
        #print 's=>%s' % key
        batch.Put(key, msgpack.packb(val))

    def start(self):
        keys, delkeys = self.key_process()

        batch = leveldb.WriteBatch()

        for key in set(keys):
            if key in self.caches and \
                    (self.caches[key][2] == 0 or time.time() < self.caches[key][2]):
                self.serialize(batch, key, self.caches[key])

        #[batch.Delete(key) for key in set(delkeys)]
        map(batch.Delete, set(delkeys))

        self.db.Write(batch, sync=False)
