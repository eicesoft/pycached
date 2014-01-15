#coding=utf-8

import struct
import socket
from json import dumps, loads

__author__ = 'kelezyb'


class PyCacheClient:
    def __init__(self, host, port):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((host, port))
        except socket.error as ex:
            print ex.message

    def set(self, key, val, expire=0, flag=0):
        sockpackage = self.build_set_package(key, val, expire, flag)
        self.sock.send(sockpackage)
        l = self.parse_int(self.sock.recv(4))
        data = self.sock.recv(l)

        return self.parse_int(data)

    def get(self, key):
        sockpackage = self.build_get_package(key)
        self.sock.send(sockpackage)
        data = self.sock.recv(4)
        l = self.parse_int(data)
        data = self.sock.recv(l)

        code = self.parse_int(data[0:4])

        if code != 0:
            return data[4:]
        else:
            return None

    def delete(self, key):
        sockpackage = self.build_delete_package(key)
        self.sock.send(sockpackage)
        l = self.parse_int(self.sock.recv(4))
        data = self.sock.recv(l)

        return self.parse_int(data)
        
    def save(self):
        sockpackage = self.build_sync_package()
        self.sock.send(sockpackage)
        l = self.parse_int(self.sock.recv(4))
        data = self.sock.recv(l)

        return self.parse_int(data)

    def lpush(self, key, val):
        sockpackage = self.build_lpush_package(key, val)
        self.sock.send(sockpackage)
        l = self.parse_int(self.sock.recv(4))
        data = self.sock.recv(l)

        return self.parse_int(data)

    def lpop(self, key):
        sockpackage = self.build_lpop_package(key)
        self.sock.send(sockpackage)
        data = self.sock.recv(4)
        l = self.parse_int(data)
        data = self.sock.recv(l)

        code = self.parse_int(data[0:4])
        if code != 0:
            return data[4:]
        else:
            return None

    def lrange(self, key, start=0, end=-1):
        sockpackage = self.build_lrange_package(key, start, end)
        self.sock.send(sockpackage)
        data = self.sock.recv(4)
        l = self.parse_int(data)
        data = self.sock.recv(l)

        code = self.parse_int(data[0:4])
        if code != 0:
            return loads(data[4:])
        else:
            return None

    @classmethod
    def build_set_package(cls, key, val, expire=0, flag=0):
        """
        header size: int
        command id: int
        key length: int
        key string: string
        val length: int
        val string: value
        expire: int
        flag: int
        """
        cmd = 0
        body = cls.build_int(cmd)
        body += cls.build_int(len(key))
        body += key
        body += cls.build_int(len(val))
        body += val
        body += cls.build_int(expire)
        body += cls.build_int(flag)

        package = cls.build_int(len(body))
        package += body

        return package

    @classmethod
    def build_get_package(cls, key):
        """
        header size: int
        command id: int
        key length: int
        key string: string
        """
        cmd = 1
        body = cls.build_int(cmd)
        body += cls.build_int(len(key))
        body += key

        package = cls.build_int(len(body))
        package += body

        return package

    @classmethod
    def build_delete_package(cls, key):
        """
        header size: int
        command id: int
        key length: int
        key string: string
        """
        cmd = 2
        body = cls.build_int(cmd)
        body += cls.build_int(len(key))
        body += key

        package = cls.build_int(len(body))
        package += body

        return package

    @classmethod
    def build_sync_package(cls):
        """
        header size: int
        command id: int
        key length: int
        key string: string
        """
        cmd = 3
        body = cls.build_int(cmd)

        package = cls.build_int(len(body))
        package += body

        return package

    @classmethod
    def build_lpush_package(cls, key, val):
        """
        header size: int
        command id: int
        key length: int
        key string: string
        val length: int
        val string: value
        expire: int
        flag: int
        """
        cmd = 4
        body = cls.build_int(cmd)
        body += cls.build_int(len(key))
        body += key
        body += cls.build_int(len(val))
        body += val

        package = cls.build_int(len(body))
        package += body
        #print package
        return package

    @classmethod
    def build_lpop_package(cls, key):
        """
        header size: int
        command id: int
        key length: int
        key string: string
        val length: int
        val string: value
        expire: int
        flag: int
        """
        cmd = 5
        body = cls.build_int(cmd)
        body += cls.build_int(len(key))
        body += key

        package = cls.build_int(len(body))
        package += body
        #print package
        return package

    @classmethod
    def build_lrange_package(cls, key, start, end):
        """
        header size: int
        command id: int
        key length: int
        key string: string
        val length: int
        val string: value
        expire: int
        flag: int
        """
        cmd = 6
        body = cls.build_int(cmd)
        body += cls.build_int(len(key))
        body += key
        body += cls.build_int(start)
        body += cls.build_int(end)
        package = cls.build_int(len(body))
        package += body

        return package

    @staticmethod
    def parse_int(data):
        return struct.unpack('!i', data)[0]

    @staticmethod
    def build_int(code):
        return struct.pack('!i', code)

    def close(self):
        self.sock.close()


import time
#import profile

pycache = PyCacheClient("127.0.0.1", 11311)


def test():
    for i in xrange(100):
        #print pycache.set("test_sdsdsdf_%d" % i, '+_+' * 20)
        #pycache.lpush('abc', 'i_%d' % i)
        #print pycache.get("test_sdsdsdf_%d" % i)
        #pycache.delete("test_sdsdsdf_%d" % i)
        pass

    print pycache.lrange('abc')
    print pycache.lpop('abc')
    print pycache.lrange('abc')

    print pycache.save()

start = time.time()
test()


print "run is %0.4f ms" % ((time.time() - start) * 1000)