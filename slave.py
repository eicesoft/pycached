#coding=utf-8

"""
Slave服务器客户端访问封装
"""

import socket
import struct
import protocol

__author__ = 'kelezyb'


class PyCachedClient(object):
    def __init__(self, host, port):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((host, port))
        except socket.error as ex:
            print ex

    def sync(self, port):
        body = self.build_int(protocol.CMD_SYNC)
        body += self.build_int(port)

        self.send_data(0, body)

    def sync_data(self, pos, length, datas):
        #print ">>>", datas
        body = self.build_int(protocol.CMD_RECV_SYNC)
        body += self.build_int(pos)
        body += self.build_int(length)
        body += self.build_string(datas)
        self.send_data(0, body)

    def sync_ok(self):
        body = self.build_int(protocol.CMD_SYNC_OK)

        self.send_data(0, body)

    def send_data(self, last_cmd_time, buf):
        try:
            buf += self.build_int(last_cmd_time)
            package = self.build_int(len(buf))
            package += buf
            #print len(package)
            self.sock.send(package)
            data = self.sock.recv(4)
            l = self.parse_int(data)
            data = self.sock.recv(l)
            return self.parse_int(data)
        except socket.error:
            pass

    @staticmethod
    def parse_int(data):
        return struct.unpack('!i', data)[0]

    @staticmethod
    def build_int(code):
        return struct.pack('!i', code)

    def build_string(self, string):
        data = self.build_int(len(string))
        data += string
        return data
