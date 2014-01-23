#coding=utf-8

"""
网络处理模块
包括网络连接的建立与消息数据的接收
"""

import types
import protocol
import msgpack
import shutil
from memory import Memory
from base import logger
from protocol import Protocol
from json import dumps
from status import Status
from tornado.tcpserver import TCPServer
from tornado.ioloop import PeriodicCallback
from gevent.threadpool import ThreadPool
from slave import PyCachedClient
from tornado.options import options

__author__ = 'kelezyb'


class CacheServer(TCPServer):
    """
    缓存服务器
    """
    status_fields = [
        'total_commands_processed',         # 0
        'connected_clients',                # 1
        'all_clients',                      # 2
        'total_connections_received',       # 3
        'total_connections_send',           # 4
        'slave_sync_rate',                  # 5
        'slave_sync_ok',                    # 6
    ]

    def __init__(self, config):
        self.config = config                                                    # 应用程序配置
        self.connections = 0                                                    # 连接客户端列表

        if self.config['savetime'] != 0:    # 不保存数据
            self.thread = PeriodicCallback(self.save_db,
                                       int(self.config['savetime']) * 1000)
            self.thread.start()                                                 # 背景保存服务线程启动

        self.workpool = ThreadPool(int(config['work_pool']))                    # 工作线程池

        self.status = Status(CacheServer.status_fields)                         # 服务器状态
        self.slave_clients = {}                                                 # 同步客户端
        self.is_sync = False                                                    # 是否在同步

        if self.config['master'] is not None:                                   # 从服务器启动
            shutil.rmtree(config['db'])
            self.master_server = PyCachedClient(self.config['master'][0],
                                                self.config['master'][1])
            self.master_server.sync(self.config['port'])
            self.slavepool = None
            self.slave = True                                                   # 是否为Slave模式
        else:                                                                   # 主服务器启动, 需要启动从命令工作线程

            self.slavepool = ThreadPool(int(config['slave_pool']))              # slave Command send pools
            self.slave = False                                                  # 是否为Slave模式

        self.memory = Memory(config['db'])                                      # 缓存服务类
        super(CacheServer, self).__init__()

    def handle_stream(self, stream, address):
        """
        有连接进入
        """
        if not self.is_sync:
            self.connections += 1
            ClientConnection(stream, self, address)
            self.status.inc(self.status_fields[1])
            self.status.inc(self.status_fields[2])
            logger.info("Client[%s] connection is success." % id(stream))
        else:
            logger.warn("Server sync mode don't connection")

    def close_stream(self, hashid):
        """
        客户端关闭连接
        """
        self.connections -= 0
        try:
            del self.slave_clients[hashid]
            logger.warn("Slave server[%s] leaving." % hashid)
        except KeyError:
            pass

        self.status.dec(self.status_fields[1])

        logger.info("Client[%s] connect is closed." % hashid)

    def save_db(self):
        """
        内存的数据保存到DB
        """
        self.memory.dump_db()
        
    def shutdown(self):
        """
        关闭服务器
        """
        logger.warn('PyCached server shutdown...')
        #gevent.wait()
        import os
        try:
            os.unlink(options.pid)
        except OSError:
            pass

        self.save_db()


class ClientConnection:
    """
    客户端连接处理
    """
    def __init__(self, stream, server, address):
        self._id = id(stream)
        self._stream = stream
        self._server = server
        self._address = address
        self._master = False

        self._stream.set_close_callback(self._close_handler)
        self._read_handler()

    def _read_handler(self):
        """
        解析数据头
        """
        if not self._stream.closed():
            try:
                self._stream.read_bytes(4, self._read_header_callback)
            except:
                logger.error("read stream error", exc_info=True)
        #else:
        #    print 'close'

    def _read_header_callback(self, buf):
        """
        读取数据包
        """
        self._server.status.inc(CacheServer.status_fields[3], len(buf))
        body_length = Protocol.parse_int(buf)
        logger.debug("header size: %d" % body_length)
        self._stream.read_bytes(body_length, self._read_body_callback)

    def _read_body_callback(self, buf):
        """
        分析数据包协议
        """
        self._server.workpool.spawn(self.protocol_process, buf)
        #self.protocol_process(buf)
        #self._workpool.spawn(self.protocol_process, buf)
        #print self._server.workpool.size
        self._read_handler()

    def protocol_process(self, buf):
        """
        协议命令处理
        """
        if not self._server.slave:
            self._server.memory.change_version()
        else:   #Slave 发送的数据会跟上一个last_cmd_time
            last_cmd_time = Protocol.parse_int(buf[-4:])
            buf = buf[:-4]
            if last_cmd_time != 0:
                self._server.memory.last_cmd_time = last_cmd_time

        self._server.status.inc(CacheServer.status_fields[0])

        ptc = Protocol(buf, self, self._server)
        cmd_id = ptc.parse_cmd_id()
        code, data = ptc.execute(cmd_id)
        self._send_client(code, data)
        if not self._server.slave and cmd_id in protocol.SLAVE_SYNC_SEND_CMDS:
            self._server.slavepool.spawn(self.slave_command_send, buf)
            #self.slave_command_send(buf)

    def slave_command_send(self, buf):
        """
        发送数据到从服务器
        """
        if self._server.slave_clients:
            logger.info("Slave Command send %d, %s" % (len(self._server.slave_clients), self._server.memory.last_cmd_time))
            for hashid, client in self._server.slave_clients.items():
                client.send_data(self._server.memory.last_cmd_time, buf)

    def add_slave(self, client):
        """
        添加从服务器到从服务器列表
        """
        self._server.slave_clients[self._id] = client
        logger.info('Add slave client[%s] success.' % self._id)

    def sync(self, port):
        """
        发送开始同步命令
        """
        self._server.slavepool.spawn(self.slave_sync_data, port)

    def sync_ok(self):
        """
        主从同步完成
        """
        self._server.status.set(CacheServer.status_fields[6], 1)

    def slave_sync_data(self, port):
        """
        同步数据
        """
        import time
        time.sleep(1)
        self._server.is_sync = True
        client = PyCachedClient(self._address[0], port)
        self.add_slave(client)

        logger.info('Slave[%s:%d] Sync data start' % (self._address[0], port))
        items = {}
        pos = 0
        MAX_SEND = 20000        # 同步数据多少Key为一组
        all_len = len(self._server.memory.caches.items())
        for key, val in self._server.memory.caches.items():
            pos += 1
            items[key] = val
            if pos % MAX_SEND == 0:
                client.sync_data(pos, all_len, msgpack.packb(items))
                items = {}

        if pos % MAX_SEND != 0:
            client.sync_data(pos, all_len, msgpack.packb(items))
        self._server.is_sync = False
        client.sync_ok()
        logger.info('Slave[%s:%d] Sync data success' % (self._address[0], port))

    def _close_handler(self):
        """
        客户端断开连接处理
        """
        self._server.close_stream(self._id)

    def _send_client(self, code, data):
        """
        发送数据包到客户端
        """
        if not self._stream.closed():
            pack = self._build_result(code, data)
            self._stream.write(pack)
            self._server.status.inc(CacheServer.status_fields[4], len(pack))

            logger.debug('Send to client data: %d bytes' % len(pack))

    @classmethod
    def _build_result(cls, code, data):
        """
        构造客户端返回包
        """

        body = Protocol.build_int(code)
        if data is not None:
            if code > 0 and len(str(data)) > 0:
                if isinstance(data, types.DictType) or isinstance(data, types.ListType):
                    body += dumps(data)
                else:
                    body += str(data)
        pack = Protocol.build_int(len(body))
        pack += body
        # print len(pack)
        return pack