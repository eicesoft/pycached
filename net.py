#coding=utf-8

import types
from memory import Memory
from base import logger
from protocol import Protocol
from json import dumps
from status import Status
from tornado.tcpserver import TCPServer
from tornado.ioloop import PeriodicCallback
from gevent.threadpool import ThreadPool

__author__ = 'kelezyb'


class CacheServer(TCPServer):
    """
    缓存服务器
    """
    status_fields = [
        'cmd_count',                #0
        'client_active_size',       #1
        'client_size',              #2
        'server_recv',              #3
        'server_send',              #4
    ]

    def __init__(self, config):
        self.config = config
        self.memory = Memory(config['db'])
        self.connections = {}
        self.thread = PeriodicCallback(self.save_db,
                                       int(self.config['savetime']) * 1000)
        self.thread.start()
        self.workpool = ThreadPool(int(config['pool']))
        self.status = Status(CacheServer.status_fields)
        super(CacheServer, self).__init__()

    def handle_stream(self, stream, address):
        """
        有连接进入
        """
        # logger.info("echo connecting: %s...", os.getpid())
        self.connections[id(stream)] = stream
        ClientConnection(stream, self)
        self.status.inc(self.status_fields[1])
        self.status.inc(self.status_fields[2])
        logger.info("Client connection is success.")

    def close_stream(self, id):
        if id in self.connections:
            del self.connections[id]

            self.status.dec(self.status_fields[1])
            logger.info("Client connect close is success.")
        else:
            logger.warn("Client connect close is warn.")

    def save_db(self):
        self.memory.dump_db()
        
    def shutdown(self):
        logger.warn('PyCached server shutdown...')
        #gevent.wait()
        self.save_db()


class ClientConnection:
    """
    客户端连接处理
    """
    def __init__(self, stream, server):
        self._id = id(stream)
        self._stream = stream
        self._server = server

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
        #logger.debug("header size: %d" % body_length)
        self._stream.read_bytes(body_length, self._read_body_callback)

    def _read_body_callback(self, buf):
        """
        分析数据包协议
        """
        #self._server.workpool.spawn(self.protocol_process, buf)
        self.protocol_process(buf)
        # self._workpool.spawn(self.protocol_process, buf)
        self._read_handler()

    def protocol_process(self, buf):
        self._server.status.inc(CacheServer.status_fields[0])
        protocol = Protocol(buf, self._server.memory, self._server.status)
        code, data = protocol.parse()
        self._send_client(code, data)

    def _close_handler(self):
        """
        客户端断开连接处理
        """
        self._server.close_stream(self._id)

    def _send_client(self, code, data):
        """
        发送数据包到客户端
        """
        pack = self._build_result(code, data)
        #print len(pack), pack
        #if self._stream.
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