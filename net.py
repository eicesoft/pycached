#coding=utf-8

import types
from memory import Memory
from base import logger
from protocol import Protocol
from json import dumps
from tornado.tcpserver import TCPServer


__author__ = 'kelezyb'


class CacheServer(TCPServer):
    """
    缓存服务器
    """
    def __init__(self, config):
        self.config = config
        self.memory = Memory(config['db'])
        self.connections = {}
        super(CacheServer, self).__init__()

    def handle_stream(self, stream, address):
        """
        有连接进入
        """
        # logger.info("echo connecting: %s...", os.getpid())
        kid = id(stream)
        self.connections[kid] = stream
        ClientConnection(stream, self)


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
        body_length = Protocol.parse_int(buf)
        #logger.debug("header size: %d" % body_length)
        self._stream.read_bytes(body_length, self._read_body_callback)

    def _read_body_callback(self, buf):
        """
        分析数据包协议
        """
        protocol = Protocol(buf, self._server.memory)
        code, data = protocol.parse()
        self._send_client(code, data)
        self._read_handler()

    def _close_handler(self):
        """
        客户端断开连接处理
        """
        if self._id in self._server.connections:
            del self._server.connections[self._id]
            logger.debug("Client connection close is success.")
        else:
            logger.warn("Client connection close is warn.")

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

        return pack

    def _send_client(self, code, data):
        """
        发送数据包到客户端
        """
        pack = self._build_result(code, data)
        #print len(pack), pack
        self._stream.write(pack)
        logger.debug('Send to client data: %d bytes' % len(pack))
