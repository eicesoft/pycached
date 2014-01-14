#coding=utf-8

import struct
from memory import Memory
from base import logger
from tornado.tcpserver import TCPServer


__author__ = 'kelezyb'

INT_LENGTH = 4

CMD_SET = 0
CMD_GET = 1
CMD_DELETE = 2
CMD_SAVE = 3


class CacheServer(TCPServer):
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
    连接处理类
    """
    def __init__(self, stream, server):
        self._stream = stream
        self._id = id(stream)
        self._server = server
        self._pos = 0
        self._body_length = 0
        self._stream.set_close_callback(self._close_handler)
        self._read_handler()

    @staticmethod
    def _parse_int(data):
        return struct.unpack('!L', data)[0]

    @staticmethod
    def _build_int(code):
        return struct.pack('!L', code)

    def _read_handler(self):
        if not self._stream.closed():
            try:
                self._stream.read_bytes(4, self._read_header_callback)
            except:
                logger.error("read stream error", exc_info=True)
        #else:
        #    print 'close'

    def _read_header_callback(self, buf):
        self._body_length = self._parse_int(buf)
        logger.debug("header size: %d" % self._body_length)
        self._stream.read_bytes(self._body_length, self._read_body_callback)

    def _read_body_callback(self, buf):
        self._pos = 0
        cmd_id = self._parse_int(buf[self._pos:self._pos+INT_LENGTH])
        self._pos += INT_LENGTH
        logger.debug("Command is: %d" % cmd_id)

        if cmd_id == CMD_SET:
            self._cmd_set_handler(buf)
        elif cmd_id == CMD_GET:
            self._cmd_get_handler(buf)
        elif cmd_id == CMD_DELETE:
            self._cmd_delete_handler(buf)
        elif CMD_SAVE == CMD_SAVE:
            self._cmd_save_handler()

        self._read_handler()

    def _cmd_set_handler(self, buf):
        key_len = self._parse_int(buf[self._pos:self._pos + INT_LENGTH])
        self._pos += INT_LENGTH
        key = buf[self._pos:self._pos + key_len]
        self._pos += key_len
        val_len = self._parse_int(buf[self._pos:self._pos + INT_LENGTH])
        self._pos += INT_LENGTH
        val = buf[self._pos:self._pos + val_len]
        self._pos += val_len
        expire = self._parse_int(buf[self._pos:self._pos + INT_LENGTH])
        self._pos += INT_LENGTH
        flag = self._parse_int(buf[self._pos:self._pos + INT_LENGTH])
        self._pos += INT_LENGTH

        self._server.memory.set(key, val, expire, flag)
        logger.debug("%s => %s, %d, %d, %d" % (key, val, expire, flag, self._pos))
        self._send_client(1, '')

    def _cmd_get_handler(self, buf):
        key_len = self._parse_int(buf[self._pos:self._pos + INT_LENGTH])
        self._pos += INT_LENGTH
        key = buf[self._pos:self._pos + key_len]
        self._pos += key_len
        code, val = self._server.memory.get(key)
        #self._stream.write(val)
        logger.debug("%s => %s, %s" % (key, code, val))
        self._send_client(code, val)

    def _cmd_delete_handler(self, buf):
        key_len = self._parse_int(buf[self._pos:self._pos + INT_LENGTH])
        self._pos += INT_LENGTH
        key = buf[self._pos:self._pos + key_len]
        self._pos += key_len
        val = self._server.memory.delete(key)
        logger.debug("%s => %s" % (key, val))
        self._send_client(val, '')

    def _cmd_save_handler(self):
        self._server.memory._dump_db()
        #logger.error("save")

    def _build_result(self, code, data):
        body = self._build_int(code)
        if data is not None:
            l = len(data)
            if code > 0 and l > 0:
                body += data

        pack = self._build_int(len(body))
        pack += body

        return pack

    def _send_client(self, code, data):
        pack = self._build_result(code, data)
        self._stream.write(pack)

    def _close_handler(self):
        #print self._server.connections
        del self._server.connections[self._id]
        logger.debug("Client connection close.")
        #print self._server.connections