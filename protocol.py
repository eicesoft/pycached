#coding=utf-8

import struct
from base import logger

__author__ = 'kelezyb'

INT_LENGTH = 4

CMD_SET = 0
CMD_GET = 1
CMD_DELETE = 2
CMD_EXISTS = 3
CMD_EXPIRE = 4
CMD_PERSIST = 5
CMD_TTL = 6
CMD_RENAME = 7

CMD_LPUSH = 100
CMD_LPOP = 101
CMD_LRANGE = 102
CMD_LLEN = 103
CMD_LINDEX = 104
CMD_LINSERT = 105

CMD_SAVE = 9999


class Protocol:
    """
    连接处理类
    """
    CMD_MAPPING = {
        CMD_SET: '_cmd_set_handler',
        CMD_GET: '_cmd_get_handler',
        CMD_DELETE: '_cmd_delete_handler',
        CMD_EXISTS: '_cmd_exists_handler',
        CMD_EXPIRE: '_cmd_expire_handler',
        CMD_PERSIST: '_cmd_persist_handler',
        CMD_TTL: '_cmd_ttl_handler',
        CMD_RENAME: '_cmd_rename_handler',
        
        CMD_LPUSH: '_cmd_lpush_handler',
        CMD_LPOP: '_cmd_lpop_handler',
        CMD_LRANGE: '_cmd_lrange_handler',
        CMD_LLEN: '_cmd_llen_handler',
        CMD_LINDEX: '_cmd_lindex_handler',
        CMD_LINSERT: '_cmd_linsert_handler',
        
        CMD_SAVE: '_cmd_save_handler',
    }

    def __init__(self, buf, memory):
        self._pos = 0
        self._buf = buf

        self._memory = memory       # 引用服务器对象的Memory对象

    def parse(self):
        """
        解析数据, 执行操作命令
        """
        cmd_id = self.parse_int_val()

        if cmd_id in self.CMD_MAPPING:
            logger.info("Command is: %d:%s" % (cmd_id, self.CMD_MAPPING[cmd_id]))
            code, data = getattr(self, self.CMD_MAPPING[cmd_id])()
            #print code, data
            return code, data
        else:       # 未知命令ID
            logger.warn("Command unkonw: %d" % cmd_id)

            return None, None

    def _cmd_set_handler(self):
        key = self.parse_string_val()
        val = self.parse_string_val()
        expire = self.parse_int_val()
        flag = self.parse_int_val()
        self._memory.set(key, val, expire, flag)
        logger.debug("Set: %s => %s, %d, %d" % (key, val, expire, flag))
        
        return 1, None

    def _cmd_get_handler(self):
        key = self.parse_string_val()
        code, val = self._memory.get(key)
        logger.debug("Get: %s => %s, %s" % (key, val, code))

        return code, val

    def _cmd_delete_handler(self):
        key = self.parse_string_val()
        val = self._memory.delete(key)
        logger.debug("Delete: %s => %s" % (key, val))

        return val, None
    
    def _cmd_exists_handler(self):
        key = self.parse_string_val()
        code = self._memory.exists(key)
        logger.debug("Exists: %s => %s" % (key, code))

        return code, None

    def _cmd_expire_handler(self):
        key = self.parse_string_val()
        expire = self.parse_int_val()
        
        code, val = self._memory.expire(key, expire)
        
        logger.debug("Expire: %s => %s, %s" % (key, code, val))

        return code, val

    def _cmd_persist_handler(self):
        key = self.parse_string_val()
        code = self._memory.persist(key)
        
        return code, None

    def _cmd_ttl_handler(self):
        key = self.parse_string_val()
        code, val = self._memory.ttl(key)
        logger.debug("TTL: %s => %s, %s" % (key, code, val))

        return code, val
    
    def _cmd_rename_handler(self):
        key = self.parse_string_val()
        newkey = self.parse_string_val()
        
        code = self._memory.rename(key, newkey)
        logger.info("Rename: %s => %s, %s" % (key, newkey, code))

        return code, None
    
    def _cmd_save_handler(self):
        self._memory.dump_db()
        logger.debug("Save: Successs")

        return 1, None

    def _cmd_lpush_handler(self):
        key = self.parse_string_val()
        val = self.parse_string_val()

        code = self._memory.lpush(key, val)
        logger.debug("LPush: %s => %s:%d" % (key, val, code))

        return code, None

    def _cmd_lpop_handler(self):
        key = self.parse_string_val()

        code, val = self._memory.lpop(key)
        logger.debug("LPOP: %s => %s:%d" % (key, val, code))

        return code, val

    def _cmd_lrange_handler(self):
        key = self.parse_string_val()
        start = self.parse_int_val()
        end = self.parse_int_val()
        code, val = self._memory.lrange(key, start, end)
        logger.debug("LRange: %s, %s, %s" % (key, start, end))

        return code, val

    def _cmd_llen_handler(self):
        key = self.parse_string_val()
        code, val = self._memory.llen(key)
        logger.debug("LLen: %s => %s, %s" % (key, val, code))
        return code, val

    def _cmd_lindex_handler(self):
        key = self.parse_string_val()
        index = self.parse_int_val()
        code, val = self._memory.lindex(key, index)
        logger.debug("LIndex: %s => %s, %d, %s" % (key, val, index, code))
        return code, val

    def _cmd_linsert_handler(self):
        key = self.parse_string_val()
        index = self.parse_int_val()
        val = self.parse_string_val()

        code, val = self._memory.linsert(key, index, val)
        logger.debug("LInserts: %s => %s, %d, %s" % (key, val, index, code))
        return code, val

    def parse_string_val(self):
        """
        解析一段字符串数据
        """
        length = self.parse_int(self._buf[self._pos:self._pos + INT_LENGTH])

        self._pos += INT_LENGTH
        val = self._buf[self._pos:self._pos + length]

        self._pos += length
        return val

    def parse_int_val(self):
        """
        解析一个Int型数据
        """
        val = self.parse_int(self._buf[self._pos:self._pos + INT_LENGTH])
        self._pos += INT_LENGTH

        return val

    @staticmethod
    def parse_int(data):
        return struct.unpack('!i', data)[0]

    @staticmethod
    def build_int(code):
        return struct.pack('!i', code)