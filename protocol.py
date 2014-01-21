#coding=utf-8

import struct
from json import loads, dumps
from base import logger

__author__ = 'kelezyb'

INT_LENGTH = 4

# string command
CMD_SET = 0
CMD_GET = 1
CMD_DELETE = 2
CMD_EXISTS = 3
CMD_EXPIRE = 4
CMD_PERSIST = 5
CMD_TTL = 6
CMD_RENAME = 7

# list command
CMD_LPUSH = 100
CMD_LPOP = 101
CMD_LRANGE = 102
CMD_LLEN = 103
CMD_LINDEX = 104
CMD_LINSERT = 105

# hash command
CMD_HMSET = 200
CMD_HSET = 201
CMD_HGET = 202
CMD_HGETALL = 203
CMD_HEXISTS = 204
CMD_HLEN = 205
CMD_HDEL = 206
CMD_HKEYS = 207
CMD_HVALS = 208

# server command
CMD_STATUS = 9000
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
        
        CMD_HMSET: '_cmd_hmset_handler',
        CMD_HSET: '_cmd_hset_handler',
        CMD_HGET: '_cmd_hget_handler',
        CMD_HGETALL: '_cmd_hgetall_handler',
        CMD_HEXISTS: '_cmd_hexists_handler',
        CMD_HLEN: '_cmd_hlen_handler',
        CMD_HDEL: '_cmd_hdel_handler',
        CMD_HKEYS: '_cmd_hkeys_handler',
        CMD_HVALS: '_cmd_hvals_handler',

        CMD_SAVE: '_cmd_save_handler',
        CMD_STATUS: '_cmd_status_handler',
    }

    def __init__(self, buf, memory, status):
        self._pos = 0
        self._buf = buf

        self._memory = memory       # 引用服务器对象的Memory对象
        self._status = status

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
        logger.debug("Rename: %s => %s, %s" % (key, newkey, code))

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
    
    def _cmd_hmset_handler(self):
        key = self.parse_string_val()
        val = self.parse_string_val()
        values = loads(val)
        code, val = self._memory.hmset(key, values)
        
        logger.debug("HMSet: %s => %s, %s" % (key, code, val))
        return code, val
    
    def _cmd_hset_handler(self):
        key = self.parse_string_val()
        field = self.parse_string_val()
        val = self.parse_string_val()
        code, val = self._memory.hset(key, field, val)
        logger.debug("HSet: %s: %s => %s, %d" % (key, field, val, code))
        return code, val
    
    def _cmd_hget_handler(self):
        key = self.parse_string_val()
        fields = loads(self.parse_string_val())
        code, val = self._memory.hget(key, fields)
        
        logger.debug("HGet: %s => %s, %s" % (key, fields, code))

        return code, dumps(val)
    
    def _cmd_hgetall_handler(self):
        key = self.parse_string_val()
        code, val = self._memory.hgetall(key)
        logger.debug("HGetall: %s => %s" % (key, code))

        return code, dumps(val)

    def _cmd_hexists_handler(self):
        key = self.parse_string_val()
        field = self.parse_string_val()
        code, val = self._memory.hexists(key, field)
        logger.debug("HExists: %s => %s, %s" % (key, code, val))

        return code, val

    def _cmd_hlen_handler(self):
        key = self.parse_string_val()
        code, val = self._memory.hlen(key)
        logger.debug("HLen: %s => %s, %s" % (key, code, val))

        return code, val

    def _cmd_hdel_handler(self):
        key = self.parse_string_val()
        fields = loads(self.parse_string_val())
        code, val = self._memory.hdel(key, fields)
        logger.debug("HDel: %s => %s, %s" % (key, code, val))
        return code, val

    def _cmd_hkeys_handler(self):
        key = self.parse_string_val()
        code, val = self._memory.hkeys(key)
        logger.debug("HLen: %s => %s, %s" % (key, code, val))

        return code, val

    def _cmd_hvals_handler(self):
        key = self.parse_string_val()
        code, val = self._memory.hvals(key)
        logger.debug("HVALS: %s => %s, %s" % (key, code, val))

        return code, val

    def _cmd_status_handler(self):
        logger.debug("Status: Get")
        server_status = self._status.get_status()
        memory_status = self._memory.get_status()
        ret = {}
        ret.update(server_status)
        ret.update(memory_status)
        return 1, dumps(ret, indent=4, sort_keys=True)

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