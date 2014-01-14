#coding=utf-8

from tornado.ioloop import IOLoop
from tornado.options import options

__author__ = 'kelezyb'


def option_init():
    """
    程序参数初始化
    """
    options.define('logger_conf', 'logger.conf', basestring, help="PyCached Logger config file")
    options.define('config', 'config.json', basestring, help="PyCached Config file")
    options.define('pid', 'logger.pid', basestring, help="Pid file")
    options.parse_command_line()


def main(config):
    from net import CacheServer
    from base import logger

    server = CacheServer(config)
    try:
        server.bind(int(config['port']))
        server.start(1)
        logger.info("PyCached running is %d" % config['port'])
        IOLoop.instance().start()
    except:
        server.memory._dump_db()
        logger.error("Server exception.", exc_info=True)
        IOLoop.instance().stop()


if __name__ == '__main__':
    option_init()  # 参数解析

    from base import load_config

    cfg = load_config(options.config)  # 载入配置
    main(cfg)