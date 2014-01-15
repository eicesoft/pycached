#coding=utf-8

from tornado.ioloop import IOLoop
from tornado.options import options

__author__ = 'kelezyb'
__version__ = (0, 9, 0, 1)


def option_init():
    """
    程序参数初始化
    """
    options.define('logger_conf', 'logger.conf', basestring, help="PyCached Logger config file")
    options.define('config', 'config.json', basestring, help="PyCached Config file")
    options.define('pid', 'logger.pid', basestring, help="Pid file")
    options.parse_command_line()


def exp(info):
    print info


def main():
    """
    入口函数
    """
    from base import load_config
    from net import CacheServer
    from base import logger

    config = load_config(options.config)  # 载入配置

    server = CacheServer(config)

    try:
        server.bind(int(config['port']))
        server.start(1)
        logger.info("PyCached running is %d" % config['port'])
        IOLoop.instance().handle_callback_exception(exp)
        IOLoop.instance().start()
    except KeyboardInterrupt:
        server.memory.dump_db()
        pass
    except:
        server.memory.dump_db()
        logger.error("Server exception.", exc_info=True)
        IOLoop.instance().stop()


if __name__ == '__main__':
    option_init()  # 参数解析
    main()