#coding=utf-8

from tornado.ioloop import IOLoop
from tornado.options import options
import signal

__author__ = 'kelezyb'
__version__ = '0.9.0.1'

server = None       # 服务器对象


def option_init():
    """
    程序参数初始化
    """
    options.define('daemon', False, bool, help="PyCached daemon mode")
    options.define('logger_conf', 'logger.conf', basestring, help="PyCached Logger config file")
    options.define('config', 'config.json', basestring, help="PyCached Config file")
    options.define('pid', 'pycached.pid', basestring, help="Pid file")
    options.parse_command_line()


def exception_handler(err):
    """
    异常处理
    """
    from base import logger

    logger.error(err)


def signal_registry():
    """
    信号处理注册
    """
    signal.signal(signal.SIGTERM, signal_handler)
    #signal.signal(signal.SIGINT, signal_handler)


def signal_handler(sig, frame):
    """
    信号处理
    """
    global server
    #print server
    server.shutdown()
    IOLoop.instance().stop()


def server_start(config):
    """
    服务器启动
    """
    global server

    from net import CacheServer
    from base import logger

    server = CacheServer(config)

    try:
        server.bind(int(config['port']))
        server.start(1)
        logger.info("PyCached running is %d" % config['port'])
        IOLoop.instance().handle_callback_exception(exception_handler)
        IOLoop.instance().start()
    except KeyboardInterrupt:
        server.shutdown()
    except:
        server.shutdown()
        logger.error("Server exception.", exc_info=True)
        IOLoop.instance().stop()


def main():
    """
    入口函数
    """

    from base import start_daemon, write_pid
    from base import load_config

    signal_registry()
    config = load_config(options.config)  # 载入配置
    if options.daemon:
        start_daemon(options.pid)
        server_start(config)
    else:
        write_pid(options.pid)
        server_start(config)


if __name__ == '__main__':
    option_init()  # 参数解析
    main()