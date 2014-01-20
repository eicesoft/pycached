#coding=utf-8

import os
import sys
import time
import functools
import logging.config
import threading
from json import load as json_decode
from tornado.options import options

__author__ = 'kelezyb'


def get_logger():
    """
    获得logger对象.
    """
    logging.config.fileConfig(options.logger_conf)

    return logging.getLogger()

logger = get_logger()    #


def load_config(configfile):
    """
    载入配置
    @param configfile 配置文件名(string)
    @return 配置数据(dict)
    """

    if os.path.exists(configfile):
        fd = file(configfile)
        configfile = json_decode(fd)
    else:
        sys.exit(1)  # 程序退出

    return configfile


def write_pid(pid):
    """
    写PID文件
    """
    fd = open(options.pid, 'w+')
    fd.write(str(pid))
    fd.close()


def start_daemon(pidfile):
    """
    守护进程模式运行
    @param pidfile: pid文件名
    """
    if os.path.exists(pidfile):
        try:
            os.unlink(pidfile)
        except OSError, ex:
            logger.warn("pid exists, unlink pid file[%s], %s" % (pidfile, ex))

    try:
        if os.fork() > 0:   # fork 1, kill main process
            sys.exit(0)
    except OSError, e:
        logger.debug("fork #1 failed: %d (%s)" % (e.errno, e.strerror))
        sys.exit(1)

    #    os.chdir("/") #网上的代码为啥都有这段??
    os.setsid()
    os.umask(0)

    try:
        pid = os.fork()
        if pid > 0:   # fork 2, kill sub process
            #create pid file
            write_pid(pid)

            sys.exit(0)
    except OSError, e:
        logger.debug("fork #2 failed: %d (%s)" % (e.errno, e.strerror))
        sys.exit(1)

    return pid


def timeit(func):
    @functools.wraps(func)
    def __do__(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        logger.info('%s used time %0.4f ms' % (func.__name__, (time.time() - start) * 1000))
        return result

    return __do__


def lock(func):
    @functools.wraps(func)
    def __do__(*args, **kwargs):
        mutex = threading.Lock()
        mutex.acquire()
        result = func(*args, **kwargs)
        mutex.release()
        return result

    return __do__