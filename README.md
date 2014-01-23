# PyCached 0.9.0.1
========================================
###Author: Kelezyb
####Date: 2014/1/14

## Python Key/Value Cache Server

	背后的存储引擎为LevelDB, 载入速度和存储数据非常快. 而且产生的数据文件大小也很小
	定时存储数据, 保证数据的一致性和安全性
	支持String, List, Hash, Set类型的数据存储

* 0.9.0.3版本主要更新(2014/1/21):

		添加Slave服务器, 支持一主多从的模式来支撑读写分离

* 0.9.0.2版本主要更新(2014/1/18):

    	支持Hash数据结构
	    支持服务器状态查看

* 0.9.0.1版本主要更新(2014/1/16):

    	支持String, List
    	定时背景存储支持.

### 启动说明

    python pycached.py      #启动服务器
    python pycached.py --config=config_slave.json --daemon #启动服务器, 并以守护进程模式运行
    python pycached.py --help   #查看启动命令参数


### 配置说明:

    port => 11312                       启动端口号
    work_pool => 1000                   Net thread pool size
    slave_pool => 1000                  Slave thread pool size
    master => ["127.0.0.1", 11311]      主服务器地址, 设置为null则启动模式为主服务器, 否则为从服务器
    db => ./db2"                        数据文件位置
    savetime => 3600                    定时保存时间

### 依赖的Python库

    leveldb (0.19) --- 数据存储
    msgpack-python (0.3.0) --- 数据序列化
    gevent (1.0) --- Thread Pool
    tornado (3.1) --- Net Level
