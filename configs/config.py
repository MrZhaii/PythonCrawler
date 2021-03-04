#-*- encoding: utf-8 -*-
'''
configs.py
Created on 2019/1/4 9:29
Copyright (c) 2019/1/4, 海牛学院版权所有.
@author: 潘牛
'''
import sys
sys.path.append('/home/hadoop/hainiu_crawler')
#日志地址
# _LOG_DIR = '/tmp/python/hainiu_crawler/log/%s'
_LOG_DIR = '/home/hadoop/python/hainiu_crawler/log/%s'

#数据地址
# _LOCAL_DATA_DIR = '/tmp/python/hainiu_crawler/data/%s'
_LOCAL_DATA_DIR = '/home/hadoop/python/hainiu_crawler/data/%s'

#数据库配置_测试
# _HAINIU_DB = {'HOST':'localhost', 'USER':'root', 'PASSWD':'root', 'DB':'hainiu_test', 'CHARSET':'utf8', 'PORT':3306}
_HAINIU_DB = {'HOST':'192.168.245.41', 'USER':'root', 'PASSWD':'12345678', 'DB':'hainiu_test', 'CHARSET':'utf8', 'PORT':3306}

# NAME, P_SLEEP_TIME, C_MAX_NUM, C_MAX_SLEEP_TIME, C_RETRY_TIMES
_QUEUE_DEMO = {'NAME':'demo', 'P_SLEEP_TIME': 5, 'C_MAX_NUM': 1, 'C_MAX_SLEEP_TIME': 3, 'C_RETRY_TIMES':3}

# 苹果业务队列
_QUEUE_APPLE = {'NAME':'apple', 'P_SLEEP_TIME': 5, 'C_MAX_NUM': 1, 'C_MAX_SLEEP_TIME': 3, 'C_RETRY_TIMES':3}


_QUEUE_HAINIU = {'NAME':'hainiu', 'P_SLEEP_TIME': 3, 'C_MAX_NUM': 5,
                 'C_MAX_SLEEP_TIME': 1, 'C_RETRY_TIMES':3, 'MAX_FAIL_TIMES': 6,
                 'LIMIT_NUM': 5}
#报警电话
_ALERT_PHONE = '110'


_FIND_NEWS_CONFIG = {'NAME':'findnews', 'P_SLEEP_TIME': 3, 'C_MAX_NUM': 5,
                 'C_MAX_SLEEP_TIME': 1, 'C_RETRY_TIMES':3, 'MAX_FAIL_TIMES': 6,
                 'LIMIT_NUM': 10}

_DOWN_NEWS_CONFIG = {'NAME':'downloadnews', 'P_SLEEP_TIME': 3, 'C_MAX_NUM': 5,
                 'C_MAX_SLEEP_TIME': 1, 'C_RETRY_TIMES':3, 'MAX_FAIL_TIMES': 6,
                 'LIMIT_NUM': 10,'FILE_FLAG':'one'}

_QUEUE_NEWS_FIND = {'NAME':'find_news', 'P_SLEEP_TIME': 3, 'C_MAX_NUM': 5,
                 'C_MAX_SLEEP_TIME': 1, 'C_RETRY_TIMES':3, 'MAX_FAIL_TIMES': 6,
                 'LIMIT_NUM': 5}

_DOWN_NEWS = {'NAME':'download_news', 'P_SLEEP_TIME': 3, 'C_MAX_NUM': 5,
                 'C_MAX_SLEEP_TIME': 1, 'C_RETRY_TIMES':3, 'MAX_FAIL_TIMES': 6,
                 'LIMIT_NUM': 5}

# redis集群
ips = ['192.168.245.41', '192.168.245.42', '192.168.245.43']

port = '6379'