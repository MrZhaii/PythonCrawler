#-*- encoding: utf-8 -*-
'''
log_demo.py
Created on 21-1-30 上午11:23
Copyright (c) 21-1-30, 海牛学院版权所有.
@author: 潘牛
'''
from commons.util.log_util import LogUtil

logger1 = LogUtil().get_logger("log_name", "log_file")


logger2 = LogUtil().get_logger("log_name", "log_file")

# 两个对象指向同一内存地址
print logger1 is logger2

logger1.info("测试 info 级别")
logger1.error("测试 error 级别")

try:
    1/0
except Exception , e:
    logger1.exception(e)