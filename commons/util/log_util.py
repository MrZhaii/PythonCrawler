#-*- encoding: utf-8 -*-
'''
log_util.py
Created on 2018/12/27 20:35
Copyright (c) 2018/12/27, 海牛学院版权所有.
@author: 潘牛
'''
import sys
sys.path.append('/home/hadoop/hainiu_crawler')
from logging.handlers import TimedRotatingFileHandler
import logging

from configs import config
import content


class LogUtil:

    base_logger = content._NULL_STR

    log_dict = {}

    def get_base_logger(self):
        # 单例
        if LogUtil.base_logger == content._NULL_STR:
            LogUtil.base_logger = self.__get_logger('info','info')
        return LogUtil.base_logger

    def get_logger(self,log_name,file_name):
        # 利用字典的key，来作为单例，只要是相同的key，就创建一样的对象
        # 也是一种单例
        key = log_name + file_name
        if not LogUtil.log_dict.has_key(key):
            LogUtil.log_dict[key] = self.__get_logger(log_name,file_name)

        return LogUtil.log_dict[key]

    def __get_new_logger(self,log_name,file_name):
        l = LogUtil()
        l.__get_logger(log_name,file_name)
        return l

    def __get_logger(self,log_name,file_name):
        # 创建logger对象
        self.logger = logging.getLogger(log_name)
        self.logger.setLevel(logging.INFO)

        # 创建按照时间滚动文件的handler
        fh = TimedRotatingFileHandler(config._LOG_DIR % (file_name),'D')
        fh.setLevel(logging.INFO)

        # 创建控制台handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        # 设置日志格式
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
        return self

    def info(self,msg):
        self.logger.info(msg)
        self.logger.handlers[0].flush()

    def error(self,msg):
        self.logger.error(msg)
        self.logger.handlers[0].flush()

    def exception(self,msg='Exception Logged'):
        self.logger.exception(msg)
        self.logger.handlers[0].flush()


if __name__ == '__main__':
    import time
    while True:
        logger = LogUtil().get_base_logger()
        logger.info("111")
        logger.error("222")
        try:
            1/0
        except:
            logger.exception()
        time.sleep(1)



