#-*- encoding: utf-8 -*-
'''
download_action.py
Created on 21-2-19 下午2:47
Copyright (c) 21-2-19, 海牛学院版权所有.
@author: 潘牛
'''
from commons.action.base_producer_action import ProducerAction
from commons.action.base_consumer_action import ConsumerAction
class DownloadProducerAction(ProducerAction):
    def queue_items(self):
        # 参考 hainiu_queue.py
        pass

class DownloadConsumerAction(ConsumerAction):
    def action(self):
        #1）把队列中的url的HTML内容下载到文件中，每个消费线程每隔5分钟生成一个新的文件。

        #2）如果保存成功，则存入hainiu_web_page表，在存入时，如果url已存在，只做 update 操作。
        pass

    def success_action(self, values):
        #1）在hainiu_queue 表中删除已经下载成功的url；

        #2）更新内链表的最后更新时间；
        pass

    def fail_action(self, values):
        #1）记录队列表错误次数和ip；

        #2）当某个机器的错误次数达到了当前机器设定的最大重试次数，把hainiu_queue 表对应的记录的
        #is_work = 0，让其他机器重试；

        #3）更新内链表的失败次数和失败ip，队列表的数据不删除；
        pass