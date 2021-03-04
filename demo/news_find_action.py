#-*- encoding: utf-8 -*-
'''
news_find_action.py
Created on 21-2-19 下午2:40
Copyright (c) 21-2-19, 海牛学院版权所有.
@author: 潘牛
'''
from commons.action.base_producer_action import ProducerAction
from commons.action.base_consumer_action import ConsumerAction

class NewsFindProducerAction(ProducerAction):
    def queue_items(self):
        # 参考 hainiu_queue.py
        pass

class NewsFindConsumerAction(ConsumerAction):
    def action(self):
        #爬取 hainiu_queue 中符合要求的url 请求页面的所有 a标签url ，
        # 并解析存入内链表或外链表，在存入时，如果url已存在，只做
        # update 操作。（保证链接页面不会重复爬取）
        pass

    def success_action(self, values):
        #1）记录种子url最后爬取成功数， （用来校验最后的爬取是否成功）；

        #2）在hainiu_queue 表中删除已经爬取成功的url；
        pass

    def fail_action(self, values):
        #1）记录hainiu_queue表错误次数和ip；

        #2）当某个机器的错误次数达到了当前机器设定的最大重试次数，把hainiu_queue
        # 表对应的记录的 is_work = 0，让其他机器重试；

        #3）更新种子表的失败次数、失败ip；队列表的数据不删除，有可能是因为目标网站把ip给封了，
        # 在某个时间，写个脚本，把失败的队列数据改状态和失败次数和失败ip，重新爬取试试。
        pass
