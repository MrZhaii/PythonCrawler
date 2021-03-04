#-*- encoding: utf-8 -*-
'''
producer.py
Created on 21-1-30 下午3:37
Copyright (c) 21-1-30, 海牛学院版权所有.
@author: 潘牛
'''
import threading,traceback,time
import sys
sys.path.append('/home/hadoop/hainiu_crawler')
from commons.util.log_util import LogUtil
from commons.action.base_producer_action import ProducerAction
from commons.action.consumer import Consumer

class Producer(threading.Thread):

    def __init__(self, queue, queue_name, p_action, p_sleep_time, c_max_num, c_max_sleep_time, c_max_retry_num):
        '''
        初始化数据
        :param queue:             Queue对象，往该对象里放数据
        :param queue_name:        队列名称，每个业务有自己的队列， 可以通过队列名称区分业务
        :param p_action:          具体业务的ProducerAction对象
        :param p_sleep_time:      生产一次后，下次生产的休眠间隔时间
        :param c_max_num:         最大的消费线程数，初始化多少个消费线程取决于该值
        :param c_max_sleep_time:  消费者线程消费完后到下次消费时的休眠间隔时间
        :param c_max_retry_num:   每个ConsumerAction对象实例如果消费失败了，可以重试，
                                  配置的最大重试次数
        '''
        # 1）主动调用父类的__init__()
        super(self.__class__, self).__init__()

        # 2) 初始化参数
        self.queue = queue
        self.queue_name = queue_name
        self.p_action = p_action
        self.p_sleep_time = p_sleep_time
        self.c_max_num = c_max_num
        self.c_max_sleep_time = c_max_sleep_time
        self.c_max_try_num = c_max_retry_num

        # 3）校验p_action的有效性
        if not isinstance(p_action, ProducerAction):
            raise Exception("%s is not ProducerAction instance!" % p_action)


        # 4）初始化日志对象
        self.thread_name = '%s_producer' %  self.queue_name
        self.logger = LogUtil().get_logger(self.thread_name, self.thread_name)


    def run(self):
        '''
        生产线程运行逻辑
        '''
        self.logger.info('%s thread running ...' % self.thread_name)

        c_actions = []
        while True:
            try:

                # 获取start_time
                start_time = time.time()

                # 1）通过p_action.queue_items() 创建对应 ConsumerAction对象列表
                if len(c_actions) == 0:
                    c_actions = self.p_action.queue_items()

                total_num = len(c_actions)
                self.logger.info('thread.name=【%s】, current time produce %d actions' %
                                 (self.thread_name, total_num))

                # 2) 把产生的列表对象往队列里放
                while True:
                    if len(c_actions) == 0:
                        break

                    # 寻找往队列里放的契机
                    # 当前队列的未完成任务数 <= 消费线程数
                    if self.queue.unfinished_tasks <= self.c_max_num:
                        # 从列表里pop，pop一次长度-1
                        c_action = c_actions.pop()
                        # 往队列里放
                        self.queue.put(c_action)

                # 获取end_time
                end_time = time.time()
                run_time = end_time - start_time

                if int(run_time) == 0:
                    rate = total_num * 60 / 0.01
                else:
                    rate = int(total_num * 60 / run_time)

                self.logger.info('thread.name=【%s】, total_num=%d, produce %d actions/min, sleep_time=%d' %
                                 (self.thread_name, total_num, rate, self.p_sleep_time))

                # 3）休眠
                time.sleep(self.p_sleep_time)

            except Exception, e:
                traceback.print_exc(e)
                self.logger.exception(e)



    def start_work(self):
        '''
        初始化并启动消费线程和启动生产线程
        '''
        # 初始化并启动消费线程
        for i in range(1, self.c_max_num+1):
            # apple_consumer_1
            c_thread_name = '%s_consumer_%d' % (self.queue_name, i)

            self.logger.info("【%s】thread init and start" % c_thread_name)
            consumer = Consumer(self.queue,
                                c_thread_name,
                                self.c_max_sleep_time,
                                self.c_max_try_num)

            consumer.start()
        # 启动生产线程
        self.logger.info('【%s】 thread init and start' % self.thread_name)
        self.start()