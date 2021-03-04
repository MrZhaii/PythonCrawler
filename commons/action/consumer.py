#-*- encoding: utf-8 -*-
'''
consumer.py
Created on 21-1-30 下午3:28
Copyright (c) 21-1-30, 海牛学院版权所有.
@author: 潘牛
'''
import threading,random,time
import sys
sys.path.append('/home/hadoop/hainiu_crawler')
from commons.util.log_util import LogUtil
from commons.action.base_consumer_action import ConsumerAction

class Consumer(threading.Thread):
    '''
    定义消费线程类
    '''

    def __init__(self, queue, thread_name, max_sleep_time, max_retry_num):
        '''
        初始化数据
        :param queue:          Queue对象，从该对象中获取要消费的对象
        :param thread_name:    线程名称，在线程中打印日志
        :param max_sleep_time: 消费完后到下次消费时的休眠间隔时间
        :param max_retry_num:  每个ConsumerAction对象实例如果消费失败了，可以重试，
                               配置的最大重试次数
        '''

        # 1）主动调用父类的__init__()
        super(self.__class__,self).__init__()

        # 2) 初始化参数
        self.queue = queue
        self.thread_name = thread_name
        self.max_sleep_time = max_sleep_time
        self.max_retry_num = max_retry_num

        # 3）初始化日志对象
        self.logger = LogUtil().get_logger(thread_name, thread_name)

    def run(self):
        self.logger.info('%s thread running ...' % self.thread_name)
        while True:
            try:
                # 计算随机休眠时间
                random_sleep_time = round(random.uniform(0.5, self.max_sleep_time),2)

                # 1) 从队列里取出c_ation
                c_action = self.queue.get()
                self.queue.task_done()

                # 校验c_action 的有效性
                if not isinstance(c_action, ConsumerAction):
                    raise Exception("%s is not ConsumerAction instance!" % c_action)
                # print self.thread_name
                c_action.consumer_thread_name = self.thread_name

                # print c_action.consumer_thread_name
                start_time = time.time()

                # 2）调用c_action.action() 执行消费，并返回结果
                result_list = c_action.action()

                end_time = time.time()
                run_time = end_time - start_time

                is_success = result_list[0]

                self.logger.info('thread.name=【%s】, run_time=%.2f s, sleep_time=%.2f s, retry_times=%d, '
                                 'result=%s, detail=%s' %
                                 (self.thread_name,
                                  run_time,
                                  random_sleep_time,
                                  c_action.current_retry_num + 1,
                                  'SUCCESS' if is_success else 'FAIL',
                                  result_list[1:] if len(result_list) > 1 else "null"))

                # 3）如果消费失败，需要重试
                # 重试的时机：当前c_action 的重试次数已经达到最大的重试次数
                # 因为是先消费，后判断，所以
                # c_action.current_retry_num < self.max_retry_num - 1
                if not is_success and c_action.current_retry_num < self.max_retry_num - 1:
                    # 当前c_action 重试次数+1
                    c_action.current_retry_num += 1
                    # 无条件还回队列
                    self.queue.put(c_action)

                # 4）随机休眠
                time.sleep(random_sleep_time)

            except Exception,e:
                self.logger.exception(e)
