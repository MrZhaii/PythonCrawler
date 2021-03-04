#-*- encoding: utf-8 -*-
'''
base_consumer_action.py
Created on 21-1-30 下午2:33
Copyright (c) 21-1-30, 海牛学院版权所有.
@author: 潘牛
'''
class ConsumerAction(object):
    '''
    消费动作基类，用于制定消费规则
    '''

    def __init__(self):
        # 初始化当前重试次数=0
        self.current_retry_num = 0
        # 初始化执行当前操作的线程名称
        self.consumer_thread_name = ''



    def action(self):
        '''
        这是个空方法（相当于Java的抽象方法），需要子类重写实现具体的业务逻辑
        :return:
        '''
        pass


    def result(self, is_success, *values):
        '''
        该方法是根据成功失败标记，来执行相应成功失败后的动作
        :param is_success: 成功失败标记
        :param values:     明细信息（是个元组）
        :return: [is_success, 明细信息]
        '''
        if is_success:
            self.success_action(values)
        else:
            self.fail_action(values)
        result_list = [is_success]
        for v in values:
            result_list.append(v)
        return result_list


    def success_action(self, values):
        '''
        这是个空方法，用来定义成功后动作规则
        如果子类不重写，就不执行
        :param values:
        :return:
        '''
        pass

    def fail_action(self, values):
        '''
        这是个空方法，用来定义失败后动作规则
        如果子类不重写，就不执行
        :param values:
        :return:
        '''
        pass