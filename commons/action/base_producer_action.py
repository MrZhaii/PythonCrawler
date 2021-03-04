#-*- encoding: utf-8 -*-
'''
base_producer_action.py
Created on 21-1-30 下午2:30
Copyright (c) 21-1-30, 海牛学院版权所有.
@author: 潘牛
'''
class ProducerAction(object):
    '''
    生产动作基类，用于制定生产规则
    '''

    def queue_items(self):
        '''
        这是个空方法（相当于Java的抽象方法），需要子类重写实现具体的业务逻辑
        :return:
        '''
        pass