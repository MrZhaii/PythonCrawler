#-*- encoding: utf-8 -*-
'''
redis_demo.py
Created on 20-5-31 上午10:53
Copyright (c) 20-5-31, 海牛学院版权所有.
@author: 潘牛
'''
from download_page.util.redis_utill import RedisUtill


redis_util = RedisUtill()

dicts = {'exist:key1': 11, 'exist:key2': 22, 'exist:key3': 33}
redis_util.set_batch_datas(dicts)

# 计划要写入redis的key
keys = ['exist:key1','exist:key2','exist:key4']

keys_dict = {}
keys_dict['exist:key1'] = 11
keys_dict['exist:key2'] = 22
keys_dict['exist:key4'] = 44

values = redis_util.get_values_batch_keys(keys)

need_put_keys = []
need_dict = {}

for i in range(0, len(values)):
    if values[i] == None:
        need_key = keys[i].split(":")[1]
        need_dict[keys[i]] = keys_dict[keys[i]]
        need_dict["down:%s" % need_key] = keys_dict[keys[i]]

print need_dict


redis_util.set_batch_datas(need_dict)