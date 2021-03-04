#-*- encoding: utf-8 -*-
'''
downliad_news_queue_redis_demo.py
Created on 2021/2/24 22:55
@author: zhaizz
'''
from download_page.util.redis_utill import RedisUtill
import redis,json
ips = ['192.168.245.41', '192.168.245.42', '192.168.245.43']
port = '6379'
def scan_limit_to_queue_table(host, port, cursor, match, count, key_list):

    r = redis.Redis(host, port)
    rs = r.scan(cursor, match, count)
    next_num = rs[0]
    # print rs

    # 把查询出来key添加到key_list里
    for i in rs[1]:
        print i
        key_list.append(i)
    # 新游标是0代表当前机器已经遍历完一次，这是递归出口
    if next_num == 0:
        return None
    scan_limit_to_queue_table(host, port, next_num, match, count,key_list)
redis_util=RedisUtill()
for ip in ips:
    key_list = []
    scan_limit_to_queue_table(ip, port, 0,'down:*', 20, key_list)
    # 根据key列表上Redis里获取value列表
    values=redis_util.get_values_batch_keys(key_list)
    # 导入hainiu_queue表
    for value in values:
        queue_param=json.loads(value)

    # 把导入表后的key列表从redis里删掉
    print '======'