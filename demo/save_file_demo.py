#-*- encoding: utf-8 -*-
'''
save_file_demo.py
Created on 20-5-24 下午4:23
Copyright (c) 20-5-24, 海牛学院版权所有.
@author: 潘牛
'''
import os

day = '20200528'
hour = '12'
minute = 6

for i in range(60,-5,-5):
    if minute < i:
        continue
    minute = i
    break

minute = '0%s' % minute if minute < 10 else minute
# print '%s%s%s' % (day,hour,minute)
now_minute = '%s%s%s' % (day,hour,minute)
print now_minute



file_names = os.listdir('/tmp/python/hainiu_crawler/data/tmp2')
thread_name = 'downloadnews_consumer_1'

last_file_name = ''
for file_name in file_names:
    tmp = file_name.split("#")[0]
    if tmp == thread_name:
        last_file_name = file_name
        break

now_file_name = "%s#%s" % (thread_name, now_minute)

print 'last_file_name:%s' % last_file_name
print 'now_file_name:%s' % now_file_name

if last_file_name == '' or last_file_name != now_file_name:
    print "写入新文件"
else:
    print "写入当前文件"