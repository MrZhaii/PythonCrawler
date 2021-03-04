#-*- encoding: utf-8 -*-
'''
db_test.py
Created on 2019/6/25 11:14
Copyright (c) 2019/6/25, 海牛学院版权所有.
@author: 潘牛
'''
from commons.util.db_util import DBUtil
from configs.config import _HAINIU_DB
db_util = DBUtil(_HAINIU_DB)

# 设置字符集是utf8mb4
db_util.execute_no_commit("set NAMES utf8mb4;")

# 测试 execute(self,sql,params = None):
# sql = """
# insert into hainiu_queue (type,action,params) values (1, 'www.hainiubl.com', 'aa');
# """
# db_util.execute(sql)

# 字符串拼接（不推荐用）
# sql = """
# insert into hainiu_queue (type,action,params) values (%d, '%s', '%s');
# """ % (1, 'www.hainiubl.com', 'aa')
# db_util.execute(sql)


# -------------------------------------
# 测试 execute(self,sql,params != None):
# sql占位符（推荐用法）
# sql = """
# insert into hainiu_queue (type,action,params) values (%s, %s, %s);
# """
# params = [1, 'www.hainiubl.com', "a'a"]
# db_util.execute(sql, params)

# -------------------------------------
# 测试 executemany(self,sql, params):
# sql = """
# insert into hainiu_queue (type,action,params) values (%s, %s, %s);
# """
# # 一个列表里带有多个元组，每个元组代表一行数据
# params = [(1, 'www.hainiubl.com', "bb"), (1, 'www.hainiubl.com', "cc")]
# db_util.executemany(sql, params)

# -------------------------------------
# 测试查询read_one(self,sql, params = None):
# 查询一行记录用 read_one()
# sql = """
# select count(*) as  num from hainiu_queue where type=%s;
# """
# params = [1]
# # 返回元组，根据下标获取元素
# rs = db_util.read_one(sql, params)
# print rs[0]

# -------------------------------------
# 测试 read_dict(self, sql, params = None):
# sql = """
# select id, type, action, params from hainiu_queue where type=%s;
# """
# params = [1]
# rs = db_util.read_dict(sql, params)
# # 元组里面套字典({},{},{})
# # 一个字典代表一行数据
# for row in rs:
#     # print row
#     id = row["id"]
#     type = row["type"]
#     action = row["action"]
#     p = row["params"]
#     print "%s %s %s %s" % (id, type, action, p)

# -------------------------------------
# read_tuple(self, sql, params = None):
# 元组里面套元组((),(),())
# sql = """
# select id, type, action, params from hainiu_queue where type=%s;
# """
# params = [1]
# rs = db_util.read_tuple(sql, params)
# # 元组里面套元组((),(),())
# # 内部的一个元组代表一行数据
# for row in rs:
#     # print row
#     id = row[0]
#     type = row[1]
#     action = row[2]
#     p = row[3]
#     print "%s %s %s %s" % (id, type, action, p)

# -------------------------------------
# # 插入两次，两次在同一个事务里
import time
sql = """
insert into hainiu_queue (type,action,params) values (%s, %s, %s);
"""
try:
    params = [1, 'www.hainiubl.com', "ff"]
    db_util.execute_no_commit(sql, params)

    time.sleep(5)
    1/0
    params = [1, 'www.hainiubl.com', "gg"]
    db_util.execute_no_commit(sql, params)

    time.sleep(5)

    db_util.commit()

except Exception, e:
    print e
    db_util.rollback()
finally:
    db_util.close()


# -------------------------------------
# 悲观锁 + 事务





