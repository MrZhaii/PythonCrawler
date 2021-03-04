#-*- encoding: utf-8 -*-
'''
Created on 2018/12/27 20:20
Copyright (c) 2018/12/27, 海牛学院版权所有.
@author: 潘牛
'''
import sys
sys.path.append('/home/hadoop/hainiu_crawler')
from configs import config

import MySQLdb

class DBUtil:
    
    def __init__(self, db):
        self.db = MySQLdb.connect(host=db['HOST'], user=db['USER'], passwd=db['PASSWD'], db=db['DB'], charset=db['CHARSET'], port=db['PORT'])
        
    def read_one(self,sql, params = None):
        '''
        select a,b,c from table
        :return  (a,b,c)
        '''
        self.cursor = self.db.cursor()
        if params == None:
            self.cursor.execute(sql)
        else:
            self.cursor.execute(sql, params)
        return self.cursor.fetchone()
        
    def read_tuple(self, sql, params = None):
        """execute sql return tuple
        select a,b,c from table
        ((a,b,c),(a,b,c))
        """
        self.cursor = self.db.cursor()
        if params == None:
            self.cursor.execute(sql)
        else:
            self.cursor.execute(sql, params)
        return self.cursor.fetchall()
    
    def read_dict(self, sql, params = None):
        """execute sql return dict
        select a,b,c from table
        ({a:1,b:2,c:33},{a:1,b:3,c:45})
        """
        self.cursor = self.db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        if params == None:
            self.cursor.execute(sql)
        else:
            self.cursor.execute(sql, params)
        return self.cursor.fetchall()
    
    def executemany(self,sql, params):
        '''
        insert into table (a,b,c) values(?,?,?)
        values   [(1,2,3),(324,6,1),(11,5,5)]
        :return:
        '''
        self.cursor = self.db.cursor()
        self.cursor.executemany(sql, params)
        self.db.commit()

    def executemany_no_commit(self,sql, params):
        self.cursor = self.db.cursor()
        self.cursor.executemany(sql, params)


    def execute(self,sql,params = None):
        '''
        执行SQL语句自动提交，防止SQL注入
        :param sql: SQL
        :param params: 参数
        :return:
        '''
        self.cursor = self.db.cursor()
        if params == None:
            self.cursor.execute(sql)
        else:
            self.cursor.execute(sql, params)
        self.db.commit()

    def execute_no_commit(self,sql, params=None):
        '''
        执行SQL语句不自动提交，防止SQL注入
        :param sql: SQL
        :param params: 参数
        :return:
        '''
        self.cursor = self.db.cursor()
        if params == None:
            self.cursor.execute(sql)
        else:
            self.cursor.execute(sql, params)

    def commit(self):
        self.db.commit()

    def close(self):
        """close db connect
        """
        self.cursor = self.db.cursor()
        self.cursor.close()
        self.db.close()
    
    def rollback(self):
        """rollback db connect
        """
        self.db.rollback()
        
    def rollback_close(self):
        """rollback and close db connect
        """
        self.db.rollback()
        self.db.close()        
    

