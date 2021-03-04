#-*- encoding: utf-8 -*-
'''
news_find_action.py
Created on 2021/2/20 22:05
@author: zhaizz
'''

import traceback
import Queue
import sys,json
sys.path.append('/home/hadoop/hainiu_crawler')
from bs4 import BeautifulSoup
from commons.action.base_producer_action import ProducerAction
from commons.action.base_consumer_action import ConsumerAction
from commons.action.producer import Producer
from commons.util.db_util import DBUtil
from commons.util.request_util import RequestUtil
from commons.util.html_util import HtmlUtil
from commons.util.util import Util
from commons.util.time_util import TimeUtil
from configs.config import _HAINIU_DB, _QUEUE_NEWS_FIND
from download_page.util.redis_utill import RedisUtill

class NewsFindProducerAction(ProducerAction):
    def queue_items(self):
        '''
        通过悲观锁+事务+更新状态来实现多个机器串行拿取数据，
        并把其封装成HainiuConsumerAction对象实例列表返回
        '''
        select_sql = """
        select id,action,params
        from hainiu_queue where type=%s and is_work=%s and fail_ip!=%s and fail_times<%s limit %s for update;
        """


        # 更新SQL-拼字符串
        update_sql = """
        update hainiu_queue set is_work=1 where id in (%s);
        """
        c_actions = []
        # 用于装id，来更新
        ids = []
        db_util = DBUtil(_HAINIU_DB)
        try:
            # sql_params = [1, 0, _QUEUE_NEWS_FIND['MAX_FAIL_TIMES'], _QUEUE_NEWS_FIND['LIMIT_NUM']]
            # 屏蔽ip查询的参数
            ip = Util().get_local_ip()
            sql_params = [1, 0, ip, _QUEUE_NEWS_FIND['MAX_FAIL_TIMES'], _QUEUE_NEWS_FIND['LIMIT_NUM']]
            # ({},{})
            res1 = db_util.read_dict(select_sql, sql_params)
            for row in res1:
                id = row['id']
                ids.append(str(id))
                act = row['action']
                params = row['params']
                c_action = NewsFindConsumerAction(id,act,params)
                c_actions.append(c_action)

            if len(ids) > 0:
                db_util.execute_no_commit(update_sql % ",".join(ids))

            db_util.commit()
        except Exception,e:
            db_util.rollback()
            traceback.print_exc(e)
        finally:
            db_util.close()

        return c_actions

class NewsFindConsumerAction(ConsumerAction):

    def __init__(self,id,act,params):
        super(self.__class__, self).__init__()
        self.id=id
        self.act=act
        self.params=params

    def action(self):
        #爬取 hainiu_queue 中符合要求的url 请求页面的所有 a标签url
        r = RequestUtil()
        hu = HtmlUtil()
        u = Util()

        redis_util = RedisUtill()
        #
        is_success=True
        db_util=DBUtil(_HAINIU_DB)
        time_util=TimeUtil()
        # 内外链表的列表
        inner_list=[]
        exter_list=[]
        #获取种子的md5
        md5 = u.get_md5(self.act)

        # redis
        keys=[]
        keys_dict={}
        need_dict = {}
        try:
            # 通过phandomjs 请求url，返回网页，包括网页的ajax请求
            html = r.http_get_phandomjs(self.act)
            #可以从HTML或XML文件中提取数据的Python第三方库
            soup = BeautifulSoup(html, 'lxml')
            # a链接dom对象列表
            a_docs = soup.find_all("a")
            if len(a_docs) == 0:
                is_success = False
            aset = set()
            #获取种子的domain
            domain = hu.get_url_domain(self.act)
            #获取种子的host
            host = hu.get_url_host(self.act)


            # 时间（create_time、create_day、create_hour、update_time）
            # create_time=time_util.get_timestamp()
            #
            # create_day = int(time_util.now_day().replace('-', ''))
            # create_hour=int(time_util.now_hour())
            # update_time=create_time
            create_time = time_util.get_timestamp()
            # 获取年月日格式
            create_day = int(time_util.now_day(format='%Y%m%d'))
            # 获取小时
            create_hour = int(time_util.now_hour())
            update_time = create_time

            # params_json = json.dumps(self.params, ensure_ascii=False, encoding='utf-8')

            for a_doc in a_docs:
                #获取a标签的href
                a_href = hu.get_format_url(self.act,a_doc,host)
                #获取a标签的内容
                a_title = a_doc.get_text().strip()
                if a_href == '' or a_title == '':
                    continue
                if aset.__contains__(a_href):
                    continue
                aset.add(a_href)
                #获取a标签的host
                a_host = hu.get_url_host(a_href)

                #获取a标签href链接url的md5
                a_md5 = u.get_md5(a_href)

                #获取a标签所对应的xpath
                a_xpath = hu.get_dom_parent_xpath_js_new(a_doc)
                # 一行数据
                row_data=(self.act,md5,self.params,domain,host,
                          a_href,a_md5,a_host,a_xpath,a_title,create_time,
                          create_day,create_hour,update_time)
                if a_href.__contains__(domain):
                    # 计划写入的redis
                    keys.append("exist:%s"%u.get_md5(md5+a_md5))
                    # redis对应的值
                    # keys_dict["exist:%s"%u.get_md5(md5+a_md5)]=[self.act,self.params,domain,host,
                    #       a_href,a_host,a_xpath,a_title,create_time,
                    #       create_day,create_hour,update_time]
                    values_pict={"domain":domain,
                                 "a_url":a_href,"a_host":a_host,"a_title":a_title,
                                 "md5":md5,"a_md5":a_md5
                                 }
                    values_json=json.dumps(values_pict,ensure_ascii=False, encoding='utf-8')
                    keys_dict["exist:%s"%u.get_md5(md5+a_md5)]=values_json
                    inner_list.append(row_data)
                else:
                    exter_list.append(row_data)
            # 并解析存入内链表或外链表，在存入时，如果url已存在，只做
            # update 操作。（保证链接页面不会重复爬取）
            if len(inner_list)>0:
                inner_redis_list=[]
                values = redis_util.get_values_batch_keys(keys)
                for i in range(0, len(values)):
                    if values[i] == None:
                        inner_redis_list.append(inner_list[i])
                        need_key = keys[i].split(":")[1]
                        need_dict[keys[i]] = keys_dict[keys[i]]
                        need_dict["down:%s" % need_key] = keys_dict[keys[i]]

                redis_util.set_batch_datas(need_dict)
                inner_insert_sql="""
              insert into hainiu_web_seed_internally
              (url,md5,param,domain,host,a_url,a_md5,a_host,a_xpath,a_title,create_time,
              create_day,create_hour,update_time)
              values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
              ON DUPLICATE KEY UPDATE update_time=values(update_time);
            """
                db_util.executemany_no_commit(inner_insert_sql,inner_redis_list)
            if len(exter_list)>0:
                exter_insert_sql="""
              insert into hainiu_web_seed_externally
              (url,md5,param,domain,host,a_url,a_md5,a_host,a_xpath,a_title,create_time,
              create_day,create_hour,update_time)
              values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
              ON DUPLICATE KEY UPDATE update_time=values(update_time);
            """
                db_util.executemany_no_commit(exter_insert_sql,exter_list)
            db_util.commit()
        except Exception,e:
            is_success=False
            db_util.rollback()
            traceback.print_exc(e);
        finally:
            db_util.close()
            r.close_phandomjs()

        return self.result(is_success,self.id,len(inner_list),len(exter_list),md5)



    def success_action(self, values):
        #1）记录种子url最后爬取成功数， （用来校验最后的爬取是否成功）；
        #2）在hainiu_queue 表中删除已经爬取成功的url；
        seed_update_sql="""
        update hainiu_web_seed set last_crawl_internally=%s,last_crawl_externally=%s,last_crawl_time=now() where md5=%s;
        """
        queue_delete_sql="""
        delete from hainiu_queue where id=%s
        """
        db_util=DBUtil(_HAINIU_DB)
        try:
            sql_param=[values[1],values[2],values[3]]
            db_util.execute_no_commit(seed_update_sql,sql_param)
            sql_param=[values[0]]
            db_util.execute_no_commit(queue_delete_sql,sql_param)
            db_util.commit()
        except Exception,e:
            traceback.print_exc(e)
            db_util.rollback()
        finally:
            db_util.close()

    def fail_action(self, values):
        ip = Util().get_local_ip()
        db_util=DBUtil(_HAINIU_DB)
        #1）记录hainiu_queue表错误次数和ip；
        # is_success,self.id,len(inner_list),len(exter_list),md5
        queue_update_sql1="""
        update hainiu_queue set fail_times=fail_times+1,fail_ip=%s where id=%s;
        """
        #2）当某个机器的错误次数达到了当前机器设定的最大重试次数，把hainiu_queue
        # 表对应的记录的 is_work = 0，让其他机器重试；
        queue_update_sql2="""
        update hainiu_queue set is_work=0 where id=%s;
        """
        #3）更新种子表的失败次数、失败ip；队列表的数据不删除，有可能是因为目标网站把ip给封了，
        # 在某个时间，写个脚本，把失败的队列数据改状态和失败次数和失败ip，重新爬取试试。
        seed_update_sql="""
        update hainiu_web_seed set  fail_times=fail_times+1,fail_ip=%s where md5=%s
        """
        try:
            sql_params=[ip,values[0]]
            db_util.execute_no_commit(queue_update_sql1,sql_params)
            # 比较失败次数
            if self.current_retry_num == _QUEUE_NEWS_FIND['C_RETRY_TIMES']-1:
                sql_params = [self.id]
                db_util.execute_no_commit(queue_update_sql2, sql_params)
            sql_params=[ip,values[3]]
            db_util.execute_no_commit(seed_update_sql,sql_params)
            db_util.commit()
        except Exception,e:
            traceback.print_exc(e)
            db_util.rollback()
        finally:
            db_util.close()

if __name__ == '__main__':

    reload(sys)   #Python2.5 初始化后删除了 sys.setdefaultencoding 方法，我们需要重新载入
    sys.setdefaultencoding("utf-8")

    queue = Queue.Queue()
    p_action = NewsFindProducerAction()
    # _QUEUE_NEWS_FIND = {'NAME':'apple', 'P_SLEEP_TIME': 5, 'C_MAX_NUM': 1, 'C_MAX_SLEEP_TIME': 3, 'C_RETRY_TIMES':3}

    producer = Producer(queue,
                        _QUEUE_NEWS_FIND["NAME"],
                        p_action,
                        _QUEUE_NEWS_FIND["P_SLEEP_TIME"],
                        _QUEUE_NEWS_FIND["C_MAX_NUM"],
                        _QUEUE_NEWS_FIND["C_MAX_SLEEP_TIME"],
                        _QUEUE_NEWS_FIND["C_RETRY_TIMES"]
                        )

    producer.start_work()