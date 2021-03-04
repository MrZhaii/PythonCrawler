#-*- encoding: utf-8 -*-
'''
download_news_action.py
Created on 2021/2/22 17:07
@author: zhaizz
'''


import traceback,Queue,sys,json,time,os,shutil
sys.path.append('/home/hadoop/hainiu_crawler')
from commons.action.base_producer_action import ProducerAction
from commons.action.base_consumer_action import ConsumerAction
from commons.util.db_util import DBUtil
from configs.config import _HAINIU_DB, _DOWN_NEWS,_QUEUE_NEWS_FIND,_LOCAL_DATA_DIR
from commons.util.request_util import RequestUtil
from commons.util.util import Util
from commons.util.time_util import TimeUtil
from commons.action.producer import Producer
from commons.util.log_util import LogUtil
class DownloadNewsProducerAction(ProducerAction):
    def queue_items(self):
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
            sql_params = [2, 0, ip, _DOWN_NEWS['MAX_FAIL_TIMES'], _DOWN_NEWS['LIMIT_NUM']]
            # ({},{})
            res1 = db_util.read_dict(select_sql, sql_params)
            for row in res1:
                id = row['id']
                ids.append(str(id))
                act = row['action']
                params = row['params']
                c_action = DownloadNewsConsumerAction(id,act,params)
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

class DownloadNewsConsumerAction(ConsumerAction):

    def __init__(self,id,act,params):
        super(self.__class__, self).__init__()
        self.id=id
        self.act=act
        self.params=params

    def action(self):
        logger = LogUtil().get_logger("download_action", "download_action")
        #1）把队列中的url的HTML内容下载到文件中，每个消费线程每隔5分钟生成一个新的文件。
        r = RequestUtil()
        # hu = HtmlUtil()
        u = Util()
        db_util=DBUtil(_HAINIU_DB)
        time_util=TimeUtil()
        # 通过phandomjs 请求url，返回网页，包括网页的ajax请求
        html = r.http_get_phandomjs(self.act)
        # 拼接要写入的内容
        html=html.replace("\r","").replace("\n","\002")
        str1=self.act+"\001"+html
        str2=u.get_md5(str1)+"\001"+str1
        # 成功失败标记
        is_success=True
        # 获取时间
        # now_time====>年月日时分秒
        now_time=time.strftime("%Y%m%d,%H,%M,%S").split(",")
        day=now_time[0]
        hour=now_time[1]
        minute=int(now_time[2])
        for i in range(60,-5,-5):
            if minute < i:
                continue
            minute = i
            break

        minute = '0%s' % minute if minute < 10 else minute
        now_minute = '%s%s%s' % (day,hour,minute)

        file_names = os.listdir(_LOCAL_DATA_DIR%('tmp'))
        logger.info("file_names:%s"%file_names)
        thread_name = self.consumer_thread_name
        logger.info("thread_name:%s"%thread_name)
        last_file_name = ''
        for file_name in file_names:
            tmp = file_name.split("#")[0]
            if tmp == thread_name:
                last_file_name = file_name
                break

        now_file_name = "%s#%s" % (thread_name, now_minute)
        try:
            if last_file_name == '' or last_file_name != now_file_name:
                # 移动老文件
                # if last_file_name != '':
                oldPath=_LOCAL_DATA_DIR%("tmp/")+last_file_name
                logger.info("oldPath:%s"%oldPath)
                # if os.path.exists(oldPath) and os.path.getsize(oldPath) > 0:
                if last_file_name != '':
                    done_file_name=last_file_name+"#"+str(TimeUtil().get_timestamp())
                    logger.info("last_file_name:%s"%last_file_name)
                    newPath=_LOCAL_DATA_DIR%("done/")+done_file_name
                    logger.info("newPath:%s"%newPath)
                    shutil.move(oldPath,newPath)
                # 写入新文件
                now_file_name=_LOCAL_DATA_DIR%("tmp/")+now_file_name
                # if not os.path.exists(_LOCAL_DATA_DIR+'tmp2/'):
                #     os.mkdir(_LOCAL_DATA_DIR+'tmp2/')

                logger.info("now_file_name:%s"%now_file_name)
                f = open(now_file_name, 'a+')
                f.write(str2)
                f.close()
            else:
                last_file_name=_LOCAL_DATA_DIR%("tmp/")+last_file_name
                logger.info("last_file_name:%s"%last_file_name)
                # 写入老文件时进行换行
                insert_str="\n"+str2
                f = open(last_file_name, 'a+')
                f.write(insert_str)
                f.close()
        except Exception,e:
            is_success=False
            traceback.print_exc(e)
        #2）如果保存成功，则存入hainiu_web_page表，在存入时，如果url已存在，只做 update 操作。
        # 插入hainiu_web_page的参数列表
        sql_params=[]

        sql_params.append(self.act)
        # 获取hainiu_queue中param字典
        download_news_queue_param=json.loads(self.params)
        sql_params.append(download_news_queue_param['a_md5'])
        sql_params.append(self.params)
        sql_params.append(download_news_queue_param['domain'])
        sql_params.append(download_news_queue_param['a_host'])
        sql_params.append(download_news_queue_param['a_title'])

        # 获取年月日
        create_day = int(time_util.now_day(format='%Y%m%d'))
        create_time = time_util.get_timestamp()
        # 获取小时
        create_hour = int(time_util.now_hour())
        update_time = create_time

        sql_params.append(create_time)
        sql_params.append(create_day)
        sql_params.append(create_hour)
        sql_params.append(update_time)
        try:
            if is_success:
                sql="""
                insert into hainiu_web_page(url,md5,param,domain,host,title,
                create_time,create_day,create_hour,update_time,status)VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE update_time=values(update_time)
                """
                sql_params.append(1)
                db_util.execute(sql,sql_params)
            else:
                sql="""
                insert into hainiu_web_page(url,md5,param,domain,host,title,
                create_time,create_day,create_hour,update_time,
                fail_ip,status)VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE fail_times=fail_times+1,fail_ip=values(fail_ip)
                """
                sql_params.append(u.get_local_ip())
                sql_params.append(2)
                db_util.execute(sql,sql_params)
        except:
            is_success = False
            db_util.rollback()
            db_util.commit()
        finally:
            db_util.close()
        a_md5=download_news_queue_param['a_md5']
        md5=download_news_queue_param['md5']
        return self.result(is_success,self.id,a_md5,md5)

    def success_action(self, values):
        db_util=DBUtil(_HAINIU_DB)
        time_util=TimeUtil()
        #1）在hainiu_queue 表中删除已经下载成功的url；
        queue_delete_sql="""
        delete from hainiu_queue where id=%s
        """
        #2）更新内链表的最后更新时间；
        inner_update_sql="""
        update hainiu_web_seed_internally set update_time= %s where a_md5=%s AND
        md5=%s
        """
        update_time=time_util.get_timestamp()
        try:
            sql_param=[values[0]]
            db_util.execute_no_commit(queue_delete_sql,sql_param)
            sql_param=[update_time,values[1],values[2]]
            db_util.execute_no_commit(inner_update_sql,sql_param)
            db_util.commit()
        except Exception,e:
            traceback.print_exc(e)
            db_util.rollback()
        finally:
            db_util.close()

    def fail_action(self, values):
        ip = Util().get_local_ip()
        db_util=DBUtil(_HAINIU_DB)
        #1）记录队列表错误次数和ip；
        queue_update_sql1="""
        update hainiu_queue set fail_times=fail_times+1,fail_ip=%s where id=%s;
        """
        #2）当某个机器的错误次数达到了当前机器设定的最大重试次数，把hainiu_queue 表对应的记录的
        #is_work = 0，让其他机器重试；
        queue_update_sql2="""
        update hainiu_queue set is_work=0 where id=%s;
        """
        #3）更新内链表的失败次数和失败ip，队列表的数据不删除；
        inner_update_sql="""
        update hainiu_web_seed_internally set  fail_times=fail_times+1,fail_ip=%s where md5=%s and a_md5=%s
        """
        try:
            # 1)
            sql_params=[ip,values[0]]
            db_util.execute_no_commit(queue_update_sql1,sql_params)
            # 2)
            # 比较失败次数
            if self.current_retry_num == _QUEUE_NEWS_FIND['C_RETRY_TIMES']-1:
                sql_params = [self.id]
                db_util.execute_no_commit(queue_update_sql2, sql_params)
            sql_params=[ip,values[1],values[2]]
            db_util.execute_no_commit(inner_update_sql,sql_params)
            db_util.commit()
        except Exception,e:
            db_util.rollback()
            traceback.print_exc(e)
        finally:
            db_util.close()
if __name__ == '__main__':

    reload(sys)   #Python2.5 初始化后删除了 sys.setdefaultencoding 方法，我们需要重新载入
    sys.setdefaultencoding("utf-8")

    queue = Queue.Queue()
    p_action = DownloadNewsProducerAction()
    # _QUEUE_NEWS_FIND = {'NAME':'apple', 'P_SLEEP_TIME': 5, 'C_MAX_NUM': 1, 'C_MAX_SLEEP_TIME': 3, 'C_RETRY_TIMES':3}

    producer = Producer(queue,
                        _DOWN_NEWS["NAME"],
                        p_action,
                        _DOWN_NEWS["P_SLEEP_TIME"],
                        _DOWN_NEWS["C_MAX_NUM"],
                        _DOWN_NEWS["C_MAX_SLEEP_TIME"],
                        _DOWN_NEWS["C_RETRY_TIMES"]
                        )

    producer.start_work()