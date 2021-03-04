#-*- encoding: utf-8 -*-
'''
download_news_queue.py
Created on 2021/2/21 21:18
@author: zhaizz
'''
import sys,redis
sys.path.append('/home/hadoop/hainiu_crawler')
import traceback,json,sys,time
from commons.util.db_util import DBUtil
from commons.util.log_util import LogUtil
from configs.config import _HAINIU_DB,ips,port
from download_page.util.redis_utill import RedisUtill
def scan_limit_to_queue_table(host, port, cursor, match, count, key_list):

    r = redis.Redis(host, port)
    rs = r.scan(cursor, match, count)
    next_num = rs[0]
    # print rs

    # 把查询出来key添加到key_list里
    for i in rs[1]:
        key_list.append(i)
    # 新游标是0代表当前机器已经遍历完一次，这是递归出口
    if next_num == 0:
        return None
    scan_limit_to_queue_table(host, port, next_num, match, count,key_list)
def put_inner_to_queue():
    redis_util=RedisUtill()
    '''

    '''
    page_show_num=10
    # 统计hainiu_queue 未处理的记录数
    select_queue_count_sql = """
    select count(*) from hainiu_queue where type=%s and is_work=0 and fail_times=0;
    """
    # 插入hainiu_queue表
    insert_queue_sql = """
    insert into hainiu_queue (type,action,params) values (%s, %s, %s);
    """

    logger = LogUtil().get_logger("download_news_queue","download_news_queue")
    db_util=DBUtil(_HAINIU_DB)
    db_util.execute_no_commit("set NAMES utf8mb4;")
    try:
        # 统计hainiu_queue 未处理的记录数
        sql_params=[2]
        res1=db_util.read_one(select_queue_count_sql,sql_params)
        queue_count=res1[0]
        start_time = time.time()
        if queue_count >= 5:
            logger.info("hainiu_queue 有 %d 条未处理的记录，不需要导入！" % queue_count)
            return None
        inner_count=0
        for ip in ips:
            key_list = []
            scan_limit_to_queue_table(ip, port, 0,'down:*', 20, key_list)

            inner_count=inner_count+len(key_list)
            # 根据key列表上Redis里获取value列表
            values=redis_util.get_values_batch_keys(key_list)
            # 导入hainiu_queue表
            insert_queue_record=[]
            for value in values:
                queue_param=json.loads(value)
                a_url=queue_param['a_url']
                insert_queue_record.append((2,a_url,value))

            db_util.executemany_no_commit(insert_queue_sql,insert_queue_record)
            db_util.commit()
            # 把导入表后的key列表从redis里删掉
            redis_util.delete_batch(key_list)



        end_time = time.time()
        run_time = end_time - start_time
        logger.info("本地导入 %d 条数据， 用时 %.2f 秒" % (inner_count, run_time))

    except Exception,e:
        traceback.print_exc(e)
        db_util.rollback()
    finally:
        db_util.close()
if __name__ == '__main__':
    # 下面的两行代码就是解决这个报错的
    # 报错的原因是 Unicode 转 str 时， 采用了默认的编码： ascii 码
    # ascii 码解释不了中文，所以报错了
    # UnicodeEncodeError: 'ascii' codec can't encode characters in
    # position 5-6: ordinal not in range(128)
    reload(sys)
    sys.setdefaultencoding('utf-8')

    put_inner_to_queue()
