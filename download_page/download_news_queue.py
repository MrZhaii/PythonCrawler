#-*- encoding: utf-8 -*-
'''
download_news_queue.py
Created on 2021/2/21 21:18
@author: zhaizz
'''
import sys
sys.path.append('/home/hadoop/hainiu_crawler')
import traceback,json,sys,time
from commons.util.db_util import DBUtil
from commons.util.log_util import LogUtil
from configs.config import _HAINIU_DB
def put_inner_to_queue():
    '''

    '''
    page_show_num=10
    # 统计hainiu_queue 未处理的记录数
    select_queue_count_sql = """
    select count(*) from hainiu_queue where type=%s and is_work=0 and fail_times=0;
    """
    # 统计内链接表符合条件的总记录数
    select_inner_count_sql = """
    select count(*) from hainiu_web_seed_internally where status=0;
    """
    # 分页查询内链接表
    select_inner_limit_sql="""
    select md5,a_url,a_md5,domain,a_host,a_title from hainiu_web_seed_internally WHERE
    status=0 limit 0,%s;
    """
    # 插入hainiu_queue表
    insert_queue_sql = """
    insert into hainiu_queue (type,action,params) values (%s, %s, %s);
    """
    # 更新内链接表的status状态
    update_inner_status_sql="""
    update hainiu_web_seed_internally set status=1 where a_md5=%s and md5=%s
    """
    logger = LogUtil().get_logger("download_news_queue","download_news_queue")
    db_util=DBUtil(_HAINIU_DB)
    try:
        # 统计hainiu_queue 未处理的记录数
        sql_params=[2]
        res1=db_util.read_one(select_queue_count_sql,sql_params)
        queue_count=res1[0]
        if queue_count >= 5:
            logger.info("hainiu_queue 有 %d 条未处理的记录，不需要导入！" % queue_count)
            return None
        # 统计内链接表符合条件的总记录数
        res2=db_util.read_one(select_inner_count_sql)
        inner_count=res2[0]

        # 计算有多少页
        page_num = inner_count / page_show_num if inner_count % page_show_num == 0 \
            else inner_count / page_show_num + 1
        start_time = time.time()
        # 分页查询
        for page in range(page_num):
            sql_params=[page_show_num]
            res3=db_util.read_dict(select_inner_limit_sql,sql_params)
            # 插入队列表的记录
            insert_queue_record=[]
            # param字典
            param_dict={}
            # inner表内要进行更新的记录
            update_innner_status_record=[]
            for row in res3:
                # md5,a_url,a_md5,domain,a_host,a_title
                md5=row['md5']
                a_url=row['a_url']
                a_md5=row['a_md5']
                domain=row['domain']
                a_host=row['a_host']
                a_title=row['a_title']
                # param数据
                param_dict['md5']=md5
                param_dict['a_md5']=a_md5
                param_dict['domain']=domain
                param_dict['a_host']=a_host
                param_dict['a_title']=a_title

                param_json=json.dumps(param_dict,ensure_ascii=False, encoding='utf-8')
                # 将数据放入列表
                insert_queue_record.append((2,a_url,param_json))
                update_innner_status_record.append((a_md5,md5))

            db_util.executemany(insert_queue_sql,insert_queue_record)
            db_util.executemany(update_inner_status_sql,update_innner_status_record)
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
