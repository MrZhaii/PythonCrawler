#-*- encoding: utf-8 -*-
'''
news_find_queue.py
Created on 2021/2/20 17:43
@author: zhaizz
'''
import time,json,sys
sys.path.append('/home/hadoop/hainiu_crawler')
from commons.util.log_util import LogUtil
from commons.util.db_util import DBUtil
from configs.config import _HAINIU_DB
def put_seed_to_queue(page_show_num):
    '''
    采用分页查询种子表数据，批量导入到hainiu_queue
    :param page_show_num: 一次查询条数
    '''
    # 统计hainiu_queue 未处理的记录数
    select_queue_count_sql = """
    select count(*) from hainiu_queue where type=%s and is_work=0 and fail_times=0;
    """

    # 统计种子表符合条件的总记录数
    select_seed_count_sql = """
    select count(*) from hainiu_web_seed where status=0;
    """

    # 分页查询种子表数据SQL
    select_seed_limit_sql = """
    select url, md5, domain, host, category from hainiu_web_seed
    where status=0 limit %s,%s;
     """

    # insert hainiu_queue sql
    insert_queue_sql = """
    insert into hainiu_queue (type,action,params) values (%s, %s, %s);
    """
    logger = LogUtil().get_logger("news_find_queue","news_find_queue")
    db_util = DBUtil(_HAINIU_DB)
    try:
        #1) 统计hainiu_queue 未处理的记录数
        sql_params = [1]
        # res1 是 ()
        res1 = db_util.read_one(select_queue_count_sql, sql_params)
        queue_count = res1[0]
        if queue_count >= 5:
            logger.info("hainiu_queue 有 %d 条未处理的记录，不需要导入！" % queue_count)
            return None

        start_time = time.time()

        #2) 统计种子表符合条件的总记录数
        res2 = db_util.read_one(select_seed_count_sql)
        seed_count = res2[0]

        # 计算有多少页
        page_num = seed_count / page_show_num if seed_count % page_show_num == 0 \
            else seed_count / page_show_num + 1

        # 分页查询
        for i in range(page_num):
            sql_params = [i * page_show_num, page_show_num]
            # ({},{},{},{},{})
            res3 = db_util.read_dict(select_seed_limit_sql, sql_params)
            # 插入队列表的数据
            insert_queue_values = []

            params_dict = {}
            for row in res3:
                # url, md5, domain, host, category
                act = row['url']
                md5 = row['md5']
                domain = row['domain']
                host = row['host']
                category = row['category']
                params_dict['md5'] = md5
                params_dict['domain'] = domain
                params_dict['host'] = host
                params_dict['category'] = category

                params_json = json.dumps(params_dict, ensure_ascii=False, encoding='utf-8')

                insert_queue_values.append((1, act, params_json))
            # 把查询的数据批量插入到队列表
            db_util.executemany(insert_queue_sql, insert_queue_values)

        end_time = time.time()
        run_time = end_time - start_time
        logger.info("本地导入 %d 条数据， 用时 %.2f 秒" % (seed_count, run_time))

    except Exception, e:
        logger.exception(e)
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

    page_show_num = 5

    put_seed_to_queue(page_show_num)
