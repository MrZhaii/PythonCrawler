#-*- encoding: utf-8 -*-
'''
request_test.py
Created on 2019/3/16 19:15
Copyright (c) 2019/3/16, 海牛学院版权所有.
@author: 潘牛
'''

import sys
from bs4 import BeautifulSoup
from commons.util.request_util import RequestUtil
from commons.util.html_util import HtmlUtil
from commons.util.util import Util

def crawler_web_seed_url(url):
    '''
    爬取种子页的所有a链接
    :param url:   种子页url
    :return: 无
    '''

    r = RequestUtil()
    hu = HtmlUtil()
    u = Util()

    # 通过phandomjs 请求url，返回网页，包括网页的ajax请求
    html = r.http_get_phandomjs(url)

    #html = html.decode('utf-8').encode(sys.getfilesystemdomainencoding())
    #print html
    #可以从HTML或XML文件中提取数据的Python第三方库
    soup = BeautifulSoup(html, 'lxml')
    # a链接dom对象列表
    a_docs = soup.find_all("a")
    aset = set()
    #获取domain
    domain = hu.get_url_domain(url)
    #获取host
    host = hu.get_url_host(url)
    print 'domain==>',domain
    print 'host==>',host
    for a in a_docs:
        #获取a标签的href
        a_href = hu.get_format_url(url,a,host)
        #获取a标签的内容
        a_title = a.get_text().strip()
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
        a_xpath = hu.get_dom_parent_xpath_js_new(a)
        print ("%s\t%s\t%s\t%s\t%s") % (a_title.decode("utf-8"),a_href,a_host,a_md5,a_xpath)

    r.close_phandomjs()

def print_news_url_content(news_url):
    '''
    打印最终新闻页面内容
    :param news_url:
    :return:
    '''
    r = RequestUtil()
    hu = HtmlUtil()
    u = Util()

    # 通过phandomjs 请求url，返回网页，包括网页的ajax请求
    html = r.http_get_phandomjs(news_url)

    #html = html.decode('utf-8').encode(sys.getfilesystemdomainencoding())

    # html=html.replace("\r","").replace("\n","\002")
    print html

    r.close_phandomjs()


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')

    # url = 'https://news.sina.com.cn/roll/#pageid=153&lid=2509&k=&num=50&page=1'
    # url = 'http://www.chinanews.com/china.shtml'
    # crawler_web_seed_url(url)


    news_url = "http://www.chinanews.com/gn/2021/02-19/9414516.shtml"
    print_news_url_content(news_url)


