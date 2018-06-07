#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/6 13:25
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    : 
# @File    : capture_flipkart.py
# @Software: PyCharm
# @Desc    :
import requests
import json
import re
import multiprocessing
import time

from gevent.pool import Group
from datetime import datetime
from retrying import retry
from urlparse import urljoin

from logger import logger
from MysqldbOperate import MysqldbOperate

class FlipkartException(Exception):
    def __init__(self, err='flipkart error'):
        super(FlipkartException, self).__init__(err)

#Something's not right!
class Flipkart500Exception(Exception):
    def __init__(self, err='flipkart 500 error'):
        super(Flipkart500Exception, self).__init__(err)

def retry_if_502_error(exception):
    return isinstance(exception, Flipkart500Exception)


MAX_PAGE = 50
flipkart_url = 'https://www.flipkart.com/'
user_agent = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
DICT_MYSQL = {'host': '127.0.0.1', 'user': 'root', 'passwd': '111111', 'db': 'capture', 'port': 3306}
TABLE_NAME_FLIPKART = 'flipkart_records'
HEADER_GET = '''
        Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
        Accept-Encoding:gzip, deflate, br
        Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
        Cache-Control:no-cache
        Connection:keep-alive
        User-Agent:{}
        '''

def getDict4str(strsource, match=':'):
    outdict = {}
    lists = strsource.split('\n')
    for list in lists:
        list = list.strip()
        if list:
            strbegin = list.find(match)
            outdict[list[:strbegin].strip()] = list[strbegin+1:].strip() if strbegin != len(list) else ''
    return outdict

header_get = getDict4str(HEADER_GET.format(user_agent))
s = requests.session()

def write_into_mysql(queue):
    t1 = 0
    first_flag = True
    mysql = MysqldbOperate(DICT_MYSQL)
    while 1:
        if queue.empty():
            if first_flag:
                t1 = datetime.now()
                first_flag = False
            else:
                t2 = datetime.now()
                if (t2 - t1).seconds > 7200:
                    logger.info('more than {}s not get datas'.format(7200))
                    break
            time.sleep(5)
        else:
            result_datas = queue.get()
            if not result_datas:
                logger.info('analyze_data stop')
                break
            table = TABLE_NAME_FLIPKART
            replace_columns = ['id', 'rating_score', 'rating_count', 'review_count', 'price', 'discount', 'countdown', 'delivery', 'policy', 'cod', 'product_url', 'category']
            save_datas(mysql, result_datas, table, replace_columns)
            first_flag = True
            t1, t2 = 0, 0
    del mysql
    return True

def save_datas(mysql, good_datas, table, replace_columns):
    try:
        result_replace = True
        if not good_datas:
            return True
        if good_datas:
            operate_type = 'replace'
            result_replace = mysql.insert_batch(operate_type, table, replace_columns, good_datas)
            # logger.info('_save_datas result_replace: {}'.format(result_replace))
        return result_replace
    except Exception, e:
        logger.error('_save_datas error: {}.'.format(e))
        return False

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def get_total_pages(source_url):
    try:
        res = s.get(source_url, headers=header_get)
        if res.status_code == 200:
            page_source = res.text.encode('utf-8')
        else:
            logger.error('source_url:{} get status error:{}'.format(source_url, res.status_code))
            raise FlipkartException('get status code:{} error'.format(res.status_code))
        pattern = re.compile(r'window.__INITIAL_STATE__ [\s\S]+?</script>', re.S)
        init_state = pattern.findall(page_source)
        source_info = init_state[0]
        source_info = source_info[26:-11]
        source_info=json.loads(source_info)
        totalPages = source_info['pageDataV4']['page']['data']['10003'][-1]['widget']['data']['totalPages']
        return totalPages
    except Exception,e:
        logger.error('get_total_pages error:{}'.format(e))
        raise

def capture_infos(source_infos):
    for source_info in source_infos.viewitems():
        logger.info('{} begin'.format(source_info[0]))
        capture_info(source_info)
        logger.info('{} end'.format(source_info[0]))

def capture_info(source_info):
    page_urls = []
    source_url = source_info[1]
    category = source_info[0]
    totalPages = get_total_pages(source_url)
    totalPages = min(totalPages, MAX_PAGE)
    for i in range(1, totalPages + 1):
        infos = {}
        page_url = '{}&page={}'.format(source_url, i)
        infos[category] = page_url
        page_urls.append(infos)
    manager = multiprocessing.Manager()
    queue = manager.Queue(maxsize=1000)
    p1 = multiprocessing.Process(target=write_into_mysql, args=(queue,))
    p1.start()
    pool = multiprocessing.Pool(processes=10)
    for page_url in page_urls:
        pool.apply_async(get_page_info, (queue, page_url))
    pool.close()
    pool.join()
    queue.put(None)
    p1.join()
    # 调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束
    logger.info("Sub-process(es) done.")

@retry(retry_on_exception=retry_if_502_error,stop_max_attempt_number=10, wait_fixed=5000)
def get_detail_info(product_url_infos):
    try:
        result = {}
        product_url = product_url_infos.values()[0]
        category = product_url_infos.keys()[0]
        res = s.get(product_url, headers=header_get)
        if res.status_code == 200:
            page_source = res.text.encode('utf-8')
        else:
            logger.error('page_url:{} get status error:{}'.format(product_url, res.status_code))
            raise FlipkartException('get status code:{} error'.format(res.status_code))
        pattern = re.compile(r'window.__INITIAL_STATE__ [\s\S]+?</script>', re.S)
        init_state = pattern.findall(page_source)
        source_info = init_state[0]
        if source_info.find('"serverErrorMessage":"Please try again later"') != -1:
            raise Flipkart500Exception

        source_info = source_info[26:-11]
        source_info = json.loads(source_info)['productPage']['productDetails']
        result['category'] = category
        result['id'] = source_info['pageContext']['productId']
        result['product_url'] = product_url
        result['price'] = source_info['pageContext']['pricing']['finalPrice']['decimalValue']

        if page_source.find('ends in less than</span><span><span>') == -1:
            result['countdown'] = 0
        else:
            result['countdown'] = 1
        try:
            result['rating_score'] = source_info['pageContext']['rating']['average']
        except:
            result['rating_score'] = 0
        try:
            result['rating_count'] = source_info['pageContext']['rating']['count']
        except:
            result['rating_count'] = 0
        try:
            result['review_count'] = source_info['pageContext']['rating']['reviewCount']
        except:
            result['review_count'] = 0
        try:
            result['discount'] = source_info['pageContext']['pricing']['totalDiscount']
        except:
            result['discount'] = 0
        try:
            result['delivery'] = source_info['data']['delivery_widget_1']['data'][0]['value']['messages'][0]['value']['dateText']
        except:
            result['delivery'] = ''
        try:
            datas = source_info['data']['product_services_1']['data'][0]['value']['services']
            for data in datas:
                info = data['text']
                if info.find('Replacement Policy') != -1:
                    result['policy'] = info
                if info.find('Cash on Delivery') != -1:
                    result['cod'] = info
        except:
            logger.info('product_url:{} not get services item'.format(product_url))
        return result
    except Exception, e:
        logger.error('product_url:{} error:{}'.format(product_url, e))
        logger.error("page_source:{}".format(page_source))
        raise

@retry(retry_on_exception=retry_if_502_error,stop_max_attempt_number=10, wait_fixed=2000)
def get_page_info(queue, page_url_infos):
    try:
        product_urls = []
        page_url = page_url_infos.values()[0]
        category = page_url_infos.keys()[0]
        res = s.get(page_url, headers=header_get)
        if res.status_code == 200:
            page_source = res.text.encode('utf-8')
        else:
            logger.error('page_url:{} get status error:{}'.format(page_url, res.status_code))
            raise FlipkartException('get status code:{} error'.format(res.status_code))
        pattern = re.compile(r'window.__INITIAL_STATE__ [\s\S]+?</script>', re.S)
        init_state = pattern.findall(page_source)
        source_info = init_state[0]
        if source_info.find('"serverErrorMessage":"Please try again later"') != -1:
            raise Flipkart500Exception
        source_info = source_info[26:-11]
        source_info = json.loads(source_info)
        data_infos = source_info['pageDataV4']['page']['data']['10003'][1:-1]
        for data_info in data_infos:
            products = data_info['widget']['data']['products']
            for product in products:
                product_infos = {}
                base_url = product['productInfo']['value']['baseUrl']
                product_url = urljoin(flipkart_url, base_url)
                product_infos[category] = product_url
                product_urls.append(product_infos)
        # logger.info('product_urls:{}'.format(product_urls))
        group = Group()
        page_detail_infos = group.map(get_detail_info, product_urls)
        queue.put(page_detail_infos)
    except Exception, e:
        logger.error('get_page_info page_url:{} error:{}'.format(page_url, e))
        raise


def main():
    startTime = datetime.now()
    source_url = {'wallets-clutches':'https://www.flipkart.com/bags-wallets-belts/wallets-clutches/pr?sid=reh%2Ccca&marketplace=FLIPKART&sort=popularity','audio-video':'https://www.flipkart.com/audio-video/headphones/pr?sid=0pm,fcn&marketplace=FLIPKART&sort=popularity'}
    # source_url = {'wallets-clutches':'https://www.flipkart.com/bags-wallets-belts/wallets-clutches/pr?sid=reh%2Ccca&marketplace=FLIPKART&sort=popularity'}
    capture_infos(source_url)
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()