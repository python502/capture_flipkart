# coding:utf-8

from gevent import monkey
monkey.patch_all()

import urllib2
from gevent.pool import Pool


import requests
from bs4 import BeautifulSoup

check_url='https://www.flipkart.com/mens-clothing/sports-wear/tshirts/pr?sid=2oq,s9b,6gr,j9y&otracker=nmenu_sub_Men_0_Sports%20T-Shirts'
import requests
s=requests.session()
class SpiderProxy(object):
    """黄哥Python培训 黄哥所写
    从http://www.xicidaili.com/wt 取代理ip
    """
    headers = {
        "Host": "www.xicidaili.com",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:47.0) Gecko/20100101 Firefox/47.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "http://www.xicidaili.com/wt/1",
    }

    def __init__(self, session_url):
        self.req = requests.session()
        self.req.get(session_url)

    def get_pagesource(self, url):
        '''取得html pagesource'''
        html = self.req.get(url, headers=self.headers)
        return html.content

    def get_all_proxy(self, url, n):
        ''' 取得所有1-n页上的代理IP'''
        data = []
        for i in range(1, n):
            html = self.get_pagesource(url + str(i))
            soup = BeautifulSoup(html, "lxml")

            table = soup.find('table', id="ip_list")
            for row in table.findAll("tr"):
                cells = row.findAll("td")
                tmp = []
                for item in cells:

                    tmp.append(item.find(text=True))
                try:
                    tmp2 = tmp[1:2][0]
                    tmp3 = tmp[2:3][0]
                    tmp4 = tmp[5:6][0]
                    data.append({tmp4: tmp2 + ":" + tmp3})
                except Exception as e:
                    pass
        return data


class IsActivePorxyIP(object):
    """类组合
     用gevent 异步并发验证 代理IP是不是可以用
    """

    def __init__(self, session_url):
        self.proxy = SpiderProxy(session_url)
        self.is_active_proxy_ip = []

    def probe_proxy_ip(self, proxy_ip):
        """代理检测"""
        proxy = urllib2.ProxyHandler(proxy_ip)
        opener = urllib2.build_opener(proxy)
        urllib2.install_opener(opener)
        try:
            html = urllib2.urlopen(check_url)
            #
            if html:
                self.is_active_proxy_ip.append(proxy_ip)
                # print html.read()
                s.proxies = proxy_ip
                print s.get(check_url)
                print proxy_ip
                return True
            else:
                return False
        except Exception as e:
            return False


if __name__ == '__main__':
    session_url = 'http://www.xicidaili.com/wt/1'
    url = 'http://www.xicidaili.com/wt/'

    p_isactive = IsActivePorxyIP(session_url)
    proxy_ip_lst = p_isactive.proxy.get_all_proxy(url, 5)

    # 异步并发
    pool = Pool(20)
    pool.map(p_isactive.probe_proxy_ip, proxy_ip_lst)
    print p_isactive.is_active_proxy_ip