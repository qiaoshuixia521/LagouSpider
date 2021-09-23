#-*- coding:utf8 -*-

'''
auther:zhangxiansheng
以拉勾网为入口实现全站西安公司信息的抓取
抓取内容包括：公司名称（name），公司地址(city)，公司所属行业（经营范围）(business)，公司介绍(introduce)，公司规模(scale)
共计30页，140条记录
多进程的使用
进程之间的通信
线上修改


'''


import json
import multiprocessing
import time
from multiprocessing import Queue
import requests
from bs4 import BeautifulSoup
from lxml import etree
from pymongo import *

#解决python3控制台输出InsecureRequestWarning的问题
#https://www.cnblogs.com/ernana/p/8601789.html
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


class UrlProcess(multiprocessing.Process):
    '''抓取西安所有IT公司的url进程'''

    def __init__(self,list_id,q,page):
        multiprocessing.Process.__init__(self)
        self.list_id=list_id
        self.q=q
        self.page = str(page)
    def run(self):
        print('start get url')
        self.get_rul()
        print('end get url')
    #突破拉钩的反爬
    def get_rul(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7',
            'Referer': 'https://www.lagou.com/',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip,deflate,br',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
        }
        params = (
            ('labelWords', ''),
            ('fromsearch', 'true'),
            ('suginput', ''),
        )
        session = requests.Session()
        session.headers.update(headers)

        response = session.get('https://www.lagou.com/jobs/list_list_%E8%A5%BF%E5%AE%89', params=params,verify=False)

        data = {
            'first': 'true',
            'pn': self.page,
            'kd': '',
        }
        headersb = {
            'Origin': 'https://www.lagou.com',
            'X-Anit-Forge-Code': '0',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Referer': 'https://www.lagou.com/jobs/list_python?labelWords=&fromSearch=true&suginput=',
            'X-Requested-With': 'XMLHttpRequest',
            'X-Anit-Forge-Token': 'None',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7',
        }
        page_url = session.post(
            'https://www.lagou.com/jobs/positionAjax.json?city=%E8%A5%BF%E5%AE%89&needAddtionalResult=false', data=data,
            headers=headersb,verify=False)
        # with open('page_url3.json', 'w', encoding='utf-8') as f:
        #     json.dump(page_url.text, f, ensure_ascii=False)
        json_dict=page_url.json()
        print(type(json_dict))

        numbers = json_dict['content']['positionResult']['result']
        for number in numbers:
            id_number = number.get('companyId', " ")
            url_company = 'https://www.lagou.com/gongsi/{0}.html'.format(id_number)
            print(url_company)
            response = session.get(url=url_company, headers=headersb,verify=False)
            # self.list_id.append(response)
            self.q.put(response.text)
            print(self.q.qsize())
            print("队列添加完成")
        # print(self.list_id[0].text)

class analyzes(multiprocessing.Process):
    client = MongoClient('mongodb://localhost:27017') #mongodb数据库连接
    db = client.company
    xian = db.xian
    def __init__(self,q):
        multiprocessing.Process.__init__(self)
        self.q = q

    def run(self):
        while self.q.qsize():
            print("提取公司id start")

            self.get_describes()
            print("提取 id end")

    def get_describes(self):
        html = self.q.get()
        print(type(html))
        soup = BeautifulSoup(html, features="lxml")
        print('starting describes')
        try:
            jiesao = soup.select_one('#company_intro .item_content .company_content').get_text().strip()  # 公司介绍
        except:
            jiesao='无'
        try:
            company_title = soup.select_one('body > div.top_info > div > div > div.company_main > h1 > a').get_text().strip()  # 公司名称
        except:
            company_title='无'
        company_propety = soup.select_one('#basic_container > div.item_content > ul > li:nth-child(1) > span').get_text().strip()  # 公司所属行业
        company_pcount = soup.select_one('#basic_container > div.item_content > ul > li:nth-child(3) > span').get_text().strip()  # 公司规模
        try:
            company_city = soup.select_one('#basic_container > div.item_content > ul > li:nth-child(4) > span').get_text().strip()  # 公司所在城市
        except:
            company_city='无'
        compary_dict = {
            "company_name": company_title,
            "company_city": company_city,
            "company_introduce": jiesao,
            "company_business": company_propety,
            "company_scale": company_pcount,

        }
        if company_city =="西安":
            self.xian.insert(compary_dict)
        else:
            pass

        print(compary_dict)


if __name__ =='__main__':
    q = Queue()
    print("main processing",id(q))
    #启动两个进程，一个进程向队列里面添加内容，另一个进程从队列取出内容进行解析，将结果保存到mongodb数据库
    for i in range(31):
        list_id=[]
        url_thread = UrlProcess(list_id,q,page=i)
        url_thread.start()
        time.sleep(2)

        get_page = analyzes(q)
        get_page.start()



