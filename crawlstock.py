#!/usr/bin/env python3
#coding: utf-8
#Copyright (C) Mr.D

import re
import urllib.request
import urllib.error
import chardet
import time
import sys
import pymongo

class CrawlStock:
    def __init__(self, userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11", dbHost = "mongodb://localhost:20008", dbName = "stock", codeSet = "stockcode", dataSet = "stockdata", timeOut = 30):
        self.user_agent = userAgent
        self.db_host = dbHost
        self.db_name = dbName
        self.code_set = codeSet
        self.data_set = dataSet
        self.db = None
        self.time_out = timeOut
        self.html = None
        self.data_list = []
    def download(self, url = "http://quote.eastmoney.com/stocklist.html", numRetries = 3, tag = 1):
        """
        Crawl web pages
        """
        print('Downloading: ', url)
        headers = {'User-agent': self.user_agent}
        request = urllib.request.Request(url, headers = headers)
        try:
            response = urllib.request.urlopen(request, None, self.time_out)
            html = response.read()
            if tag == 1:
                self.html = html.decode('GBK')
            elif tag == 2:
                self.html = html.decode('utf-8')
            else:
                code = chardet.detect(html).get('encoding')
                self.html = html.decode(code)
        except urllib.error.HTTPError as e:
            print('Download error: ', e.reason)
            self.html = None
            if numRetries > 0:
                if hasattr(e , 'code') and 500 <= e.code < 600:
                    self.download(url, numRetries-1, tag)
        except:
            ex_type, ex_value, ex_stack = sys.exc_info()
            print(ex_type, ex_value, ex_stack)
            self.html = None
    def get_codes(self):
        """
        Return a list of stock code from html
        """
        webpage_regex = re.compile(r'<a[^>]+href=["\'].*([s][hz][036]\d{5}).*["\']>(.*)\(', re.I)
        self.data_list = webpage_regex.findall(self.html) 
    def get_datas(self):
        """
        Return a list of stock data tuple from html
        """
        webpage_regex = re.compile(r'stock-info.*?state.*?(\d{4}-\d{2}-\d{2}).*?price.*?_close">(\d*.*?)<.*?>([-\+]?\d*\.?\d*%)<.*?今开.*?>([-\d]+.*?)<.*?成交量.*?>([-\d]+.*?)<.*?最高.*?>([-\d]+.*?)<.*?每股收益.*?>([-\d]+.*?)<.*?总股本.*?>([-\d]+.*?)<.*?昨收.*?>([-\d]+.*?)<.*?最低.*?>([-\d]+.*?)<', re.I|re.DOTALL)
        self.data_list = webpage_regex.findall(self.html)
    def conn_mongo(self):
        my_client = pymongo.MongoClient(self.db_host)
        self.db = my_client[self.db_name]
    def ins_code_mongo(self):
        """
        Insert code into mongodb
        """
        my_set = self.db[self.code_set]
        for i in self.data_list:
            my_dict = {'id':i[0],'name':i[1]}
            my_set.insert_one(my_dict)
    def ins_data_mongo(self, url = "https://gupiao.baidu.com/stock/", numRetries = 3):
        my_set1 = self.db[self.code_set]
        my_set2 = self.db[self.data_set]
        datas = my_set1.find({},{"_id":0},no_cursor_timeout = True)
        for i in datas:
            num_retries = numRetries
            #print(i['id'])
            my_url = url + i['id'] + ".html"
            #time.sleep(30)
            self.download(my_url, numRetries, 2)
            if self.html:
                self.get_datas()
            else:
                self.data_list = []
            while (not self.data_list) and (num_retries > 0):
                num_retries -= 1
                self.download(my_url, numRetries, 2)
                if self.html:
                    self.get_datas()
                else:
                    self.data_list = []
            print([i['id']] + self.data_list)
            if self.data_list:
                my_dict = {'id':i['id'],'日期':self.data_list[0][0],'收盘价':self.data_list[0][1],'开盘价':self.data_list[0][3],'昨收盘价':self.data_list[0][8],'涨跌率':self.data_list[0][2],'最高价':self.data_list[0][5],'最低价':self.data_list[0][9],'成交量':self.data_list[0][4],'每股收益':self.data_list[0][6],'总股本':self.data_list[0][7]}
                my_set2.insert_one(my_dict)  
        datas.close()
    def test(self):
        my_set1 = self.db[self.code_set]
        my_set2 = self.db[self.data_set]
        for i in my_set1.find({},{"_id":0}):
            print(i['id'] , i['name'])
                
if __name__ == '__main__':
    stock = CrawlStock()
    stock.conn_mongo()
    stock.ins_data_mongo()
    #stock.test()

