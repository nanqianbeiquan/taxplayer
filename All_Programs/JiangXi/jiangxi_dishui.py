# coding=utf-8
import PackageTool
import re
import os
import traceback
import gevent
import gevent.monkey
import datetime
import requests
from lxml import html
import urlparse
import logging
from xml.etree import ElementTree
from tax import MyException, MySQL,MainFunc
from tax.config import config_one, config_two

class ShuiWu(MainFunc.MainFunc):
    def __init__(self,province,region,dishui_type=None):
        super(ShuiWu,self).__init__()
        self.pinyin='Jiang_Xi'
        self.region=region
        self.url=config_two[province][region][dishui_type]['url']

        self.encoding=config_two[province][region][dishui_type]['encoding']
        self.div_class = config_two[province][region][dishui_type]['divclass']
        self.province=province
        result=urlparse.urlparse(self.url)
        self.href_ahead=result.scheme+'://'+result.netloc
        self.oldest_time='2015-01-01'



    def look_for_a(self,url):
        count=0
        tree=self.get_tree(url)
        all_a=tree.xpath('//a/@href')
        for a in all_a:
            if a.endswith('.doc') or a.endswith('.xls') or a.endswith('.xlsx') or a.endswith('.docx'):
                count+=1
                self.doc_url=self.href_ahead+a
                try:
                    self.filename=re.findall('filename=(.*)',a)[0]
                except:
                    self.filename=a.split('/')[-1]

                self.download()
        if count==0:
            self.doc_url = url
            self.download()

    def do_dishui(self):

        tree=self.get_tree(self.url,encoding=self.encoding)
        try:
            self.get_dishui_title(tree)
        except MyException.TitleOverTimeException:
            pass
        else:
            startpage=2
            while True:
                #print '正在抓取page'+str(startpage)
                param='pageNo='+str(startpage)+'%5D'
                url=re.sub('pageNo=1',param,self.url)
                tree = self.get_tree(url, encoding=self.encoding)
                try:
                    self.get_dishui_title(tree)
                except MyException.TitleOverTimeException:
                    break
                else:startpage+=1

    def get_dishui_title(self,tree):
        str='//div[@class="'+self.div_class+'"]/ul/li'
        news=tree.xpath(str)
        for gonggao in news:
            url=gonggao.xpath('a/@href')[0]
            self.title=gonggao.xpath('a/text()')[0].replace('\r','').replace('\n','')
            self.title_time=gonggao.xpath('span/text()')[0].replace('(','').replace(')','')
            if self.title_time < self.oldest_time:
                raise MyException.TitleOverTimeException
            url=self.href_ahead+url
            rex='^((?!流失).)*$'
            re_words = u'(?:欠税公告|非正常户)'
            if re.search(u'%s' % (re_words,), self.title) and re.match(rex, self.title):
                self.deal_title(url)
    def deal_title(self,url):

        if not self.is_exist():
            self.filename=re.findall('contentId=(.*?)category',url)[0]+'.html'

            self.look_for_a(url)
            logging.info('-----'+url)



if __name__ == '__main__':

    tasks=[]
    today=datetime.date.today()
    file_name = os.path.basename(__file__).split('.')[0]
    MainFunc.MainFunc.write_log(file_name)
    m='江西省'
    for n in config_two[m].keys():
         for x in config_two[m][n].keys():
             shuiwu=ShuiWu(m,n,dishui_type=x)
             dd=shuiwu.do_dishui
             tasks.append(gevent.spawn(dd))
    gevent.joinall(tasks)







