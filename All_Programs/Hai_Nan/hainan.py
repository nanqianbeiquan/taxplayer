# coding=utf-8
import PackageTool
import re
import os
import traceback
import gevent.monkey
import datetime
from lxml import html
import urlparse
import logging
from tax import MySQL,MyException,config,SpiderMan_FYH,MainFunc
requests=SpiderMan_FYH.SpiderMan()


class ShuiWu(MainFunc.MainFunc):
    def __init__(self,url,region):
        super(ShuiWu,self).__init__()
        self.province='海南省'
        self.url=url
        self.region=region
        self.domain='http://www.tax.hainan.gov.cn'
        self.oldest_time='2015-01-01'
        self.get_tk_url='http://www.tax.hainan.gov.cn/attachment_url.jspx'
        self.download_url='http://www.tax.hainan.gov.cn/attachment.jspx?'
        self.pinyin = 'Hai_Nan'


    def get_title(self):
        tree = self.get_tree(self.url)
        try:
            self.get_content(tree)
        except MyException.TitleOverTimeException:
            pass
        # else:
        #     last_page_a=tree.xpath('//div[@class="listrightbj_list"]/following-sibling::*[1]//a[last()]/@onclick')[0]
        #     last_page_href=re.findall("\('(.*?)'\)",last_page_a)[0]
        #     last_num=int(re.findall('index_(.*?)\.',last_page_href)[0])
        #     last_num_param='index_'+str(last_num)
        #     href_ahead=self.url.split('index')[0]
        #     for i in range(2,last_num+1):
        #         param='index_'+str(i)
        #         url=re.sub(last_num_param,param,last_page_href)
        #         whole_url= href_ahead+url
        #         tree = self.get_tree(whole_url)
        #         try:
        #             self.get_content(tree)
        #         except MyException.TitleOverTimeException:
        #             break

    def get_content(self,tree):
        title_list=tree.xpath('//div[@class="listrightbj_list"]/table[1]/tr')
        for one_title in title_list:
            self.title=one_title.xpath('td[2]/a/text()')[0]
            link = one_title.xpath('td[2]/a/@href')[0]
            whole_url=self.domain+link
            self.title_time = self.replace_word(one_title.xpath('td[3]/text()')[0])
            if self.title_time>self.oldest_time:
                re_words = u'(?:欠税|非正常户)'
                if re.search(u'%s' % (re_words,), self.title):
                    if not self.is_exist():
                        logging.info('------'+whole_url)
                        self.look_for_attachment(whole_url)
            else:
                raise MyException.TitleOverTimeException

    def look_for_attachment(self,url):
        self.doc_url=url
        self.filename=url.split('/')[-1]
        content=requests.get(url)
        content.encoding='utf-8'
        try:
            inform=re.findall('Cms.attachment\((.*?)\)',content.text)[0]
        except:
            print url
        else:
            inform_list=inform.split(',')
            self.cid = self.replace_word(inform_list[1])
            n=self.replace_word(inform_list[2])
            #xls文件动态加载出来
            if int(n)>=1:
                print self.doc_url
                params={'cid':self.cid,'n':n}
                tks=self.get_tandk(params)
                for tk in tks:
                    self.doc_url=self.download_url+'cid='+self.cid+'&i=0'+tk
                    self.filename = re.findall('id="attach0">(.*?)<',content.text)[0]
                    self.download()
            #页面还有静态xls等文件
            else:
                count=0
                tree = html.fromstring(content.text)
                all_a = tree.xpath('//a/@href')
                for a in all_a:
                    re_words = u'(?:\.doc|\.docx|\.xls|\.xlsx|\.pdf|\.xlt)'
                    if re.search(u'%s' % (re_words,), a):
                        self.filename = a.split('/')[-1]
                        if self.filename.endswith('.xlt'):
                            self.filename = self.filename.replace('.xlt', '.xls')
                        count += 1
                        self.doc_url = self.domain + a

                        self.download()
                if count == 0:
                    self.download()


    def get_tandk(self,params):
        tk=requests.get(url=self.get_tk_url,params=params).text
        return re.findall('"(.*)"',tk)[0]

    def replace_word(self, word):
        return word.replace('\n', '').replace('\t', '').replace('"', '').strip()




if __name__=='__main__':
    file_name = os.path.basename(__file__).split('.')[0]
    MainFunc.MainFunc.write_log(file_name)
    url_dict=config.hainan_config
    for m in url_dict.keys():
        # print m
        url=url_dict[m]
        a=ShuiWu(url,m)
        a.get_title()


