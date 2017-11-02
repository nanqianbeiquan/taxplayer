# coding=utf-8
from tax.taxplayer_download import TaxplayerDownload
from tax.Mysql_Config_Fyh import logger
from tax.Mysql_Config_Fyh import data_to_mysql
from bs4 import BeautifulSoup
import os
import re
import sys
import time
import requests
import shutil
from lxml import etree


class AnHuiTaxCrawler(TaxplayerDownload):
    def __init__(self):
        super(AnHuiTaxCrawler, self).__init__()
        self.province = u'浙江省'
        self.province_py = 'Zhe_Jiang'
        self.path = self.get_savefile_directory(self.province_py)
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.get_directory(self.path)
        self.set_config()

    def set_config(self):
        self.xzqys = [u'宁波市',u'宁波市',u'浙江省',]
        self.find_tags = ['DSjc04', 'DSjc04', '4079987']
        self.url_sources = [
            'http://www.nb-n-tax.gov.cn/xxgk/tzgg/fzchgg',              #宁波市非正常户
            'http://www.nb-n-tax.gov.cn/xxgk/tzgg/qsgg',                #宁波市欠税公告
            'http://www.zj-l-tax.gov.cn/col/col1228546/index.html',     #浙江省地税
        ]

    def log(self, message):
        log_name = 'zhe_jiang_tax_ds.log'
        logger(log_name, message)

    def get_tag_list(self, url, find_tag, i):
        tag_list = []
        for t in range(5):
            print url
            r = self.get(url)
            res_html = r.content.decode('utf-8')
            html = etree.HTML(res_html,parser=etree.HTMLParser(encoding='utf-8',strip_cdata=False))
            if i == 0 or i == 1:
                tag_list = html.xpath('//table[@class="%s"]//tr'%find_tag)
                print tag_list
            elif i == 2:
                temp = etree.tostring(html,pretty_print=True,encoding='utf-8')
                tag_list_str = re.findall('<li>        .*?</li>',temp)
                for i in tag_list_str:
                    i = etree.HTML(i.decode('utf-8'))
                    tag_list.append(i)
            return tag_list
        return tag_list

    def run(self):
        log_name = 'zhe_jiang_tax.log'
        max_repeat_time = 10
        for i in range(0, len(self.xzqys)):
            find_tag = self.find_tags[i]
            region = self.xzqys[i] + u'税务局'
            url_source = self.url_sources[i]
            url_host = 'http://' + url_source.split('/')[2]
            print region, url_host
            self.log(region)
            # continue
            repeat_time = 0
            for p in range(65):
                break_condition = repeat_time > max_repeat_time
                if break_condition:
                    self.log('break_condition: repeat_time > ' + str(max_repeat_time))
                    break
                if i == 2:
                    url = url_source + '?uid=4079987&pageNum=%s' % str(p + 1)
                else:
                    if p == 0:
                        url = url_source + '/index.htm'
                    else:
                        url = url_source + '/index_%s.htm' % str(p)
                self.log(region + '  ' + url)
                #取得大标题列表
                tag_list = self.get_tag_list(url, find_tag, i)

                if tag_list:
                    for tag in tag_list:
                        url_now = 'http://' + url_source.split('//')[1]
                        if i == 2:
                            fbrq = tag.xpath('//span/text()')[0]
                            href = tag.xpath('//a/@href')[0]
                            title = tag.xpath('//a/text()')[0]
                            print 'fbrq',fbrq
                        else:
                            fbrq = tag.xpath("td[3]/text()")[0].replace('[','').replace(']','')
                            href = tag.xpath("td/a/@href")[0]
                            title = tag.xpath('td/a/text()')[0]
                            print href
                        #拼接标题链接地址
                        if './' in href:
                            href = href.replace('./','/',1)
                            url_inner = url_now + href
                            print 'url_inner1',url_inner
                        else:
                            url_inner = url_host + href
                            print 'url_inner2',url_inner
                        self.log('url_inner: ' + url_inner)
                        print 'url_inner',url_inner
                        html_filename = self.get_html_filename(url_inner)
                        html_savepath = self.path + html_filename
                        print title
                        if u'欠' in title or u'缴' in title or u'非正常户' in title or u'失踪' in title:
                            r_inner = self.get(url_inner)
                            res_inner = r_inner.content.decode('utf-8')
                            html = etree.HTML(res_inner)
                            a_tag_inners = html.xpath("//a/@href")
                            #匹配详情页内链接地址
                            href_inners = self.get_href_new(a_tag_inners)
                            url_now = re.findall(r'(.*)/.*?', url_inner)[0]

                            if href_inners:
                                for href_inner in href_inners:

                                    if './' in str(href_inner):
                                        # 下载地址拼接为当前路径+href
                                        href_inner = href_inner.replace('./', '/', 1)
                                        #拼接下载链接地址
                                        download_url = url_now + href_inner
                                    else:
                                        download_url = url_host + href_inner
                                    print 'download_url', download_url
                                    filename = self.get_filename(download_url)
                                    savepath = self.path  + filename
                                    sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', " \
                                          "'%s', '%s', '%s')" % (self.province, region, fbrq, title, filename,
                                                                 download_url, self.last_update_time)
                                    if os.path.isfile(savepath):
                                        print '文件已经存在1'
                                        repeat_time = data_to_mysql(log_name, 0, sql, repeat_time)
                                    else:
                                        self.download_file(download_url, filename, savepath)
                                        repeat_time = data_to_mysql(log_name, 0, sql, repeat_time)
                                        print 'download_url1', download_url
                                        print 'filename', filename
                                        print 'savepath', savepath

                            else:
                                sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', '%s', " \
                                      "'%s', '%s')" % (self.province, region, fbrq, title, html_filename, url_inner,
                                                       self.last_update_time)
                                if os.path.isfile(html_savepath):
                                    repeat_time = data_to_mysql(log_name, 0, sql, repeat_time)
                                    print '文件已经存在2'
                                else:
                                    self.download_htmlfile(r_inner, html_savepath)
                                    repeat_time = data_to_mysql(log_name, 0, sql, repeat_time)
                                    print 'r_inner', html_savepath
                                    print 'html_savepath', html_savepath

                else:
                    break


if __name__ == '__main__':
    crawler = AnHuiTaxCrawler()
    crawler.run()
