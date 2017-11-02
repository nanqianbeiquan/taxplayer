# coding=utf-8
from tax.taxplayer_download import TaxplayerDownload
from tax.Mysql_Config_Fyh import logger
from tax.Mysql_Config_Fyh import data_to_mysql
import re
import logging
import time
import sys
import os

import requests
import MySQLdb
from multiprocessing import pool
from bs4 import BeautifulSoup
# from SpiderMan import SpiderMan
from tax.Mysql_Config_Fyh import my_conn
# from Mysql_Config_Fyh import data_to_mysql
# from Mysql_Config_Fyh import logger


class ShangHaiTaxplayerCrawler(TaxplayerDownload):
    def __init__(self):
        super(ShangHaiTaxplayerCrawler, self).__init__()
        self.province = u'上海市'
        self.province_py = 'Shang_Hai'
        self.path = self.get_savefile_directory(self.province_py)
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.get_directory(self.path)
        self.set_config()
        self.order_nbr = '5fe6cf97-5592-11e7-be16-f45c89a63279'
        self.conn = my_conn(0)
        self.cursor = self.conn.cursor()

    def set_config(self):
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.path = sys.path[0] + '\All_files\\'

    def get_url_info(self):
        info = ''
        sql = "select * from taxplayer_url where pid = '1' and category_id = '2'"
        nums = self.cursor.execute(sql)  # 返回符合条件的总数表
        print nums
        if nums > 0:
            info = self.cursor.fetchmany(nums)
            for i in info:
                print i
        return info

    def log(self, message):
        self
        log_name = 'sh_taxplayer_crawler.log'
        logger(log_name, message)

    def get_tag_list(self,url):
        tag_list = []
        for i in range(5):
            r = self.get(url)
            r.encoding = 'utf-8'
            res = BeautifulSoup(r.text, 'html5lib')
            tag_div = res.findAll('div', {'class': 'list_content'})
            if tag_div:
                tag_list = tag_div[0].findAll('dd')
                return tag_list
        return tag_list

    def run(self):
        log_name = 'shang_hai_tax_qs.log'
        max_repeat_time = 5
        info = self.get_url_info()
        for i in range(0, len(info)):
            xzqy = info[i][6]
            xzqy_py = info[i][7]
            url_source = info[i][10]
            url_host = info[i][12]
            self.log(xzqy + xzqy_py + url_source)
            print xzqy, xzqy_py, url_source
            url = url_source + '/index.html'
            repeat_time = 0
            self.log(xzqy)
            for page in range(0, 30):
                break_condition = repeat_time > max_repeat_time
                if break_condition:
                    self.log('break_condition: repeat_time > ' + str(max_repeat_time))
                    break
                if page == 0:
                    url = url
                    print 'url1',url
                else:
                    url = url_source + '/index_' + str(page) + '.html'
                self.log(xzqy + '  ' + url)
                print xzqy + '  ' + url
                tag_list = self.get_tag_list(url)
                if tag_list:
                    for tag in tag_list:
                        fbrq = re.findall(r'\d{4}-\d{2}-\d{2}', tag.text.strip())

                        if not fbrq:
                            continue
                        else:
                            fbrq = fbrq[0]
                        print fbrq
                        a_tag = tag.find('a')
                        # print a_tag
                        #拼接标题链接地址
                        url_now = 'http://' + url_source.split('//')[1]
                        href = a_tag.get('href')
                        if './' in href:
                            href = a_tag.get('href').replace("./", "/", 1)
                            url_inner = url_now + href
                        else:
                            url_inner = url_host + href
                        self.log('url_inner: ' + url_inner)
                        print 'url_inner', url_inner
                    self.log(u'第' + str(page + 1) + u'页')

                    html_filename = self.get_html_filename(url_inner)
                    html_savepath = self.path + html_filename
                    # print 'html_savapath', html_savepath
                    title = a_tag.text.strip()
                    if u'欠税公告' in title or u'欠' in title and u'催缴' not in title:
                        print 'title',title,url_inner
                        r_inner = self.get(url_inner)
                        r_inner.encoding = 'utf-8'
                        res_inner = BeautifulSoup(r_inner.text, 'html5lib')
                        res_inner_str = res_inner.encode('utf8')
                        a_tag_inners = re.findall(r'<a.*?href=.*?</a>|<A.*?href=.*?</A>', res_inner_str)
                        #匹配详情页内链接地址
                        href_inners = self.get_href(a_tag_inners)
                        if href_inners:
                            for href_inner in href_inners:
                                url_host_now = re.findall(r'(.*)/.*?', url_inner)[0]
                                if './' in str(href_inner):
                                    # 下载地址拼接为当前路径+href
                                    href_inner = href_inner.replace('./', '/', 1)
                                    #拼接下载链接地址
                                    download_url = url_host_now + href_inner
                                else:
                                    download_url = url_host + href_inner
                                print 'download_url', download_url
                                filename = self.get_filename(download_url)
                                savepath = self.path  + filename
                                sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', " \
                                      "'%s', '%s', '%s')" % (self.province, xzqy, fbrq, title, filename,
                                                             download_url, self.last_update_time)
                                if os.path.isfile(savepath):
                                    print '%%%%%%%%%%%%%%'
                                    repeat_time = data_to_mysql(log_name, 0, sql, repeat_time)
                                    print 'repeat_time', repeat_time
                                else:
                                    self.download_file(download_url, filename, savepath)
                                    repeat_time = data_to_mysql(log_name, 0, sql, repeat_time)
                                    print 'download_url1', download_url
                                    print 'filename', filename
                                    print 'savepath', savepath
                                    print 'repeat_time', repeat_time

                        else:
                            sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', '%s', " \
                                  "'%s', '%s')" % (self.province, xzqy, fbrq, title, html_filename, url_inner,
                                                   self.last_update_time)
                            if os.path.isfile(html_savepath):
                                print '@@@@@@@@@@@'
                                repeat_time = data_to_mysql(log_name, 0, sql, repeat_time)
                                print 'repeat_time', repeat_time
                            else:
                                self.download_htmlfile(r_inner, html_savepath)
                                repeat_time = data_to_mysql(log_name, 0, sql, repeat_time)
                                print 'html_savepath', html_savepath
                                print 'savepath', html_savepath
                                print 'repeat_time', repeat_time

                else:
                    break


                #         res_inner = self.get_webpage(url_inner)
                #         res_inner_str = res_inner.encode('utf8')
                #         title_div = res_inner.findAll('div', {'id': 'ivs_title'})
                #         if not title_div:
                #             title_div = res_inner.findAll('div', {'class': 'title'})
                #         if title_div:
                #             title = title_div[0].find('p').text.strip()
                #             if u'欠' in title or u'缴' in title or u'欠税公告' in title:
                #                 href_inners = self.get_href(res_inner_str)
                #                 if href_inners:
                #                     for href_inner in href_inners:
                #                         file_name = href_inner[1:].split('/')[-1]
                #                         download_url = url_source + connect_date + href_inner[1:]
                #                         sql = "INSERT into taxplayer_sh_newoo_filename VALUES" \
                #                               "('%s', '%s', '%s', '%s', '%s', '%s')" \
                #                               % (xzqy, date, title, file_name, download_url, self.last_update_time)
                #                         self.log('download_url: ' + download_url)
                #                         print 'download_url', download_url
                #                         savepath = self.path + file_name
                #                         if os.path.isfile(savepath):
                #                             repeat_time = data_to_mysql(log_name, 0, sql, repeat_time)
                #                         else:
                #                             fs = self.get_download_file(download_url)
                #                             with open(savepath, 'wb') as f:
                #                                 f.write(fs.content)
                #                                 repeat_time = data_to_mysql(log_name, 0, sql, repeat_time)
                #         else:
                #             self.log('this page not found title_div, ' + url_inner)
                #             print 'not found title_div', url_inner
                # else:
                #     self.log('this page not found list_div' + url)
                #     print 'not found list_div', url
                #     break


if __name__ == '__main__':
    Crawler = ShangHaiTaxplayerCrawler()
    Crawler.run()
