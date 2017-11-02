# coding=utf-8
import PackageTool
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


class TianJinTaxCrawler(TaxplayerDownload):
    def __init__(self):
        super(TianJinTaxCrawler, self).__init__()
        self.province = u'天津市'
        self.province_py = 'Tian_Jin'
        self.path = self.get_savefile_directory(self.province_py)
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.get_directory(self.path)
        self.set_config()

    def set_config(self):
        self.xzqys = [u'和平区', u'河东区', u'河西区', u'南开区', u'河北区', u'红桥区', u'滨海新区中心商务区',
                      u'中新天津生态城', u'临港经济区', u'东丽区', u'西青区', u'津南区', u'北辰区', u'宁河区',
                      u'武清区', u'静海区', u'宝坻区', u'蓟州区', u'海洋石油', u'直属', u'保税区', u'滨海新区',
                      u'经济技术开发区', u'滨海高新技术产业开发区', u'东疆保税港区']
        self.fjdms = [
            '11241000000', '11242000000', '11243000000', '11244000000', '11245000000', '11246000000', '11247000000',
            '11248000000', '11249000000', '11250000000', '11251000000', '11252000000', '11253000000', '11254000000',
            '11255000000', '11256000000', '11257000000', '11258000000', '11290000000', '11291000000', '11294000000',
            '11296000000', '11297000000', '11298000000', '11299000000'
        ]

    def log(self, message):
        self
        log_name = 'tian_jin_tax.log'
        logger(log_name, message)

    def get_tag_list(self, url):
        tag_list = []
        for t in range(15):
            r = self.get(url)
            if r.status_code == 200:
                r.encoding = 'utf-8'
                res = BeautifulSoup(r.text, 'html5lib')
                big_tags = res.findAll('td', {'id': 'textContent'})
                for big_tag in big_tags:
                    tag_list.extend(big_tag.findAll('tr'))
                return tag_list
        return tag_list

    def run(self):
        log_name = 'tian_jin_tax.log'
        max_repeat_time = 20
        url_host = 'http://wzcx.tjsat.gov.cn'
        for i in range(0, len(self.xzqys)):
            if i == 18 or i == 19:
                region = self.xzqys[i] + u'税务分局'
            else:
                region = self.xzqys[i] + u'国家税务局'
            print region
            self.log(region)
            fjdm = self.fjdms[i]
            # continue
            repeat_time = 0
            for p in range(50):
                break_condition = repeat_time > max_repeat_time
                if break_condition:
                    self.log('break_condition: repeat_time > ' + str(max_repeat_time))
                    break
                url = 'http://wzcx.tjsat.gov.cn/detailedGgxx.action?gglx=4&fjdm=%s&pageNum=%s' % (fjdm, str(p + 1))
                self.log(region + '  ' + url)
                tag_list = self.get_tag_list(url)
                if not tag_list and p > 2:
                    break
                if tag_list:
                    for tag in tag_list:
                        fbrq = tag.findAll('td')[-1].text.strip()
                        if not fbrq:
                            continue
                        a_tag = tag.find('a')
                        href = a_tag.get('href')
                        url_inner = url_host + href
                        self.log('url_inner: ' + url_inner)
                        print 'url_inner', url_inner
                        html_filename = self.get_html_filename(url_inner)
                        html_savepath = self.path + '\\' + html_filename
                        title = a_tag.text.strip()
                        if u'欠' in title or u'缴' in title or u'非正常户' in title or u'失踪' in title:
                            r_inner = self.get(url_inner)
                            r_inner.encoding = 'utf-8'
                            res_inner = BeautifulSoup(r_inner.text, 'html5lib')
                            res_inner_str = res_inner.encode('utf8')
                            a_tag_inners = re.findall(r'<a.*?href=.*?</a>|<A.*?href=.*?</A>', res_inner_str)
                            href_inners = self.get_href(a_tag_inners)
                            if href_inners:
                                for href_inner in href_inners:
                                    download_url = url_host + href_inner
                                    self.log('download_url: ' + download_url)
                                    print 'download_url', download_url
                                    # filter_condition = self.check_download_url(download_url)
                                    # if filter_condition:
                                    filename = self.get_filename(download_url)
                                    savepath = self.path + '\\' + filename
                                    sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', " \
                                          "'%s', '%s', '%s')" % (self.province, region, fbrq, title, filename,
                                                                 download_url, self.last_update_time)
                                    if os.path.isfile(savepath):
                                        repeat_time = data_to_mysql(log_name, 0, sql, repeat_time)
                                    else:
                                        self.download_file(download_url, filename, savepath)
                                        repeat_time = data_to_mysql(log_name, 0, sql, repeat_time)
                            else:
                                sql = "INSERT into taxplayer_filename VALUES('%s', '%s', '%s', '%s', '%s', " \
                                      "'%s', '%s')" % (self.province, region, fbrq, title, html_filename, url_inner,
                                                       self.last_update_time)
                                if os.path.isfile(html_savepath):
                                    repeat_time = data_to_mysql(log_name, 0, sql, repeat_time)
                                else:
                                    self.download_htmlfile(r_inner, html_savepath)
                                    repeat_time = data_to_mysql(log_name, 0, sql, repeat_time)
                else:
                    break


if __name__ == '__main__':
    crawler = TianJinTaxCrawler()
    crawler.run()
