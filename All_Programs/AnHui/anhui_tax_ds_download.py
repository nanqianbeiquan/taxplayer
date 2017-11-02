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


class AnHuiTaxCrawler(TaxplayerDownload):
    def __init__(self):
        super(AnHuiTaxCrawler, self).__init__()
        self.province = u'安徽省'
        self.province_py = 'An_hui'
        self.path = self.get_savefile_directory(self.province_py)
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.get_directory(self.path)
        self.set_config()

    def set_config(self):
        self.xzqys = [u'合肥市', u'淮北市', u'宿州市', u'蚌埠市', u'阜阳市', u'淮南市', u'滁州市', u'六安市',
                      u'马鞍山市', u'芜湖市', u'宣城市', u'铜陵市', u'池州市', u'安庆市', u'黄山市', u'合肥市',
                      u'六安市', u'马鞍山市', u'池州市']
        self.find_tags = ['td_x', 'boxxx', 'boxxx', 'list14', 'linex', 'boxxx', 'boxxx', 'boxxx', 'boxxh', 'news_list',
                          'boxxx', 'list_14xx', 'nylist14', 'boxxh', 'boxxx', 'td_x', 'boxxx', 'boxxh', 'nylist14']
        self.url_sources = [
            'http://www.ahhf-l-tax.gov.cn/hefei/bsfw/ssgg/qsgg',
            'http://www.ahhb-l-tax.gov.cn/huaibei/zwxxgk/qsgg',
            'http://60.166.52.36/suzhou/zwgk/ggl',
            'http://www.ahbb-l-tax.gov.cn/bengbu/swgk/qsgg',
            'http://www.ahfy-l-tax.gov.cn/fuyang/zwgk/ggxw/qsgg',  # 只有3个文件，已经手动好
            'http://www.ahhn-l-tax.gov.cn/huainan/bszx/sscx/qsgg',
            'http://60.166.52.36/chuzhou/nsfw/qsgg',
            'http://www.ahla-l-tax.gov.cn/liuan/zwgk/qsgg/qsgg',
            'http://www.ahma-l-tax.gov.cn/maanshan/nsfw/tzgg/sstz',
            'http://www.ahwh-l-tax.gov.cn/wuhu/dsfw/sscx/qsgg',
            'http://www.ahxc-l-tax.gov.cn/xuancheng/bsfw/tzgg/qsgg',
            'http://60.166.52.36/tltax/nsfw/tzgg/qsgg',
            'http://www.ahcz-l-tax.gov.cn/chizhou/bsfw/ssgg/qsgg',
            'http://www.ahaq-l-tax.gov.cn/anqing/zwgk/gggs',
            'http://www.ahhs-l-tax.gov.cn/hstax/bsfw/ssgg',
            'http://www.ahhf-l-tax.gov.cn/hefei/bsfw/ssgg/fzch',
            'http://www.ahla-l-tax.gov.cn/liuan/zwgk/qsgg/fzch',
            'http://www.ahma-l-tax.gov.cn/maanshan/nsfw/tzgg/fzch',
            'http://www.ahcz-l-tax.gov.cn/chizhou/bsfw/ssgg/fzchrd'
        ]

    def log(self, message):
        self
        log_name = 'an_hui_tax_ds.log'
        logger(log_name, message)

    def get_tag_list(self, url, find_tag):
        tag_list = []
        for t in range(15):
            r = self.get(url)
            if r.status_code == 200:
                r.encoding = 'utf-8'
                res = BeautifulSoup(r.text, 'html5lib')
                big_tags = res.findAll('table', {'class': find_tag})
                for big_tag in big_tags:
                    tag_list.extend(big_tag.findAll('tr'))
                return tag_list
        return tag_list

    def run(self):
        log_name = 'an_hui_tax_ds.log'
        max_repeat_time = 10
        for i in range(0, len(self.xzqys)):
            find_tag = self.find_tags[i]
            region = self.xzqys[i] + u'地方税务局'
            url_source = self.url_sources[i]
            url_host = 'http://' + url_source.split('/')[2]
            print region, url_host
            # continue
            self.log(region)
            # continue
            repeat_time = 0
            for p in range(50):
                break_condition = repeat_time > max_repeat_time
                if break_condition:
                    self.log('break_condition: repeat_time > ' + str(max_repeat_time))
                    break
                if p == 0:
                    url = url_source + '/index.htm'
                else:
                    url = url_source + '/index_%s.htm' % str(p + 1)
                self.log(region + '  ' + url)
                tag_list = self.get_tag_list(url, find_tag)
                if not tag_list and p > 2:
                    break
                if tag_list:
                    for tag in tag_list:
                        fbrq = re.findall(r'\d{4}-\d{2}-\d{2}', tag.text.strip())
                        if not fbrq:
                            continue
                        else:
                            fbrq = fbrq[0]
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
    crawler = AnHuiTaxCrawler()
    crawler.run()
