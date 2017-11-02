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


class HeBeiTaxCrawler(TaxplayerDownload):
    def __init__(self):
        super(HeBeiTaxCrawler, self).__init__()
        self.province = u'河北省'
        self.province_py = 'He_Bei'
        self.path = self.get_savefile_directory(self.province_py)
        print self.path
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.get_directory(self.path)
        self.set_config()

    def set_config(self):
        self.xzqys = [u'石家庄市', u'承德市', u'张家口', u'秦皇岛市', u'秦皇岛市', u'唐山市', u'廊坊市', u'保定市',
                      u'沧州市', u'衡水市', u'邢台市', u'邯郸市']
        self.xzqy_pys = ['sjz', 'cd', 'zjk', 'qhd', 'qhd', 'ts', 'lf', 'bd', 'cz', 'hs', 'xt', 'hd']
        self.url_sources = [
            'http://www.he-n-tax.gov.cn/sjzgsww_new/bsfw/qsgg',
            'http://www.he-n-tax.gov.cn/cdgsww_new/bsfw/ssgg/qsgg',
            'http://www.he-n-tax.gov.cn/zjkgsww_new/bsfw/ssgg/qsgg',
            'http://www.he-n-tax.gov.cn/qhdgsww_new/bsfw/swgg/qsgg',
            'http://www.he-n-tax.gov.cn/qhdgsww_new/bsfw/swgg/fzchgg',
            'http://www.he-n-tax.gov.cn/tsgsww_new/bsfw/ssgg/qsgg',
            'http://www.he-n-tax.gov.cn/lfgsww_new/bsfw/ssgg/qsgg',
            'http://www.he-n-tax.gov.cn/bdgsww_new/bsfw/ssgg/qsgg',
            'http://www.he-n-tax.gov.cn/czgsww/wfuwu/wqsgg/czgsgg_35111',
            'http://www.he-n-tax.gov.cn/hsgsww_new/bsfw/ssgg/qsgg',
            'http://www.he-n-tax.gov.cn/xtgsww_new/qsgg15',
            'http://www.he-n-tax.gov.cn/hdgsww/bsfw_19806/tzgg/4',
        ]

    def log(self, message):
        self
        log_name = 'he_bei_tax.log'
        logger(log_name, message)

    def get_tag_list(self, i, url):
        tag_list = []
        gbks = [0, 8, 10]
        for t in range(15):
            r = self.get(url)
            if r.status_code == 200:
                if i in gbks:
                    r.encoding = 'gbk'
                else:
                    r.encoding = 'utf-8'
                res = BeautifulSoup(r.text, 'html5lib')
                if i == 0:
                    big_tags = res.findAll('div', {'class': 'tabcontent'})
                else:
                    big_tags = res.findAll('ul', {'class': 'lr_list'})
                for big_tag in big_tags:
                    tag_list.extend(big_tag.findAll('li'))
                return tag_list
        return tag_list

    def run(self):
        log_name = 'he_bei_tax.log'
        max_repeat_time = 20
        url_host = 'http://www.he-n-tax.gov.cn'
        gbks = [0, 8, 10]
        for i in range(12):
            if i in gbks:
                decode_way = 'gbk'
            else:
                decode_way = 'utf-8'
            region = self.xzqys[i] + u'国家税务局'
            url_source = self.url_sources[i]
            self.log(region)
            print region
            repeat_time = 0
            for p in range(50):
                break_condition = repeat_time > max_repeat_time
                if break_condition:
                    self.log('break_condition: repeat_time > ' + str(max_repeat_time))
                    break
                if p == 0:
                    url = url_source + '/index.htm'
                else:
                    url = url_source + '/index_%s.htm' % str(p)
                self.log('url: ' + url)
                tag_list = self.get_tag_list(i, url)
                if not tag_list and p > 2:
                    break
                if tag_list:
                    for tag in tag_list:
                        fbrq = re.findall(r'\d{4}-\d{2}-\d{2}', tag.encode('utf-8'))
                        if not fbrq:
                            continue
                        else:
                            fbrq = fbrq[0]
                        a_tag = tag.find('a')
                        href = a_tag.get('href')[1:]
                        connect_date = href.split('/')[1]
                        title = a_tag.text.strip()
                        url_inner = url_source + href
                        self.log('url_inner: ' + url_inner)
                        print 'url_inner', url_inner
                        html_filename = self.get_html_filename(url_inner)
                        html_savepath = self.path + '\\' + html_filename
                        if u'欠' in title or u'缴' in title or u'非正常户' in title or u'失踪' in title:
                            r_inner = self.get(url_inner)
                            r_inner.encoding = decode_way
                            res_inner = BeautifulSoup(r_inner.text, 'html5lib')
                            res_inner_str = res_inner.encode('utf8')
                            a_tag_inners = re.findall(r'<a.*?href=.*?</a>', res_inner_str)
                            # print a_tag_inners
                            href_inners = self.get_href(a_tag_inners)
                            if href_inners:
                                for href_inner in href_inners:
                                    print href_inner
                                    download_url = url_source + '/' + str(connect_date) + href_inner[1:]
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
    crawler = HeBeiTaxCrawler()
    crawler.run()
