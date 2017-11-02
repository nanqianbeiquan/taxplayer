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


class JiangSuTaxCrawler(TaxplayerDownload):
    def __init__(self):
        super(JiangSuTaxCrawler, self).__init__()
        self.province = u'江苏省'
        self.province_py = 'Jiang_Su'
        self.path = self.get_savefile_directory(self.province_py)
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.get_directory(self.path)
        self.set_config()

    def set_config(self):
        self.xzqys = [u'(南京)', u'(无锡)', u'(徐州)', u'(常州)', u'(苏州)', u'(南通)', u'(连云港)', u'(苏州园区)',
                      u'(淮安)', u'(盐城)', u'(扬州)', u'(镇江)', u'(泰州)', u'(宿迁)', u'(保税区)']
        self.coluids = ['48038', '48118', '48039', '48119', '48040', '48120', '48041', '48121', '48042', '48122',
                        '48043', '48123', '48044', '48124', '48045', '48125', '48046', '48126', '48047', '48127',
                        '48048', '48128', '48049', '48129', '48050', '48130', '48051', '48131', '48052', '48132']
        self.unitids = ['73729', '74015', '73729', '74015', '73729', '74015', '73729', '74015', '73729', '74015',
                        '73729', '74015', '73729', '74015', '73729', '74015', '73729', '74015', '73729', '74015',
                        '73729', '74015', '73729', '74015', '73729', '74015', '73729', '74015', '73729', '74015']

    def log(self, message):
        self
        log_name = 'jiang_su_tax.log'
        logger(log_name, message)

    def get_tag_list(self, i, url, data):
        tag_list = []
        for t in range(5):
            if i == 30:
                r = self.get(url)
                if r.status_code == 200:
                    r.encoding = 'utf-8'
                    res = BeautifulSoup(r.text, 'html5lib')
                    big_tags = res.findAll('table', {'class': 'tb_main'})
                    for big_tag in big_tags:
                        tag_list.extend(big_tag.findAll('tr')[1:])
                    return tag_list
            else:
                r = self.post(url, data=data)
                if r.status_code == 200:
                    r.encoding = 'utf-8'
                    res = BeautifulSoup(r.text, 'html5lib')
                    pattern = '<a.*?</a>.*?\d{4}-\d{2}-\d{2}'
                    tag_list = re.findall(pattern, res.encode('utf-8'))
                    return tag_list
        return tag_list

    def run(self):
        log_name = 'jiang_su_tax.log'
        max_repeat_time = 5
        url_host = 'http://pub.jsds.gov.cn'
        for i in range(31):
            self.log(str(i))
            if i == 30:
                region = u'江苏省国家税务局'
                data = dict()
            else:
                region = u'江苏省地方税务局(' + self.xzqys[i / 2] + u')'
                data = {'appid': '1', 'col': '1', 'columnid': self.coluids[i], 'path': '/', 'permissiontype': '0',
                        'unitid': self.unitids[i], 'webid': '1', 'webname': '江苏省地方税务局'}
            self.log(region)
            print region
            repeat_time = 0
            for p in range(50):
                break_condition = repeat_time > max_repeat_time
                if break_condition:
                    self.log('break_condition: repeat_time > ' + str(max_repeat_time))
                    break
                if i == 30:
                    url = 'http://xxgk.jsgs.gov.cn/xxgk/jcms_files/jcms1/web1/site/zfxxgk/search.jsp?' \
                          'showsub=1&orderbysub=0&cid=43&cid=43&jdid=1&divid=div4869&currpage=%s' % (p + 1)
                else:
                    url = 'http://pub.jsds.gov.cn/module/jslib/jquery/jpage/dataproxy.jsp?' \
                          'perpage=40&startrecord=%s&endrecord=%s' % (p * 40 + 1, (p + 1) * 40)
                self.log('url: ' + url)
                tag_list = self.get_tag_list(i, url, data)
                if not tag_list and p > 2:
                    break
                if tag_list:
                    for tag in tag_list:
                        if i == 30:
                            fbrq = re.findall(r'\d{4}-\d{2}-\d{2}', tag.text.strip())[0]
                            href = tag.find('a').get('href')
                            title = tag.find('a').get('mc')
                            url_inner = href
                        else:
                            fbrq = re.findall(r'\d{4}-\d{2}-\d{2}', tag)[0]
                            soup = BeautifulSoup(tag, 'html.parser')
                            a_tag = soup.find('a')
                            href = a_tag.get('href').split("'")[1].replace('\\', '')
                            title = a_tag.text.strip()
                            url_inner = url_host + href
                        self.log('url_inner: ' + url_inner)
                        print 'url_inner', url_inner
                        html_filename = self.get_html_filename(url_inner)
                        html_savepath = self.path + '\\' + html_filename
                        if u'欠' in title or u'缴' in title or u'非正常户' in title or u'失踪' in title:
                            r_inner = self.get(url_inner)
                            r_inner.encoding = 'utf-8'
                            res_inner = BeautifulSoup(r_inner.text, 'html5lib')
                            res_inner_str = res_inner.encode('utf8')
                            a_tag_inners = re.findall(r'<a.*?href=.*?</a>', res_inner_str)
                            href_inners = self.get_href(a_tag_inners)
                            if href_inners:
                                for href_inner in href_inners:
                                    if i == 30:
                                        download_url = 'http://xxgk.jsgs.gov.cn' + href_inner
                                    else:
                                        download_url = href_inner
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
    crawler = JiangSuTaxCrawler()
    crawler.run()
