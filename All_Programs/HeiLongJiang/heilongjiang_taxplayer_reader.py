# coding=utf-8
import PackageTool
from tax.taxplayer_reader import TaxplayerReader
import sys
import os
import time
import bs4
import re


class HeiLongJiangTaxplayerReader(TaxplayerReader):
    def __init__(self):
        super(HeiLongJiangTaxplayerReader, self).__init__()
        self.province = u'黑龙江省'
        self.province_py = 'Hei_Long_Jiang'
        self.path = self.get_savefile_directory(self.province_py)
        # self.today = "'%'"
        # self.today = "'%2017-09-08%'"
        self.today = "'%" + time.strftime('%Y-%m-%d') + "%'"
        self.last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.fieldnames_directory = self.get_fieldnames_directory()
        self.row = -1
        self.log_name = '%s_reader.log' % self.province_py
        self.test_log_name = '%s_reader_test.log' % self.province_py
        self.db_table = 'test'
        self.db_table = ''

    def log(self, message):
        if self.db_table:
            self.logger(self.test_log_name, message)
        else:
            self.logger(self.log_name, message)

    def word_to_html(self):
        sql = "SELECT * from taxplayer_filename where (title like '%欠税%' or title like '%缴%' or title " \
              "like '%非正常户%' or title like '%非正户%') and filename like '%.doc%' and province = '" + \
              self.province.encode('utf8') + "' and last_update_time like " + self.today
        save_directory = self.path + '\word_to_html\\'
        self.convert_word_to_html(sql, self.path, save_directory)

    def get_abnormal_excel_fieldnames(self):
        sql = "SELECT * from taxplayer_filename where (title like '%非正常户%' or title like '%非正户%') and " \
              "filename like '%.xls%' and province = '" + self.province.encode('utf8') + \
              "' and last_update_time like " + self.today
        savepath = self.fieldnames_directory + '\\' + '%s_fields_read.xls' % self.province_py
        self.row = self.read_excel_fieldnames(sql, self.path, savepath, self.row)

    def get_abnormal_html_fieldnames(self):
        sql = "SELECT * from taxplayer_filename where (title like '%非正常户%' or title like '%非正户%') and " \
              "(filename like '%.doc%' or filename like '%.htm%') and province = '" + self.province.encode('utf8') + \
              "' and last_update_time like " + self.today
        savepath = self.fieldnames_directory + '\\' + '%s_fields_read.xls' % self.province_py
        self.row = self.read_html_field_info(sql, self.path, savepath, self.row)

    def get_qsgg_excel_fieldnames(self):
        sql = "SELECT * from taxplayer_filename where (title like '%欠税%' or title like '%缴%')" \
              " and filename like '%.xls%' and province = '" + self.province.encode('utf8') + \
              "' and last_update_time like " + self.today
        savepath = self.fieldnames_directory + '\\' + '%s_fields_read.xls' % self.province_py
        self.row = self.read_excel_fieldnames(sql, self.path, savepath, self.row)

    def get_qsgg_html_fieldnames(self):
        sql = "SELECT * from taxplayer_filename where (title like '%欠税%' or title like '%缴%')" \
              " and (filename like '%.doc%' or filename like '%.htm%') and province = '" \
              + self.province.encode('utf8') + "' and last_update_time like " + self.today
        savepath = self.fieldnames_directory + '\\' + '%s_fields_read.xls' % self.province_py
        self.row = self.read_html_field_info(sql, self.path, savepath, self.row)

    def abnormal_excel_reader(self):
        self.log('abnormal_excel_reader')
        fields = list()
        sql = "SELECT * from taxplayer_filename where (title like '%非正常户%' or title like '%非正户%') and " \
              "filename like '%.xls%' and province = '" + self.province.encode('utf8') + \
              "' and last_update_time like " + self.today
        self.log(sql)
        info = self.get_province_info(sql)
        all_num = 0
        k2 = 0
        for n in range(0, len(info)):
            k1 = 0
            num = 0
            region = info[n][1]
            fbrq = info[n][2]
            filepath = self.path + info[n][4]
            try:
                excel = self.get_excel(filepath)
                sheets = excel.sheets()
                count = len(sheets)  # sheet数量
                for m in range(count):
                    try:
                        table = excel.sheets()[m]
                        rows = table.nrows
                        match_fields = self.get_excel_abnormal_field_info(table, rows, fields)
                    except Exception as e:
                        print e
                        print n + 1, filepath, 'aaaaaaaa'
                    else:
                        if match_fields:
                            self.log(n + 1)
                            self.log(match_fields)
                            if len(match_fields) > 1:
                                print n + 1, match_fields
                            else:
                                continue
                                # continue
                            start_idx = self.get_excel_start_idx(table, rows)
                            int_fields = 'nsrsbh,zjhm,sfzhm'
                            rq_fields = 'rdrq,fbrq,djrq,dsrdsj'
                            for j in range(start_idx, rows):
                                row_vals = table.row_values(j)
                                col_vals = []
                                field_keys = 'province,region,last_update_time,'
                                val = "'" + self.province.encode('utf8') + "','" + region.encode(
                                    'utf8') + "','" + self.last_update_time + "','"
                                for md in range(len(match_fields)):
                                    key = match_fields[md].keys()[0]
                                    position = match_fields[md].values()[0]
                                    if key in int_fields:
                                        val += self.get_int_field(row_vals[position]) + "','"
                                        field_keys += key + ','
                                    elif key in rq_fields:
                                        val += self.get_formate_date(self.get_date(table, j, position)) + "','"
                                        field_keys += key + ','
                                    elif key == 'nsrmc':
                                        if row_vals[position].strip():
                                            val += row_vals[position].encode('utf8') + "','"
                                            field_keys += key + ','
                                    elif isinstance(row_vals[position], float):
                                        val += str(int(row_vals[position])) + "','"
                                        field_keys += key + ','
                                    else:
                                        val += row_vals[position].encode('utf8') + "','"
                                        field_keys += key + ','
                                field_keys = field_keys[0:-1]
                                val = val[0:-2]
                                if 'nsrmc' not in field_keys:
                                    continue
                                if 'rdrq' not in field_keys:
                                    field_keys += ',rdrq'
                                    val += ",'" + fbrq.encode('utf8') + "'"
                                if 'fbrq' not in field_keys:
                                    field_keys += ',fbrq'
                                    val += ",'" + fbrq.encode('utf8') + "'"
                                if self.db_table:
                                    sql = 'insert into test_abnormal (' + field_keys + ') values (' + val + ')'
                                else:
                                    sql = 'insert into taxplayer_abnormal (' + field_keys + ') values (' + val + ')'
                                data_nums = self.data_to_mysql(sql, num, k1, n)
                                num = data_nums[0]
                                k1 = data_nums[1]
                                if self.db_table:
                                    print n + 1, sql
                                    break
            except Exception as e:
                if '<html' in str(e[0]) or '<!DOCT' in str(e[0]):
                    try:
                        soup = self.get_soup(filepath)
                        tr_list, inner_signal = self.get_tr_list(soup)
                        if tr_list:
                            match_fields = self.get_html_abnormal_field_info(tr_list, fields)
                            new_tr_list = tr_list[1:]
                            if match_fields:
                                self.log(n + 1)
                                self.log(match_fields)
                                if len(match_fields) > 1:
                                    print n + 1, match_fields
                                else:
                                    continue
                                    # continue
                                for j in range(len(new_tr_list)):
                                    td = new_tr_list[j].findAll('td')
                                    field_keys = 'province,region,fbrq,last_update_time,'
                                    rq_fields = 'rdrq, djrq'
                                    val = "'" + self.province.encode('utf8') + "','" + region.encode('utf8') + "','" \
                                          + fbrq.encode('utf8') + "','" + self.last_update_time + "','"
                                    try:
                                        for md in range(len(match_fields)):
                                            key = match_fields[md].keys()[0]
                                            position = match_fields[md].values()[0]
                                            if key in rq_fields:
                                                val += self.get_formate_date(
                                                    td[position].text.strip().encode('utf8')) + "','"
                                                field_keys += key + ','
                                            elif key == 'nsrsbh':
                                                nsrsbh = td[position].text.strip().encode('utf8').replace("'", '')
                                                val += nsrsbh + "','"
                                                field_keys += key + ','
                                            elif key == 'nsrmc':
                                                if td[position].text.strip():
                                                    nsrmc = td[position].text.strip().encode('utf8').replace("'",
                                                                                                             '')
                                                    val += nsrmc + "','"
                                                    field_keys += key + ','
                                            else:
                                                val += td[position].text.strip().encode('utf8') + "','"
                                                field_keys += key + ','
                                        field_keys = field_keys[0:-1]
                                        val = val[0:-2]
                                    except Exception as e:
                                        print n + 1, e
                                    if 'nsrmc' not in field_keys:
                                        continue
                                    if 'rdrq' not in field_keys:
                                        field_keys += ',rdrq'
                                        val += ",'" + fbrq.encode('utf8') + "'"
                                    if self.db_table:
                                        sql = 'insert into test_abnormal (' + field_keys + ') values (' + val + ')'
                                    else:
                                        sql = 'insert into taxplayer_abnormal (' + field_keys + ') values (' + val + ')'
                                    data_nums = self.data_to_mysql(sql, num, k1, n)
                                    num = data_nums[0]
                                    k1 = data_nums[1]
                                    if self.db_table:
                                        print n + 1, sql
                                        break
                    except Exception as e:
                        self.log(e[0])
                        self.log(str(n + 1) + ',' + filepath + ',' + 'cccccc')
                        print e
                        print n + 1, filepath, 'cccccc'
                else:
                    self.log(e[0])
                    self.log(str(n + 1) + ',' + filepath + ',' + 'bbbbbb')
                    print e
                    print n + 1, filepath, 'bbbbbb'
            print str(n + 1) + u'表插入' + str(num) + u'条'
            print str(n + 1) + u'表重复' + str(k1) + u'条'
            k2 += k1
            all_num += num
        self.log(self.province + u'一共插入' + str(all_num) + u'条')
        self.log(self.province + u'一共失败' + str(k2) + u'条')
        print self.province + u'一共插入' + str(all_num) + u'条'
        print self.province + u'一共失败' + str(k2) + u'条'

    def abnormal_html_reader(self):
        self.log('abnormal_html_reader')
        fields = list()
        sql = "SELECT * from taxplayer_filename where (title like '%非正常户%' or title like '%非正户%') and " \
              "(filename like '%.doc%' or filename like '%.htm%') and province = '" + self.province.encode('utf8') + \
              "' and last_update_time like " + self.today
        self.log(sql)
        info = self.get_province_info(sql)
        all_num = 0
        k2 = 0
        for n in range(0, len(info)):
            k1 = 0
            num = 0
            region = info[n][1]
            fbrq = info[n][2]
            decode_way = self.get_decode_way(info[n][4])
            filepath = self.get_filepath(info[n][4], self.path)
            soup = self.get_soup(filepath, decode_way)
            tr_list, inner_signal = self.get_tr_list(soup)
            if tr_list:
                match_fields = self.get_html_abnormal_field_info(tr_list, fields)
                new_tr_list = tr_list[1:]
                if match_fields:
                    self.log(n + 1)
                    self.log(match_fields)
                    if len(match_fields) > 1:
                        print n + 1, match_fields
                    else:
                        continue
                        # continue
                    for j in range(len(new_tr_list)):
                        td = new_tr_list[j].findAll('td')
                        field_keys = 'province,region,fbrq,last_update_time,'
                        rq_fields = 'rdrq, djrq'
                        val = "'" + self.province.encode('utf8') + "','" + region.encode('utf8') + "','" \
                              + fbrq.encode('utf8') + "','" + self.last_update_time + "','"
                        try:
                            for md in range(len(match_fields)):
                                key = match_fields[md].keys()[0]
                                position = match_fields[md].values()[0]
                                if key in rq_fields:
                                    val += self.get_formate_date(td[position].text.strip().encode('utf8')) + "','"
                                    field_keys += key + ','
                                elif key == 'nsrsbh':
                                    nsrsbh = td[position].text.strip().encode('utf8').replace("'", '')
                                    val += nsrsbh + "','"
                                    field_keys += key + ','
                                elif key == 'nsrmc':
                                    if td[position].text.strip():
                                        nsrmc = td[position].text.strip().encode('utf8').replace("'", '')
                                        val += nsrmc + "','"
                                        field_keys += key + ','
                                else:
                                    val += td[position].text.strip().encode('utf8') + "','"
                                    field_keys += key + ','
                            field_keys = field_keys[0:-1]
                            val = val[0:-2]
                        except Exception as e:
                            print n + 1, e
                        if 'nsrmc' not in field_keys:
                            continue
                        if 'rdrq' not in field_keys:
                            field_keys += ',rdrq'
                            val += ",'" + fbrq.encode('utf8') + "'"
                        if self.db_table:
                            sql = 'insert into test_abnormal (' + field_keys + ') values (' + val + ')'
                        else:
                            sql = 'insert into taxplayer_abnormal (' + field_keys + ') values (' + val + ')'
                        data_nums = self.data_to_mysql(sql, num, k1, n)
                        num = data_nums[0]
                        k1 = data_nums[1]
                        if self.db_table:
                            print n + 1, sql
                            break
                print str(n + 1) + u'表插入' + str(num) + u'条'
                print str(n + 1) + u'表重复' + str(k1) + u'条'
                k2 += k1
                all_num += num
        self.log(self.province + u'一共插入' + str(all_num) + u'条')
        self.log(self.province + u'一共失败' + str(k2) + u'条')
        print self.province + u'一共插入' + str(all_num) + u'条'
        print self.province + u'一共失败' + str(k2) + u'条'

    def qsgg_excel_reader(self):
        self.log('qsgg_excel_reader')
        fields = list()
        sql = "SELECT * from taxplayer_filename where (title like '%欠税%' or title like '%缴%')" \
              " and filename like '%.xls%' and province = '" + self.province.encode('utf8') + \
              "' and last_update_time like " + self.today
        self.log(sql)
        info = self.get_province_info(sql)
        all_num = 0
        k2 = 0
        for n in range(0, len(info)):
            k1 = 0
            num = 0
            region = info[n][1]
            fbrq = info[n][2]
            if '.xls' in info[n][4]:
                filepath = self.path + info[n][4]
            else:
                filepath = self.path + info[n][4].split('.')[0] + '.xls'
            try:
                excel = self.get_excel(filepath)
                sheets = excel.sheets()
                count = len(sheets)  # sheet数量
                for m in range(count):
                    try:
                        table = excel.sheets()[m]
                        rows = table.nrows
                        match_fields, wan = self.get_excel_qsgg_field_info(table, rows, fields)
                        # special = [25, 26, 45, 56, 57]
                        # if n + 1 in special:
                        #     match_fields = [{'nsrmc': 0}, {'nsrsbh': 1}, {'fddbr': 2}, {'jydz': 3}, {'qssz': 4},
                        #                     {'qsje': 6}]
                    except Exception as e:
                        if str(e[0]) == 'need more than 0 values to unpack':
                            continue
                        else:
                            print e
                            print n + 1, filepath, 'aaaaaaaa'
                    else:
                        if match_fields:
                            self.log(n + 1)
                            self.log(match_fields)
                            if len(match_fields) < 3:
                                continue
                            keys = [match_field.keys()[0] for match_field in match_fields]
                            cqsje_condition = 'dqsje' in keys and 'cqsje' in keys
                            if 'nsrsbh' not in keys or 'nsrmc' not in keys:
                                print 'not nsrsbh or nsrmc ', n + 1, info[n][4]
                            if 'qsje' not in keys and not cqsje_condition:
                                print 'nnnn', n + 1, info[n][4]
                                continue
                            print n + 1, match_fields, wan
                            # continue
                            start_idx = self.get_excel_start_idx(table, rows)
                            merge_cells = self.get_merge_cells(start_idx, table)
                            int_fields = 'nsrsbh,zjhm,sfzhm'
                            money_fields = 'cqsje,qsje,dqsje'
                            rq_fields = 'xjrq, ssqs, ssqz'
                            keep_fields = 'cqsje,qsje,dqsje,qssz'
                            for j in range(start_idx, rows):
                                row_vals = table.row_values(j)
                                first_position = match_fields[0].values()[0]
                                if type(row_vals[first_position]) == float:
                                    first_col_val = row_vals[first_position]
                                else:
                                    first_col_val = row_vals[first_position].strip()
                                field_keys = 'province,last_update_time,'
                                keys = []
                                val = "'" + self.province.encode('utf8') + "','" + self.last_update_time + "','"
                                for md in range(len(match_fields)):
                                    key = match_fields[md].keys()[0]
                                    position = match_fields[md].values()[0]
                                    row_val = row_vals[position]
                                    try:
                                        if key not in keep_fields and (merge_cells or not first_col_val):
                                            row_val_new = self.get_field_val(row_val, j, table, merge_cells, position)
                                        else:
                                            row_val_new = row_val
                                    except:
                                        row_val_new = row_val
                                    # continue
                                    if type(row_val_new) == unicode:
                                        row_val_new = row_val_new.strip().replace("'", '')
                                    if key in int_fields:
                                        val += self.get_int_field(row_val_new) + "','"
                                        field_keys += key + ','
                                        keys.append(key)
                                    elif key in money_fields:
                                        qsje_condition = key == 'qsje' and not row_val_new
                                        if type(row_val_new) == unicode:
                                            if (not re.findall(u'\d+', row_val_new) and row_val_new != u'') \
                                                    or qsje_condition:
                                                break
                                            else:
                                                val += self.get_money_field(row_val_new, wan) + "','"
                                                field_keys += key + ','
                                                keys.append(key)
                                        else:
                                            if row_val_new or key != 'qsje' or row_val_new == 0:
                                                val += self.get_money_field(row_val_new, wan) + "','"
                                                field_keys += key + ','
                                                keys.append(key)
                                            else:
                                                break
                                    elif key in rq_fields:
                                        val += self.get_date(table, j, position) + "','"
                                        field_keys += key + ','
                                        keys.append(key)
                                    elif isinstance(row_val_new, float):
                                        val += str(int(row_val_new)) + "','"
                                        field_keys += key + ','
                                        keys.append(key)
                                    elif key == 'qssz':
                                        if u'小计' in row_val_new or u'合计' in row_val_new or u'总和' in row_val_new:
                                            break
                                        else:
                                            val += row_val_new.strip().encode('utf8') + "','"
                                            field_keys += key + ','
                                            keys.append(key)
                                    else:
                                        val += row_val_new.strip().encode('utf8') + "','"
                                        field_keys += key + ','
                                        keys.append(key)
                                field_keys = field_keys[0:-1]
                                val = val[0:-2]
                                cqsje_condition = 'dqsje' in keys and 'cqsje' in keys
                                if 'qsje' not in keys and not cqsje_condition:
                                    continue
                                if 'fbrq' not in field_keys:
                                    field_keys += ',fbrq'
                                    val += ",'" + fbrq.encode('utf8') + "'"
                                if self.db_table:
                                    sql = 'insert into test_qsgg (' + field_keys + ') values (' + val + ')'
                                else:
                                    sql = 'insert into taxplayer_qsgg (' + field_keys + ') values (' + val + ')'
                                data_nums = self.data_to_mysql(sql, num, k1, n)
                                num = data_nums[0]
                                k1 = data_nums[1]
                                if self.db_table:
                                    print n + 1, sql
                                    break
            except Exception as e:
                if '<html' in str(e[0]) or '<!DOCT' in str(e[0]):
                    try:
                        soup = self.get_soup(filepath)
                        dw = self.get_money_dw_html(soup)
                        tr_list, inner_signal = self.get_tr_list(soup)
                        if tr_list:
                            match_fields, wan = self.get_html_qsgg_field_info(tr_list, fields)
                            wan = wan or dw
                            new_tr_list = tr_list[1:]
                            if match_fields:
                                self.log(n + 1)
                                self.log(match_fields)
                                if len(match_fields) < 3:
                                    continue
                                keys = [match_field.keys()[0] for match_field in match_fields]
                                cqsje_condition = 'dqsje' in keys and 'cqsje' in keys
                                if 'nsrsbh' not in keys or 'nsrmc' not in keys:
                                    print 'not nsrsbh or nsrmc ', n + 1, info[n][4]
                                if not cqsje_condition and 'qsje' not in keys:
                                    print 'nnnn', n + 1, info[n][4]
                                    continue
                                print n + 1, match_fields, wan
                                # continue
                                hb_tr = ''
                                for j in range(len(new_tr_list)):
                                    if inner_signal:
                                        tds = new_tr_list[j].children
                                        td_texts = [td.text.strip() for td in tds if isinstance(td, bs4.element.Tag)]
                                    else:
                                        tds = new_tr_list[j].findAll('td')
                                        td_texts = [td.text.strip() for td in tds]
                                    field_keys = 'province,fbrq,last_update_time,'
                                    money_fields = 'cqsje, qsje, dqsje'
                                    rq_fields = 'xjrq,ssqs,ssqz'
                                    hb_fields = 'nsrsbh, nsrmc, nsrzk, fddbr, zjzl, zjhm, jydz'
                                    vals = "'" + self.province.encode('utf8') + "','" + fbrq.encode('utf8') + "','" \
                                           + self.last_update_time + "','"
                                    try:
                                        keys = []
                                        if len(td_texts) >= len(match_fields):
                                            hb_tr = new_tr_list[j]
                                            for md in range(len(match_fields)):
                                                key = match_fields[md].keys()[0]
                                                position = match_fields[md].values()[0]
                                                if key in money_fields:
                                                    temp_val = td_texts[position]
                                                    qsje_condition = key == 'qsje' and not temp_val
                                                    if (not re.findall(u'\d+', temp_val) and temp_val != u'') \
                                                            or qsje_condition:
                                                        break
                                                    val = temp_val.encode('utf8')
                                                    val = self.get_money_field(val, wan)
                                                    field_keys += key + ','
                                                    keys.append(key)
                                                elif key in rq_fields:
                                                    val = td_texts[position].encode('utf8')
                                                    val = self.get_formate_date(val)
                                                    field_keys += key + ','
                                                    keys.append(key)
                                                else:
                                                    val = self.get_hb_cell_val(j, new_tr_list, position, inner_signal)
                                                    field_keys += key + ','
                                                    keys.append(key)
                                                vals += val + "','"
                                            field_keys = field_keys[:-1]
                                            vals = vals[:-2]
                                        else:
                                            position_new = -1
                                            for md in range(len(match_fields)):
                                                key = match_fields[md].keys()[0]
                                                position = match_fields[md].values()[0]
                                                if key in hb_fields:
                                                    hb_tds = hb_tr.findAll('td')
                                                    val = hb_tds[position].text.strip().encode('utf8')
                                                    field_keys += key + ','
                                                    keys.append(key)
                                                else:
                                                    tds = new_tr_list[j].findAll('td')
                                                    position_new += 1
                                                    val = tds[position_new].text.strip().encode('utf8')
                                                    if key in money_fields:
                                                        qsje_condition = key == 'qsje' and not val
                                                        if (not re.findall('\d+', val) and val != '') \
                                                                or qsje_condition:
                                                            break
                                                        val = self.get_money_field(val, wan)
                                                        field_keys += key + ','
                                                        keys.append(key)
                                                    elif key in rq_fields:
                                                        val = self.get_formate_date(val)
                                                        field_keys += key + ','
                                                        keys.append(key)
                                                    else:
                                                        field_keys += key + ','
                                                        keys.append(key)
                                                vals += val + "','"
                                            field_keys = field_keys[:-1]
                                            vals = vals[:-2]
                                    except Exception as e:
                                        field_keys = field_keys[:-1]
                                        vals = vals[:-2]
                                        print n + 1, e
                                    cqsje_condition = 'dqsje' in keys and 'cqsje' in keys
                                    if not cqsje_condition and 'qsje' not in keys:
                                        continue
                                    if self.db_table:
                                        sql = 'insert into test_qsgg (' + field_keys + ') values (' + vals + ')'
                                    else:
                                        sql = 'insert into taxplayer_qsgg (' + field_keys + ') values (' + vals + ')'
                                    data_nums = self.data_to_mysql(sql, num, k1, n)
                                    num = data_nums[0]
                                    k1 = data_nums[1]
                                    if self.db_table:
                                        print n + 1, sql
                                        break
                    except Exception as e:
                        print e
                        print n + 1, filepath, 'bbbbbb'
                else:
                    # self.log(e[0])
                    print e
                    print n + 1, filepath, 'bbbbbb'
            print str(n + 1) + u'表插入' + str(num) + u'条'
            print str(n + 1) + u'表重复' + str(k1) + u'条'
            k2 += k1
            all_num += num
        self.log(self.province + u'一共插入' + str(all_num) + u'条')
        self.log(self.province + u'一共失败' + str(k2) + u'条')
        print self.province + u'一共插入' + str(all_num) + u'条'
        print self.province + u'一共失败' + str(k2) + u'条'

    def qsgg_html_reader(self):
        self.log('qsgg_html_reader')
        fields = list()
        sql = "SELECT * from taxplayer_filename where (title like '%欠税%' or title like '%缴%')" \
              " and (filename like '%.doc%' or filename like '%.htm%') and province = '" \
              + self.province.encode('utf8') + "' and last_update_time like " + self.today
        self.log(sql)
        info = self.get_province_info(sql)
        k2 = 0
        all_num = 0
        for n in range(0, len(info)):
            k1 = 0
            num = 0
            fbrq = info[n][2]
            decode_way = self.get_decode_way(info[n][4])
            filepath = self.get_filepath(info[n][4], self.path)
            soup = self.get_soup(filepath, decode_way)
            dw = self.get_money_dw_html(soup)
            tr_list, inner_signal = self.get_tr_list(soup)
            if tr_list:
                match_fields, wan = self.get_html_qsgg_field_info(tr_list, fields)
                new_tr_list = tr_list[1:]
                wan = wan or dw
                if match_fields:
                    self.log(n + 1)
                    self.log(match_fields)
                    if len(match_fields) < 3:
                        continue
                    keys = [match_field.keys()[0] for match_field in match_fields]
                    cqsje_condition = 'dqsje' in keys and 'cqsje' in keys
                    if 'nsrsbh' not in keys or 'nsrmc' not in keys:
                        print 'not nsrsbh or nsrmc ', n + 1, info[n][4]
                    if not cqsje_condition and 'qsje' not in keys:
                        print 'nnnn', n + 1, info[n][4]
                        continue
                    print n + 1, match_fields, wan
                    # continue
                    hb_tr = ''
                    for j in range(len(new_tr_list)):
                        if inner_signal:
                            tds = new_tr_list[j].children
                            td_texts = [td.text.strip() for td in tds if isinstance(td, bs4.element.Tag)]
                        else:
                            tds = new_tr_list[j].findAll('td')
                            td_texts = [td.text.strip() for td in tds]
                        field_keys = 'province,fbrq,last_update_time,'
                        money_fields = 'cqsje, qsje, dqsje'
                        rq_fields = 'xjrq,ssqs,ssqz'
                        hb_fields = 'nsrsbh, nsrmc, nsrzk, fddbr, zjzl, zjhm, jydz'
                        vals = "'" + self.province.encode('utf8') + "','" + fbrq.encode('utf8') + "','" \
                               + self.last_update_time + "','"
                        try:
                            keys = []
                            if len(td_texts) >= len(match_fields):
                                hb_tr = new_tr_list[j]
                                for md in range(len(match_fields)):
                                    key = match_fields[md].keys()[0]
                                    position = match_fields[md].values()[0]
                                    if key in money_fields:
                                        temp_val = td_texts[position]
                                        qsje_condition = key == 'qsje' and not temp_val
                                        if (not re.findall(u'\d+', temp_val) and temp_val != u'') \
                                                or qsje_condition:
                                            break
                                        val = temp_val.encode('utf8')
                                        val = self.get_money_field(val, wan)
                                        field_keys += key + ','
                                        keys.append(key)
                                    elif key in rq_fields:
                                        val = td_texts[position].encode('utf8')
                                        val = self.get_formate_date(val)
                                        field_keys += key + ','
                                        keys.append(key)
                                    else:
                                        val = self.get_hb_cell_val(j, new_tr_list, position, inner_signal)
                                        field_keys += key + ','
                                        keys.append(key)
                                    vals += val + "','"
                                field_keys = field_keys[:-1]
                                vals = vals[:-2]
                            else:
                                position_new = -1
                                for md in range(len(match_fields)):
                                    key = match_fields[md].keys()[0]
                                    position = match_fields[md].values()[0]
                                    if key in hb_fields:
                                        hb_tds = hb_tr.findAll('td')
                                        val = hb_tds[position].text.strip().encode('utf8')
                                        field_keys += key + ','
                                        keys.append(key)
                                    else:
                                        tds = new_tr_list[j].findAll('td')
                                        position_new += 1
                                        val = tds[position_new].text.strip().encode('utf8')
                                        if key in money_fields:
                                            qsje_condition = key == 'qsje' and not val
                                            if (not re.findall('\d+', val) and val != '') \
                                                    or qsje_condition:
                                                break
                                            val = self.get_money_field(val, wan)
                                            field_keys += key + ','
                                            keys.append(key)
                                        elif key in rq_fields:
                                            val = self.get_formate_date(val)
                                            field_keys += key + ','
                                            keys.append(key)
                                        else:
                                            field_keys += key + ','
                                            keys.append(key)
                                    vals += val + "','"
                                field_keys = field_keys[:-1]
                                vals = vals[:-2]
                        except Exception as e:
                            field_keys = field_keys[:-1]
                            vals = vals[:-2]
                            print n + 1, e
                        cqsje_condition = 'dqsje' in keys and 'cqsje' in keys
                        if not cqsje_condition and 'qsje' not in keys:
                            continue
                        if self.db_table:
                            sql = 'insert into test_qsgg (' + field_keys + ') values (' + vals + ')'
                        else:
                            sql = 'insert into taxplayer_qsgg (' + field_keys + ') values (' + vals + ')'
                        data_nums = self.data_to_mysql(sql, num, k1, n)
                        num = data_nums[0]
                        k1 = data_nums[1]
                        if self.db_table:
                            print n + 1, sql
                            break
                else:
                    self.log(str(n + 1))
            else:
                self.log(str(n + 1))
            print str(n + 1) + u'表插入' + str(num) + u'条'
            print str(n + 1) + u'表重复' + str(k1) + u'条'
            k2 += k1
            all_num += num
        self.log(self.province + u'一共插入' + str(all_num) + u'条')
        self.log(self.province + u'一共失败' + str(k2) + u'条')
        print self.province + u'一共插入' + str(all_num) + u'条'
        print self.province + u'一共失败' + str(k2) + u'条'


if __name__ == '__main__':
    reader = HeiLongJiangTaxplayerReader()
    reader.word_to_html()
    # reader.get_abnormal_excel_fieldnames()
    # reader.get_abnormal_html_fieldnames()
    # reader.get_qsgg_excel_fieldnames()
    # reader.get_qsgg_html_fieldnames()
    reader.abnormal_excel_reader()
    reader.abnormal_html_reader()
    reader.qsgg_excel_reader()
    reader.qsgg_html_reader()
