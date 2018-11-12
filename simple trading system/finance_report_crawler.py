
#  -*- coding: utf-8 -*-

"""
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
©Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
"""
"""
抓取财报数据，主要关注EPS、公告日期、报告期
"""

import json, traceback, urllib3
from pymongo import UpdateOne, ASCENDING
from database import DB_CONN
from stock_util import get_all_codes
import tushare as ts

user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'


def crawl_finance_report():
    # 先获取所有的股票列表
    codes = ts.get_stock_basics().index.tolist()

    # 创建连接池
    conn_pool = urllib3.PoolManager()

    url = 'http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get?' \
          'type=YJBB20_YJBB&token=70f12f2f4f091e459a279469fe49eca5&st=reportdate&sr=-1' \
          '&filter=(scode={0})&p={page}&ps={pageSize}&js={"pages":(tp),"data":%20(x)}'
    
    error_code = []
    total = len(codes)
    for i, code in enumerate(codes):
        retry = True
        retry_num = 0
        while retry and retry_num<=3:
        
            try:
                response = conn_pool.request('GET', url.replace('{0}', code))
        
                # 解析抓取结果
            
                result = json.loads(response.data.decode('UTF-8'))
                
                retry = False
            
            except:
                retry_num += 1
        if retry_num == 3:
            error_code.append(code)
            continue
        

        reports = result['data']

        update_requests = []
        for report in reports:
            doc = {
                # 报告期
                'report_date': report['reportdate'][0:10],
                # 公告日期
                'announced_date': report['latestnoticedate'][0:10],
                # 每股收益
                'eps': report['basiceps'],
                'code': code
            }

            update_requests.append(
                UpdateOne(
                    {'code': code, 'report_date': doc['report_date']},
                    {'$set': doc}, upsert=True))

        if len(update_requests) > 0:
            update_result = DB_CONN['finance_report'].bulk_write(update_requests, ordered=False)
            print('股票 %s, 财报，更新 %d, 插入 %d' %
                  (code, update_result.modified_count, update_result.upserted_count))
        print('财报数据获取进度: (%s/%s)' % (i+1, total))
    print('error code list :', error_code, flush=True)


if __name__ == "__main__":
#    col_list = DB_CONN.list_collection_names()
#    if 'finance_report' not in col_list:
    finance_report_col = DB_CONN['finance_report']
    if 'code_1_report_date_1' not in finance_report_col.index_information().keys():
        DB_CONN['finance_report'].create_index(
            [('code',ASCENDING), ('report_date',ASCENDING)])
            
    crawl_finance_report()
