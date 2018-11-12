# -*- coding: utf-8 -*-
"""
Created on Wed Oct 24 16:47:53 2018

@author: kite
"""

from datetime import datetime, timedelta

import tushare as ts
from pymongo import UpdateOne, ASCENDING

from database import DB_CONN
from stock_util import get_trading_dates

"""
从tushare获取股票基础数据，保存到本地的MongoDB数据库中
"""

def crawl_basic(start, end=None):
    """
    
    """
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]
    
    all_dates = get_trading_dates(start, end)
    if all_dates is None:
        print('没有获取到交易日历')
    total = len(all_dates)
    for i, date in enumerate(all_dates):
        try:
            crawl_basic_at_date(date)
        except:
            print('抓取股票基本信息时出错，日期：%s' % date, flush=True)
        print('基础信息数据获取进度: (%s/%s)' % (i+1, total))
            
def crawl_basic_at_date(date):
    """
    
    """
    
    df_basics = ts.get_stock_basics(date)
    # 如果没有创建索引，这里需要创建索引
    if df_basics is None:
        return
    
    update_requests = []
    codes = set(df_basics.index)
    for code in codes:
        doc = dict(df_basics.loc[code])
        try:
            time_to_market = datetime \
                .strptime(str(doc['timeToMarket']), '%Y%m%d') \
                .strftime('%Y-%m-%d')
                
            totals = float(doc['totals'])
            outstanding = float(doc['outstanding'])
            doc.update({
                    'code':code,
                    'date':date,
                    'timeToMarket':time_to_market,
                    'outstanding':outstanding,
                    'totals':totals
                    })
            
            update_requests.append(
                    UpdateOne(
                        {'code': code, 'date': date},
                        {'$set': doc}, upsert=True))
            
        except:
            print('发生异常，股票代码：%s，日期：%s' % (code, date), flush=True)
            print(doc, flush=True)
            
    
    if len(update_requests) > 0:
        update_result = DB_CONN['basic'].bulk_write(update_requests, ordered=False)

        print('抓取股票基本信息，日期：%s, 插入：%4d条，更新：%4d条' %
              (date, update_result.upserted_count, update_result.modified_count), flush=True)
    
    
if __name__ == '__main__':
#    col_list = DB_CONN.list_collection_names()
#    if 'basic' not in col_list:
    basic_col = DB_CONN['basic']
    if 'code_1_date_1' not in basic_col.index_information().keys():
        DB_CONN['basic'].create_index(
            [('code',ASCENDING), ('date',ASCENDING)])
        
    start = '2017-12-31'
    end = '2018-06-30'
    crawl_basic(start, end)
