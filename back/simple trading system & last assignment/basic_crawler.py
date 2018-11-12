# -*- coding: utf-8 -*-
"""
Created on Wed Oct 24 16:47:53 2018

@author: kite
"""

from datetime import datetime, timedelta

import tushare as ts
from pymongo import UpdateOne

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
    
    for date in all_dates:
        try:
            crawl_basic_at_date(date)
        except:
            print('抓取股票基本信息时出错，日期：%s' % date, flush=True)
            
def crawl_basic_at_date(date):
    """
    
    """
    df_basics = ts.get_stock_basics(date)
    
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
    crawl_basic('2017-01-01', '2017-12-31')
