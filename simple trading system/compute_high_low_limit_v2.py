# -*- coding: utf-8 -*-
"""
Created on Fri Oct 26 16:14:54 2018

@author: kite
"""
import datetime, time
from pymongo import UpdateOne, ASCENDING
from database import DB_CONN
from stock_util import get_trading_dates, get_all_codes
import tushare as ts
import numpy as np
import pandas as pd
import requests
import json

"""
计算涨跌停价格

只要获取到前一天的价格

获取name和上市日期

最新ipo规则
如果是上市当天，则涨停价是上市发行价格的1.44倍
所以需要获取到发行价格
要不是
"""

#  获取发行价格并保存到数据库中
def fill_issueprice_and_timeToMarket():
    """
    ipo_info.xlsx 是从东方choice中提取出来;
    columns:
        code -- 股票代码
        name -- 股票当前名字
        issueprice -- 发行价格
        timeToMarket -- 上市时间
    """
    df = pd.read_excel('ipo_info.xlsx', header=0, dtype={'code':str})
    df = df.set_index('code')
    codes = df.index.tolist()
    
    update_requests = []
    
    for i,code in enumerate(codes):
        try:
            update_requests.append(
                UpdateOne(
                {'code':code},
                {'$set':{'issueprice':df.issueprice[code],
                        'timeToMarket':df.timeToMarket[code]}},
                upsert=True))
        except:
            print('code: %s, has problem' % code)
            
    if len(update_requests)>0:
        update_result = DB_CONN['basic'].bulk_write(update_requests, ordered=False)
        print('填充字段， 字段名: issueprice，数据集：%s，插入：%4d条，更新：%4d条' %
                  ('basic', update_result.upserted_count, update_result.modified_count), flush=True)


def fill_high_and_low_price_between(start, end):
    
    """
    for code in codes:
        timeToMarket = basic.find()
        
        for 
    """
    st_mark = ['st', 'ST', '*st', '*ST']
    codes = ts.get_stock_basics().index.tolist()
    total = len(codes)
    error_code = []

    for i,code in enumerate(codes):
        try:
            timeToMarket = DB_CONN['basic'].find_one({'code':code}, 
                    projection={'code':True, 'timeToMarket':True, '_id':False})['timeToMarket']
        except:
            error_code.append(code)
            continue
        
        daily_cursor = DB_CONN['daily'].find(
                {'code':code, 'date':{'$lte': end, '$gte': timeToMarket}, 'index':False},
                projection={'code':True, 'date':True, 'pre_close':True, '_id':False})
        
        update_requests = []
        
        for j,daily in enumerate(daily_cursor):
            date = daily['date']
            
            try:
                pre_close = daily['pre_close']
            except:
                if (j == 0) & (timeToMarket != date):
                    pass
#                    print('code: %s, time: %s, 数据初始日没有pre_close' % (code, date))
                elif timeToMarket == date:
                    print('code: %s, date: %s' % (code, date))
                    issueprice = DB_CONN['basic'].find_one({'code':code},
                              projection={'issueprice':True, '_id':False})['issueprice']
    
                    high_limit = np.round(np.round(issueprice * 1.2, 2) * 1.2, 2)
                    low_limit = np.round(np.round(issueprice * 0.8, 2) * 0.8, 2)
                    
                    update_requests.append(
                            UpdateOne({'code':code, 'date':date, 'index':False},
                                       {'$set':{'high_limit':high_limit, 'low_limit':low_limit}},
                                       upsert=True))
                else:
                    print('code: %s, time: %s, ipo_date: %s， 请速查原因' % (code, date, timeToMarket))
                    error_code.append(code)
                continue
            
            if date < '2016-08-09':
                _date = '2016-08-09'
            else:
                _date = date
                
            try:
                name = DB_CONN['basic'].find_one({'code':code, 'date':_date},
                              projection={'name':True, '_id':False})['name']
                last_name = name
            except:
                if j == 0:
                    name = DB_CONN['basic'].find_one({'code':code},
                      projection={'name':True, '_id':False})['name']
                    last_name = name
                else:
#                    print('code: %s, date: %s' % (code, date))
                    name = last_name
            
#            if timeToMarket == date:
#                
#                issueprice = DB_CONN['basic'].find_one({'code':code},
#                          projection={'issueprice':True, '_id':False})['issueprice']
#                
#                high_limit = np.round(np.round(issueprice * 1.2, 2) * 1.2, 2)
#                low_limit = np.round(np.round(issueprice * 0.8, 2) * 0.8, 2)

            if (name[:2] in st_mark) or (name[:3] in st_mark) :
                high_limit = np.round(pre_close * 1.05, 2)
                low_limit = np.round(pre_close * 0.95, 2)
            
            else:
                high_limit = np.round(pre_close * 1.1, 2)
                low_limit = np.round(pre_close * 0.9, 2)
                
            update_requests.append(
                    UpdateOne({'code':code, 'date':date, 'index':False},
                               {'$set':{'high_limit':high_limit, 'low_limit':low_limit}},
                               upsert=True))
        
        if len(update_requests)>0:
            update_result = DB_CONN['daily'].bulk_write(update_requests, ordered=False)
            print('涨跌停计算, 进度: (%s/%s), code：%s, 数据集：%s, 插入：%4d条, 更新：%4d条' %
                  (i+1, total, code, 'daily', update_result.upserted_count, update_result.modified_count), flush=True)
        
#        print('stock: %s high low limit complish, 进度: (%s/%s)' % (code, i+1, total), flush=True)

#  main funciton
if __name__ == '__main__':
    start = '2015-01-01'
    end = '2018-06-30'
    
    fill_issueprice_and_timeToMarket()
    tic = time.process_time()
    fill_high_and_low_price_between(start, end)
    toc = time.process_time()
    delta = toc - tic
    print(delta)