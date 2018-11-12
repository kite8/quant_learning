# -*- coding: utf-8 -*-
"""
Created on Fri Nov  2 15:19:45 2018

@author: kite
"""

import datetime, time
from pymongo import UpdateOne, ASCENDING, UpdateMany
from database import DB_CONN
from stock_util import get_trading_dates, get_all_codes
import tushare as ts
import numpy as np
import pandas as pd
import requests
import json
import datetime

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
    df = pd.read_excel('data/ipo_info.xlsx', header=0, dtype={'code':str})
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

def fixing_is_st(start, end):
    # 第一阶段
    df = pd.read_excel('data/stock_basic.xlsx', header=0, dtype={'code':str})
    df = df.set_index('code')
    codes = df[df['是否ST过'] == 1].index.tolist()
    total = len(codes)
#    all_dates = get_trading_dates(start, end)
    
    daily = DB_CONN['daily']
    
    excel_name = 'data/st_info.xlsx'
    for i in range(4):
        if i == 0:
            all_dates = get_trading_dates('2015-01-01', '2015-12-31')
        elif i == 1:
            all_dates = get_trading_dates('2016-01-01', '2016-12-31')
        if i == 2:
            all_dates = get_trading_dates('2017-01-01', '2017-12-31')
        elif i == 3:
            all_dates = get_trading_dates('2018-01-01', '2018-09-30')
        
        
        print('数据读取中')
        df = pd.read_excel(excel_name, i, header=0, dtype={'code':str})
        df = df.set_index(['code','state'])
        df.columns = df.columns.astype(np.datetime64)
        df.columns = df.columns.to_period('D')
        df.columns = df.columns.astype('str')
        print('数据读取完毕')
        
        
        for j, code in enumerate(codes):
            update_requests = []
            for date in all_dates:
                try:
                    st_state = df.xs([code])[date]['是否ST']
                    sst_state = df.xs([code])[date]['是否*ST']
                    if (st_state == '否') and (sst_state == '否'):
                        is_st_flag = False
                    else:
                        is_st_flag = True
                    
                    update_requests.append(
                        UpdateOne(
                                {'code':code, 'date':date, 'index':False},
                                {'$set':{'is_st':is_st_flag}}
                                )
                        )
                except:
                    print('something is wrong, code : %s, date : %s' % (code, date))
                        
            if len(update_requests)>0:
                update_result = daily.bulk_write(update_requests, ordered=False)
                print('第%s年填充进度: %s/%s， 字段名: is_st，数据集：%s，插入：%4d条，更新：%4d条' %
                          (i+1, j+1, total, 'daily', update_result.upserted_count, update_result.modified_count), flush=True)
               
            

def fill_high_and_low_price_between(start, end):
    
    """
    for code in codes:
        timeToMarket = basic.find()
        
        for 
    """
#    st_mark = ['st', 'ST', '*st', '*ST']
    codes = ts.get_stock_basics().index.tolist()
    _df = pd.read_excel('data/stock_basic.xlsx', header=0, dtype={'code':str})
    _df = _df.set_index('code')
    st_codes = _df[_df['是否ST过'] == 1].index.tolist()
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
#                    print('code: %s, date: %s' % (code, date))
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
            
#            if date < '2016-08-09':
#                _date = '2016-08-09'
#            else:
#                _date = date
#                
#            try:
#                name = DB_CONN['basic'].find_one({'code':code, 'date':_date},
#                              projection={'name':True, '_id':False})['name']
#                last_name = name
#            except:
#                if j == 0:
#                    name = DB_CONN['basic'].find_one({'code':code},
#                      projection={'name':True, '_id':False})['name']
#                    last_name = name
#                else:
##                    print('code: %s, date: %s' % (code, date))
#                    name = last_name
            
#            if timeToMarket == date:
#                
#                issueprice = DB_CONN['basic'].find_one({'code':code},
#                          projection={'issueprice':True, '_id':False})['issueprice']
#                
#                high_limit = np.round(np.round(issueprice * 1.2, 2) * 1.2, 2)
#                low_limit = np.round(np.round(issueprice * 0.8, 2) * 0.8, 2)

#            if daily['is_st'] :
            if code in st_codes:
                st_flag = DB_CONN['daily'].find_one({'code':code, 'date':date, 'index':False})['is_st']
                if st_flag:
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
    daily_col = DB_CONN['daily']
    if 'code_1_index_1' not in daily_col.index_information().keys():
        daily_col.create_index(
            [('code', ASCENDING), ('index', ASCENDING)]
                )
    start = '2015-01-01'
    end = '2018-09-30'
    tic = time.process_time()
    fixing_is_st(start, end)
#    fill_issueprice_and_timeToMarket()
    fill_high_and_low_price_between(start, end)
    toc = time.process_time()
    delta = toc - tic
    print(delta)