# -*- coding: utf-8 -*-
"""
Created on Wed Oct 24 22:41:42 2018

@author: kite
"""
import datetime
from pymongo import UpdateOne, ASCENDING
from database import DB_CONN
from stock_util import get_trading_dates, get_all_codes
import tushare as ts
import numpy as np
import pandas as pd
import requests
import json

start = '2015-01-01'
end = '2015-12-31'

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
def fill_issueprice():
    Sess = requests.Session()
    agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.89 Safari/537.36'
    headers = {'User-Agent':agent}
    url = 'http://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?type=XGSG_LB&token=70f12f2f4f091e459a279469fe49eca5&st=listingdate,securitycode&sr=-1&p={page}&ps=2000&js={"pages":(tp),"data":(x)}'
    html = Sess.get(url, headers=headers)
    result = json.loads(html.text)
    df = pd.DataFrame(result['data'])
    df = df.set_index('securitycode')
    doc_issueprice = dict(df.loc[:,'issueprice'])
    
    codes = doc_issueprice.keys()
    update_requests = []
    
    for code in codes:
        basic = DB_CONN['basic'].find({'code':code},
                       projection={'code':True, '_id':False})
        result = [b for b in basic]
        if len(result)>0:
            update_requests.append(
                UpdateOne(
                    {'code':code},
                    {'$set':{'issueprice':doc_issueprice[code]}},
                    upsert=True))
            
    if len(update_requests)>0:
        update_result = DB_CONN['basic'].bulk_write(update_requests, ordered=False)
        print('填充字段， 字段名: issueprice，数据集：%s，插入：%4d条，更新：%4d条' %
                  ('basic', update_result.upserted_count, update_result.modified_count), flush=True)


#for _date in trade_dates:
#    result = [b for b in basic.find({'timeToMarket':_date}, projection = {'code':True, 'date':True, 'timeToMarket':True, '_id':False})]
#    if len(result) > 0:
#        break
    
# 
"""
st_mark = ['st', 'ST', '*st', '*ST']

好，现在有发行价格了

开始计算涨跌停价格

按单天来算

先获取日期内的交易日

对交易日进行循环，按照每一个交易日

    填充所有股票的单个交易日涨跌停价格

def  填充所有股票的单个交易日涨跌停价格
    
    for code in codes:

        获取该股票在这个交易日下的名字,发行日期
        
        if 该股票的发行日期和当前日期相同，再获取该股票的发行价格，正常来说，是应该能获取得到:
    
            high_limit = np.round(np.round(issueprice * 1.2, 2) * 1.2, 2)
            low_limit = np.round(np.round(issueprice * 0.8, 2) * 0.8, 2)
            
        elif 如果股票的名字中前两位是st 或者 前 3位是 *st:
            high_limit = np.round(pre_close * 1.05, 2)
            low_limit = np.round(pre_close * 0.95, 2)
        else:
            high_limit = np.round(pre_close * 1.1, 2)
            low_limit = np.round(pre_close * 0.9, 2)
            
        将数据填充到daily
        
"""
def getBasics():
    before = datetime.datetime.now() - datetime.timedelta(days=1)
    while 1:
        last_trading_date = before.strftime('%Y-%m-%d')
        basic_cursor = DB_CONN['basic'].find(
            {'date': last_trading_date},
            projection={'code': True, 'name':True, 'timeToMarket': True, '_id': False},
            batch_size=5000)

        basics = [basic for basic in basic_cursor]

        if len(basics) > 0:
            break

        before -= datetime.timedelta(days=1)
    
    return basics

def fill_high_and_low_price_between(start, end):
    
    
    all_trades = get_trading_dates(start, end)
    codes = ts.get_stock_basics().index.tolist() # get_all_codes()
#    basics = getBasics()
    total = len(all_trades)
    for i,date in enumerate(all_trades):
        fill_high_and_low_price_at_one_date(codes, date)
        print('涨跌停计算进度: (%s/%s)' % (i+1, total))

def fill_high_and_low_price_at_one_date(codes, date):
    """
    
    """
    
    st_mark = ['st', 'ST', '*st', '*ST']
    update_requests = []
    
    if date < '2016-08-09':
        _date = '2016-08-08'
    # 用于获取code的基本信息
    else:
        _date = date
    
    for code in codes:
        basic_cursor = DB_CONN['basic'].find(
                {'code':code, 'date':_date}, 
                projection={'code':True, 'date':True,
               'name':True, 'timeToMarket':True, '_id':False})
        
        basics = [i for i in basic_cursor]
    
#    for basic in basics:
#        code = basic['code']
        
        daily_cursor = DB_CONN['daily'].find(
                {'code':code, 'date':date, 'index':False},
                projection={'code':True, 'date':True, 'pre_close':True, '_id':False})
        
        daily = [j for j in daily_cursor]
        
        if len(basics) <= 0 :
            continue
        
        if len(daily) <= 0:
#            print('日期：%s, 股票代码：%s, 没能正确填充pre_close数据' % (date, code), flush=True)
            continue
        
        basic = basics[0]
        daily = daily[0]
        
        try:
            pre_close = daily['pre_close']
            
        except:
#            print("日期：%s, 股票代码：%s, pre_close获取异常" % \
#                      (date, code),flush=True)
            continue
        
        if basic['timeToMarket'] == date:
            try:
                issueprice_cursor = [DB_CONN['basic'].find(
                        {'code':code, 'date':date},
                        projection={'code':True, 'date':True, 'issueprice':True, '_id':False})]
                issueprice = [ip for ip in issueprice_cursor]
            except:
#                print("日期：%s, 股票代码：%s, 发行价格获取异常" % \
#                      (date, code),flush=True)
                continue
            
            high_limit = np.round(np.round(issueprice * 1.2, 2) * 1.2, 2)
            low_limit = np.round(np.round(issueprice * 0.8, 2) * 0.8, 2)
            
        elif (basic['name'][:2] in st_mark) or (basic['name'][:3] in st_mark) :
            high_limit = np.round(pre_close * 1.05, 2)
            low_limit = np.round(pre_close * 0.95, 2)
        
        else:
            high_limit = np.round(pre_close * 1.1, 2)
            low_limit = np.round(pre_close * 0.9, 2)
        
        update_requests.append(
                UpdateOne(
                        {'code':code, 'date':date, 'index':False},
                        {'$set':{'high_limit':high_limit, 'low_limit':low_limit}},
                        upsert=True))
    
    if len(update_requests) > 0:
        update_result = DB_CONN['daily'].bulk_write(update_requests, ordered=False)
        print('涨跌停数据填充，日期：%s，数据集：%s，插入：%4d条，更新：%4d条' %
                  (date, 'daily', update_result.upserted_count, update_result.modified_count), flush=True)



#  main funciton
if __name__ == '__main__':
    start = '2015-01-01'
    end = '2017-12-31'
    
#    fill_issueprice()
    fill_high_and_low_price_between(start, end)