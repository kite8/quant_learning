# -*- coding: utf-8 -*-# -*- coding: utf-8 -*-
"""
Created on Sun Oct 14 19:52:31 2018

@author: kite
"""
import datetime
from pymongo import UpdateOne, ASCENDING
from database import DB_CONN
from stock_util import get_trading_dates, get_all_codes

def fill_is_trading_between(start, end):
    """
    
    """
    pass
    all_dates = get_trading_dates(start, end)
    
    for date in all_dates:
        fill_single_date_is_trading(date, 'daily')
        fill_single_date_is_trading(date, 'daily_hfq')
        
def fill_is_trading(date=None):
    """
    为日线数据增加is_trading字段，表示是否交易的状态，True - 交易  False - 停牌
    从Tushare来的数据不包含交易状态，也不包含停牌的日K数据，为了系统中使用的方便，我们需要填充停牌是的K数据。
    一旦填充了停牌的数据，那么数据库中就同时包含了停牌和交易的数据，为了区分这两种数据，就需要增加这个字段。

    在填充该字段时，要考虑到是否最坏的情况，也就是数据库中可能已经包含了停牌和交易的数据，但是却没有is_trading
    字段。这个方法通过交易量是否为0，来判断是否停牌
    """
    if date is None:
        all_dates = get_trading_dates()
    else:
        all_dates = [date]
    for date in all_dates:
        fill_single_date_is_trading(date, 'daily')
        fill_single_date_is_trading(date, 'daily_hfq')
    
    
def fill_single_date_is_trading(date, collection_name):
    """
    
    """
    print('填充字段， 字段名: is_trading，日期：%s，数据集：%s' %
          (date, collection_name), flush=True)
    daily_cursor = DB_CONN[collection_name].find(
            {'date':date},
            projection={'code':True, 'volume':True, 'index':True, '_id':False},
            batch_size=1000)
    
    update_requests = []
    
    for daily in daily_cursor:
        is_trading = True
        
        if daily['volume'] == 0:
            is_trading = False
            
        update_requests.append(
                UpdateOne(
                    {'code':daily['code'], 'date':daily['code'], 'index':daily['index']},
                    {'$set': {'is_trading':is_trading}}))
    
    if len(update_requests) > 0:
        update_result = DB_CONN[collection_name].bulk_write(update_requests, ordered=False)
        print('填充字段， 字段名: is_trading，日期：%s，数据集：%s，更新：%4d条' % 
              (date, collection_name, update_result.modified_count), flush=True)
    
def fill_daily_k_at_suspension_days(start=None, end=None):
    """
    :param start:
    :param end:
    :return:
    """
    before = datetime.datetime.now() - datetime.timedelta(days=1)
    while 1:
        last_trading_date = before.strftime('%Y-%m-%d')
        basic_cursor = DB_CONN['basic'].find(
            {'date': last_trading_date},
            projection={'code': True, 'timeToMarket': True, '_id': False},
            batch_size=5000)

        basics = [basic for basic in basic_cursor]

        if len(basics) > 0:
            break

        before -= datetime.timedelta(days=1)

    all_dates = get_trading_dates(start, end)

    fill_daily_k_at_suspension_days_at_date_one_collection(
        basics, all_dates, 'daily')
    fill_daily_k_at_suspension_days_at_date_one_collection(
        basics, all_dates, 'daily_hfq')
    
def fill_daily_k_at_suspension_days_at_date_one_collection():
    """
    
    """
    pass























