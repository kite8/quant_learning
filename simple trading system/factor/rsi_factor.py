
#  -*- coding: utf-8 -*-

"""
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
©Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
"""

from pandas import DataFrame
from pymongo import ASCENDING, UpdateOne

from database import DB_CONN
import tushare as ts
# from stock_util import get_all_codes


def compute_rsi(begin_date, end_date):
    codes = ts.get_stock_basics().index.tolist() # get_all_codes()

    # 计算RSI
    N = 12
    for code in codes:
        try:
            # 获取后复权的价格，使用后复权的价格计算RSI
            daily_cursor = DB_CONN['daily_hfq'].find(
                {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}, 'index': False},
                sort=[('date', ASCENDING)],
                projection={'date': True, 'close': True, '_id': False}
            )

            df_daily = DataFrame([daily for daily in daily_cursor])

            if df_daily.index.size < N:
                print('数据量不够： %s' % code, flush=True)
                continue

            df_daily.set_index(['date'], 1, inplace=True)
            df_daily['pre_close'] = df_daily['close'].shift(1)
            df_daily['change_pct'] = (df_daily['close'] - df_daily['pre_close']) * 100 / df_daily['pre_close']
            # 保留上涨的日期
            df_daily['up_pct'] = DataFrame({'up_pct': df_daily['change_pct'], 'zero': 0}).max(1)

            # 计算RSI
            df_daily['RSI'] = df_daily['up_pct'].rolling(N).mean() / abs(df_daily['change_pct']).rolling(N).mean() * 100

            # 移位
            df_daily['PREV_RSI'] = df_daily['RSI'].shift(1)

            # # 超买，RSI下穿80，作为卖出信号
            df_daily_over_bought = df_daily[(df_daily['RSI'] < 80) & (df_daily['PREV_RSI'] >= 80)]
            # # 超卖，RSI上穿20，作为买入信号
            df_daily_over_sold = df_daily[(df_daily['RSI'] > 20) & (df_daily['PREV_RSI'] <= 20)]
            #
            # # 保存结果到数据库
            update_requests = []
            for date in df_daily_over_bought.index:
                update_requests.append(UpdateOne(
                    {'code': code, 'date': date},
                    {'$set': {'code':code, 'date': date, 'signal': 'over_bought'}},
                    upsert=True))

            for date in df_daily_over_sold.index:
                update_requests.append(UpdateOne(
                    {'code': code, 'date': date},
                    {'$set': {'code':code, 'date': date, 'signal': 'over_sold'}},
                    upsert=True))

            if len(update_requests) > 0:
                update_result = DB_CONN['rsi'].bulk_write(update_requests, ordered=False)
                print('Save RSI, 股票代码：%s, 插入：%4d, 更新：%4d' %
                      (code, update_result.upserted_count, update_result.modified_count), flush=True)
        except:
            print('错误发生： %s' % code, flush=True)


def is_rsi_over_sold(code, date):
    count = DB_CONN['rsi'].count({'code': code, 'date': date, 'signal': 'over_sold'})
    return count == 1


def is_rsi_over_bought(code, date):
    count = DB_CONN['rsi'].count({'code': code, 'date': date, 'signal': 'over_bought'})
    return count == 1


if __name__ == '__main__':
    rsi_col = DB_CONN['rsi']
    if 'code_1_date_1' not in rsi_col.index_information().keys():
        rsi_col.create_index(
            [('code', ASCENDING), ('date', ASCENDING)])
    
    if 'code_1_date_1_signal_1' not in rsi_col.index_information().keys():
        rsi_col.create_index(
            [('code', ASCENDING), ('date', ASCENDING), ('signal', ASCENDING)])
    
    compute_rsi('2015-01-01', '2018-12-31')

