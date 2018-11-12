
#  -*- coding: utf-8 -*-

from database import DB_CONN
from stock_util import get_all_codes
from pymongo import ASCENDING, UpdateOne
from pandas import DataFrame
import traceback


def compute_macd(begin_date, end_date):
    codes = get_all_codes()
    # 短时
    short = 12
    # 长时
    long = 26

    for code in codes:
        try:
            # 获取后复权的价格，使用后复权的价格计算MACD
            daily_cursor = DB_CONN['daily_hfq'].find(
                {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}, 'index': False},
                sort=[('date', ASCENDING)],
                projection={'date': True, 'close': True, '_id': False}
            )

            df_daily = DataFrame([daily for daily in daily_cursor])

            df_daily.set_index(['date'], 1, inplace=True)

            # 计算EMA
            index = 0
            EMA1 = []
            EMA2 = []
            for date in df_daily.index:
                if index == 0:
                    # 初始化短时EMA和长时EMA
                    EMA1.append(df_daily.loc[date]['close'])
                    EMA2.append(df_daily.loc[date]['close'])
                else:
                    EMA1.append(2/(short + 1) * (df_daily.loc[date]['close'] - EMA1[index - 1]) + EMA1[index - 1])
                    EMA2.append(2/(long + 1) * (df_daily.loc[date]['close'] - EMA2[index - 1]) + EMA2[index - 1])

                index += 1

            df_daily['EMA1'] = EMA1
            df_daily['EMA2'] = EMA2

            # 计算DIFF，短时EMA - 长时EMA
            df_daily['DIFF'] = df_daily['EMA1'] - df_daily['EMA2']

            # 计算DEA EMA(DIFF，M)
            m = 9
            index = 0
            DEA = []
            for date in df_daily.index:
                if index == 0:
                    DEA.append(df_daily.loc[date]['DIFF'])
                else:
                    # M = 9 DEA = EMA(DIFF, 9)
                    DEA.append(2/(m+1) * (df_daily.loc[date]['DIFF'] - DEA[index - 1]) + DEA[index - 1])
                index += 1

            df_daily['DEA'] = DEA

            df_daily['delta'] = df_daily['DIFF'] - df_daily['DEA']
            df_daily['pre_delta'] = df_daily['delta'].shift(1)
            # 金叉，DIFF下穿DEA
            df_daily_gold = df_daily[(df_daily['pre_delta'] >= 0) & (df_daily['delta'] < 0)]
            # 死叉，DIFF上穿DEA
            df_daily_dead = df_daily[(df_daily['pre_delta'] <= 0) & (df_daily['delta'] > 0)]

            # 保存结果到数据库
            update_requests = []
            for date in df_daily_gold.index:
                update_requests.append(UpdateOne(
                    {'code': code, 'date': date},
                    {'$set': {'code':code, 'date': date, 'signal': 'gold'}},
                    upsert=True))

            for date in df_daily_dead.index:
                update_requests.append(UpdateOne(
                    {'code': code, 'date': date},
                    {'$set': {'code':code, 'date': date, 'signal': 'dead'}},
                    upsert=True))

            if len(update_requests) > 0:
                update_result = DB_CONN['macd'].bulk_write(update_requests, ordered=False)
                print('Save MACD, 股票代码：%s, 插入：%4d, 更新：%4d' %
                      (code, update_result.upserted_count, update_result.modified_count), flush=True)
        except:
            print('错误发生： %s' % code, flush=True)
            traceback.print_exc()

def is_macd_gold(code, date):
    count = DB_CONN['macd'].count({'code': code, 'date': date, 'signal': 'gold'})
    return count == 1

def is_macd_dead(code, date):
    count = DB_CONN['macd'].count({'code': code, 'date': date, 'signal': 'dead'})
    return count == 1


if __name__ == '__main__':
    compute_macd('2015-01-01', '2015-12-31')

