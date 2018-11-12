
#  -*- coding: utf-8 -*-

"""
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
©Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
"""

import traceback

from pandas import DataFrame
from pymongo import UpdateOne, ASCENDING

from database import DB_CONN
from stock_util import get_all_codes


def compute(begin_date, end_date):
    """
    计算指定日期内的信号
    :param begin_date: 开始日期
    :param end_date: 结束日期
    """
    all_codes = get_all_codes()

    all_codes = ['000651']

    for code in all_codes:
        try:
            # 获取后复权的价格，使用后复权的价格计算MACD
            daily_cursor = DB_CONN['daily_hfq'].find(
                {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}, 'index': False},
                sort=[('date', ASCENDING)],
                projection={'date': True, 'close': True, '_id': False}
            )

            df_daily = DataFrame([daily for daily in daily_cursor])

            # 计算MB，盘后计算，这里用当日的Close
            df_daily['MB'] = df_daily['close'].rolling(20).mean()
            # 计算STD20
            df_daily['std'] = df_daily['close'].rolling(20).std()

            print(df_daily, flush=True)
            # 计算UP
            df_daily['UP'] = df_daily['MB'] + 2 * df_daily['std']
            # 计算down
            df_daily['DOWN'] = df_daily['MB'] - 2 * df_daily['std']

            print(df_daily, flush=True)

            # # 将日期作为索引
            # df_daily.set_index(['date'], inplace=True)
            #
            # # 将close移动一个位置，变为当前索引位置的前收
            # last_close = df_daily['close'].shift(1)
            #
            # # 突破上轨
            # shifted_up = df_daily['UP'].shift(1)
            # df_daily['up_mask'] = (last_close <= shifted_up) & (df_daily['close'] > shifted_up)
            #
            # # 突破下轨
            # shifted_down = df_daily['DOWN'].shift(1)
            # df_daily['down_mask'] = (last_close >= shifted_down) & (df_daily['close'] < shifted_down)
            #
            # # 过滤结果
            # df_daily = df_daily[df_daily['up_mask'] | df_daily['down_mask']]
            # df_daily.drop(['close', 'std', 'MB', 'UP', 'DOWN'], 1, inplace=True)
            #
            # # 将信号保存到数据库
            # update_requests = []
            # for index in df_daily.index:
            #     doc = {
            #         'code': code,
            #         'date': index,
            #         # 方向，向上突破 up，向下突破 down
            #         'direction': 'up' if df_daily.loc[index]['up_mask'] else 'down'
            #     }
            #     update_requests.append(
            #         UpdateOne(doc, {'$set': doc}, upsert=True))
            #
            # if len(update_requests) > 0:
            #     update_result = DB_CONN['boll'].bulk_write(update_requests, ordered=False)
            #     print('%s, upserted: %4d, modified: %4d' %
            #           (code, update_result.upserted_count, update_result.modified_count),
            #           flush=True)
        except:
            traceback.print_exc()


if __name__ == '__main__':
    compute(begin_date='2015-01-01', end_date='2015-12-31')