# -*- coding: utf-8 -*-
"""
Created on Tue Oct 30 12:52:18 2018

@author: kite
"""

from pymongo import ASCENDING, DESCENDING
from database import DB_CONN
from datetime import datetime, timedelta
import tushare as ts
import pandas as pd

def is_k_up_break_ma10(code, _date):
    """
    判断某只股票在某日是否满足K线上穿10日均线

    :param code: 股票代码
    :param _date: 日期
    :return: True/False
    """

    # 如果股票当日停牌或者是下跌，则返回False
    current_daily = DB_CONN['daily_hfq'].find_one(
        {'code': code, 'date': _date, 'index':False, 'is_trading': True})

    if current_daily is None:
        print('计算信号，K线上穿MA10，当日没有K线，股票 %s，日期：%s' % (code, _date), flush=True)
        return False

    # 计算MA10
    daily_cursor = DB_CONN['daily_hfq'].find(
        {'code': code, 'date': {'$lte': _date}, 'index':False},
        sort=[('date', DESCENDING)],
        limit=11,
        projection={'code': True, 'close': True, 'is_trading': True}
    )

    dailies = [x for x in daily_cursor]

    if len(dailies) < 11:
        print('计算信号，K线上穿MA10，前期K线不足，股票 %s，日期：%s' % (code, _date), flush=True)
        return False

    dailies.reverse()

    last_close_2_last_ma10 = compare_close_2_ma_10(dailies[0:10])
    current_close_2_current_ma10 = compare_close_2_ma_10(dailies[1:])

    print('计算信号，K线上穿MA10，股票：%s，日期：%s， 前一日 %s，当日：%s' %
          (code, _date, str(last_close_2_last_ma10), str(current_close_2_current_ma10)), flush=True)

    if last_close_2_last_ma10 is None or current_close_2_current_ma10 is None:
        return False

    # 判断收盘价和MA10的大小
    is_break = (last_close_2_last_ma10 <= 0) & (current_close_2_current_ma10 == 1)

    print('计算信号，K线上穿MA10，股票：%s，日期：%s， 前一日 %s，当日：%s，突破：%s' %
          (code, _date, str(last_close_2_last_ma10), str(current_close_2_current_ma10), str(is_break)), flush=True)

    return is_break


def is_k_down_break_ma10(code, _date):
    """
    判断某只股票在某日是否满足K线下穿10日均线

    :param code: 股票代码
    :param _date: 日期
    :return: True/False
    """

    # 如果股票当日停牌或者是下跌，则返回False
    current_daily = DB_CONN['daily_hfq'].find_one(
        {'code': code, 'date': _date, 'index':False, 'is_trading': True})
    if current_daily is None:
        print('计算信号，K线下穿MA10，当日没有K线，股票 %s，日期：%s' % (code, _date), flush=True)
        return False

    # 计算MA10
    daily_cursor = DB_CONN['daily_hfq'].find(
        {'code': code, 'date': {'$lte': _date}, 'index':False},
        sort=[('date', DESCENDING)],
        limit=11,
        projection={'code': True, 'close': True, 'is_trading': True}
    )

    dailies = [x for x in daily_cursor]

    if len(dailies) < 11:
        print('计算信号，K线下穿MA10，前期K线不足，股票 %s，日期：%s' % (code, _date), flush=True)
        return False

    dailies.reverse()

    last_close_2_last_ma10 = compare_close_2_ma_10(dailies[0:10])
    current_close_2_current_ma10 = compare_close_2_ma_10(dailies[1:])

    if last_close_2_last_ma10 is None or current_close_2_current_ma10 is None:
        return False

    # 判断收盘价和MA10的大小
    is_break = (last_close_2_last_ma10 >= 0) & (current_close_2_current_ma10 == -1)

    print('计算信号，K线下穿MA10，股票：%s，日期：%s， 前一日 %s，当日：%s, 突破：%s' %
          (code, _date, str(last_close_2_last_ma10), str(current_close_2_current_ma10), str(is_break)), flush=True)

    return is_break


def compare_close_2_ma_10(dailies):
    """
    比较当前的收盘价和MA10的关系
    :param dailies: 日线列表，10个元素，最后一个是当前交易日
    :return: 0 相等，1 大于， -1 小于, None 结果未知
    """
    current_daily = dailies[9]
    close_sum = 0
    code = None
    for daily in dailies:
        # 10天当中，只要有一天停牌则返回False
        if 'is_trading' not in daily or daily['is_trading'] is False:
            return None

        # 用后复权累计
        close_sum += daily['close']
        code = daily['code']

    # 计算MA10
    ma_10 = close_sum / 10

    # 判断收盘价和MA10的大小
    post_adjusted_close = current_daily['close']
    differ = post_adjusted_close - ma_10

    # print('计算信号，股票： %s, 收盘价：%7.2f, MA10: %7.2f, 差值：%7.2f' %
    #       (code, post_adjusted_close, ma_10, differ), flush=True)
    if differ > 0:
        return 1
    elif differ < 0:
        return -1
    else:
        return 0