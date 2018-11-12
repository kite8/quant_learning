
#  -*- coding: utf-8 -*-

"""
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
©Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
"""

from pymongo import ASCENDING, DESCENDING
from database import DB_CONN
from datetime import datetime, timedelta
import tushare as ts
import numpy as np
import pandas as pd

def compute_drawdown(net_values):
    """
    计算最大回撤
    :param net_values: 净值列表
    """
    # 最大回撤初始值设为0
    max_drawdown = 0
    size = len(net_values)
    index = 0
    # 双层循环找出最大回撤
    for net_value in net_values:
        for sub_net_value in net_values[index:]:
            drawdown = 1 - sub_net_value / net_value
            if drawdown > max_drawdown:
                max_drawdown = drawdown

        index += 1

    return max_drawdown

def dynamic_max_drawdown(net_value):
    nums = len(net_value)
    maxDrawDown = pd.Series()
    for i in range(nums):
        C = net_value[:i].max()
        if C == net_value[i]:
            maxDrawDown.loc[i] = 0
        else:
            maxDrawDown.loc[i] = abs((C - net_value[i]) / C)
    return maxDrawDown


def compute_annual_profit(trading_days, net_value):
    """
    计算年化收益
    """

    annual_profit = 0
    if trading_days > 0:
        # 计算年数
        years = trading_days / 245
        # 计算年化收益
        annual_profit = pow(net_value, 1 / years) - 1

    annual_profit = np.round(annual_profit * 100, 2)

    return annual_profit


def compute_sharpe_ratio(net_value, df_day_profit):
    """
    计算夏普比率
    :param net_value: 最后的净值
    :param df_day_profit: 单日的收益，profit：策略单日收益，hs300：沪深300的单日涨跌幅
    """

    # 总交易日数
    trading_days = df_day_profit.index.size

    # 计算单日收益标准差
    profit_std = np.round(df_day_profit['profit'].std(), 4)
    print(profit_std)

    # 年化收益
    annual_profit = compute_annual_profit(trading_days, net_value)

    # 夏普比率
    sharpe_ratio = (annual_profit - 4.75) / (profit_std * pow(245, 1 / 2))

    return annual_profit, sharpe_ratio


def compute_ir(df_day_profit):
    """
    计算信息率
    :param df_day_profit: 单日收益，profit - 策略收益 hs300 - 沪深300的
    :return: 信息率
    """
    # 计算单日的无风险收益率
    base_profit = 4.5 / 245

    df_extra_profit = pd.DataFrame(columns=['profit', 'hs300'])
    df_extra_profit['profit'] = df_day_profit['profit'] - base_profit
    df_extra_profit['hs300'] = df_day_profit['hs300'] - base_profit

    # 计算策略的单日收益和基准单日涨跌幅的协方差
    cov = df_extra_profit['profit'].cov(df_extra_profit['hs300'])
    # 计算策略收益和基准收益沪深300的方差
    var_profit = df_extra_profit['profit'].var()
    var_hs300 = df_extra_profit['hs300'].var()
    # 计算Beta
    beta = cov / var_hs300
    # 残差风险
    omega = pow((var_profit - pow(beta, 2) * var_hs300) * 245, 1/2)
    # Alpha
    alpha = (df_extra_profit['profit'].mean() - (beta * df_extra_profit['hs300'].mean())) * 245
    # 信息率
    ir = np.round(alpha / omega, 4)

    print('cov：%10.4f，var_profit：%10.4f，var_hs300：%10.4f，beta：%10.4f，omega：%10.4f，alpha：%10.4f，ir：%10.4f' %
          (cov, var_profit, var_hs300, beta, omega, alpha, ir), flush=True)

    return ir

def get_trading_dates(begin_date=None, end_date=None):
    """
    获取指定日期范围的按照正序排列的交易日列表
    如果没有指定日期范围，则获取从当期日期向前365个自然日内的所有交易日

    :param begin_date: 开始日期
    :param end_date: 结束日期
    :return: 日期列表
    """
    # 开始日期，默认今天向前的365个自然日
    now = datetime.now()
    if begin_date is None:
        one_year_ago = now - timedelta(days=365)
        begin_date = one_year_ago.strftime('%Y-%m-%d')

    # 结束日期默认为今天
    if end_date is None:
        end_date = now.strftime('%Y-%m-%d')
    
    # 下面这个方法是很好，起码是本地的，不需要联网，前提是有下载这段时间的数据
    # 因为默认下载的是2015年的，所以需要再下载2017年的数据
#    daily_cursor = DB_CONN.daily.find(
#        {'code': '000001', 'date': {'$gte': begin_date, '$lte': end_date}, 'index': True},
#        sort=[('date', ASCENDING)],
#        projection={'date': True, '_id': False})
#
#    dates = [x['date'] for x in daily_cursor]
#    
#    if len(dates) == 0:
    all_trade_dates = ts.trade_cal()
    trade_dates = all_trade_dates[(all_trade_dates.isOpen == 1) & \
                                 (all_trade_dates.calendarDate >= begin_date) & \
                                 (all_trade_dates.calendarDate <= end_date)]
    dates = trade_dates.calendarDate.tolist()

    return dates


def get_all_codes(date=None):
    """
    获取某个交易日的所有股票代码列表，如果没有指定日期，则从当前日期一直向前找，直到找到有
    数据的一天，返回的即是那个交易日的股票代码列表

    :param date: 日期
    :return: 股票代码列表
    """

    datetime_obj = datetime.now()
    if date is None:
        date = datetime_obj.strftime('%Y-%m-%d')

    codes = []
    while len(codes) == 0:
        code_cursor = DB_CONN.basic.find(
            {'date': date},
            projection={'code': True, '_id': False})

        codes = [x['code'] for x in code_cursor]

        datetime_obj = datetime_obj - timedelta(days=1)
        date = datetime_obj.strftime('%Y-%m-%d')

    return codes
