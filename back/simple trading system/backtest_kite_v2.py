# -*- coding: utf-8 -*-
"""
Created on Tue Oct 30 10:12:34 2018

@author: kite
"""

"""
完成策略的回测，绘制以沪深300为基准的收益曲线，计算年化收益、最大回撤、夏普比率
主要的方法包括:
    ma10_factor:
        is_k_up_break_ma10：当日K线是否上穿10日均线
        is_k_down_break_ma10：当日K线是否下穿10日均线
        compare_close_2_ma_10：工具方法，某日收盘价和当日对应的10日均线的关系
        
    backtest：回测主逻辑方法，从股票池获取股票后，按照每天的交易日一天天回测
"""

from pymongo import DESCENDING, ASCENDING
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from stock_pool_strategy import stock_pool, find_out_stocks
from database import DB_CONN
from ma10_factor import is_k_up_break_ma10, is_k_down_break_ma10
from stock_util import get_trading_dates, compute_drawdown, compute_sharpe_ratio, compute_ir
	

	
plt.rcParams['figure.figsize'] = [14, 8]
plt.rcParams['image.interpolation'] = 'nearest'
plt.rcParams['image.cmap'] = 'gray'
plt.style.use('ggplot')

CURRENT_MAX_DROP_RATE = 0.03
MAX_DROP_RATE = 0.05




def backtest(begin_date, end_date):
    """
    Arguments:
        begin_date: 回测开始日期
        end_date: 回测结束日期
    
    Returns:
        Account: 数据类型,dict
            
            init_assets : 初始资产, 默认1E7
            history_table : 交割单
            net_value : 每日净值
            final_net_value : 最终日净值
            profit : 收益
            day_profit : 每日收益
    """
    stop_lose_position_date_current = []
    stop_lose_position_date = []
    
    cash = 1E7
    single_position = 2E5

    # 时间为key的净值、收益和同期沪深基准
    df_profit = pd.DataFrame(columns=['net_value', 'profit', 'hs300'])
	# 时间为key的单日收益和同期沪深基准
    df_day_profit = pd.DataFrame(columns=['profit', 'hs300'])

    all_dates = get_trading_dates(begin_date, end_date)

    hs300_begin_value = DB_CONN['daily'].find_one(
        {'code': '000300', 'date': all_dates[0], 'index': True},
        projection={'close': True})['close']

    adjust_dates, date_codes_dict = stock_pool(begin_date, end_date)

    last_phase_codes = None
    this_phase_codes = None
    to_be_sold_codes = set()
    to_be_bought_codes = set()
    holding_code_dict = dict()
    last_date = None
    # 用于记录入场时间
    last_entry_dates = {}
    # 记录 交易记录
    history_table = pd.DataFrame()
    # 记录回测账户信息
    Account = {}
    Account['init_assets'] = cash
    
    # 前一天的总资产值，初始值为初始总资产
    last_total_capital = 1e7
    # 前一天的HS300值，初始值为第一天的值
    last_hs300_close = hs300_begin_value
    # 净值
    net_value = 1
    # 按照日期一步步回测
    count = 0
    for _date in all_dates:
        print('Backtest at %s.' % _date)

        # 当期持仓股票列表
        before_sell_holding_codes = list(holding_code_dict.keys())

        # 处理复权
        if last_date is not None and len(before_sell_holding_codes) > 0:
            last_daily_cursor = DB_CONN['daily'].find(
                {'code': {'$in': before_sell_holding_codes}, 'date': last_date, 'index': False},
                projection={'code': True, 'au_factor': True, '_id':False})

            code_last_aufactor_dict = dict()
            for last_daily in last_daily_cursor:
                code_last_aufactor_dict[last_daily['code']] = last_daily['au_factor']

            current_daily_cursor = DB_CONN['daily'].find(
                {'code': {'$in': before_sell_holding_codes}, 'date': _date, 'index': False},
                projection={'code': True, 'au_factor': True, '_id':False})

            for current_daily in current_daily_cursor:
                print(current_daily['code'], _date)
                current_aufactor = current_daily['au_factor']
                code = current_daily['code']
                before_volume = holding_code_dict[code]['volume']
                if code in code_last_aufactor_dict:
                    last_aufactor = code_last_aufactor_dict[code]
                    after_volume = int(before_volume * (current_aufactor / last_aufactor))
                    holding_code_dict[code]['volume'] = after_volume
                    print('持仓量调整：%s, %6d, %10.6f, %6d, %10.6f' %
                          (code, before_volume, last_aufactor, after_volume, current_aufactor))

        # 卖出
        print('待卖股票池：', to_be_sold_codes, flush=True)
        if len(to_be_sold_codes) > 0:
            sell_daily_cursor = DB_CONN['daily'].find(
                {'code': {'$in': list(to_be_sold_codes)}, 'date': _date, 'index': False, 'is_trading': True},
                projection={'open': True, 'code': True, 'low_limit':True}
            )

            for sell_daily in sell_daily_cursor:
                code = sell_daily['code']
                
                # 若开盘价是跌停价不准卖出
                open_price = sell_daily['open']
                low_limit = sell_daily['low_limit']
                
                if (code in before_sell_holding_codes) & (open_price > low_limit):
                    holding_stock = holding_code_dict[code]
                    holding_volume = holding_stock['volume']
                    sell_price = sell_daily['open']
                    sell_amount = holding_volume * sell_price
                    cash += sell_amount

                    cost = holding_stock['cost']
                    single_profit = (sell_amount - cost) * 100 / cost
                    
                    last_entry_dates[code] = None
                    
                    print('卖出 %s, %6d, %6.2f, %8.2f, %4.2f' %
                          (code, holding_volume, sell_price, sell_amount, single_profit))
                    
                    # 记录 交易记录
                    count += 1
                    _order = {'datetime':_date, 'code':code, 'price':sell_price,
                              'amount':-1 * holding_volume, 'cash':cash}
                    temp = pd.DataFrame(data=_order, index=[count])
                    history_table = pd.concat([history_table, temp])
                    
                    del holding_code_dict[code]
                    to_be_sold_codes.remove(code)

        print('卖出后，现金: %10.2f' % cash)

        # 买入
        print('待买股票池：', to_be_bought_codes, flush=True)
        if len(to_be_bought_codes) > 0:
            buy_daily_cursor = DB_CONN['daily'].find(
                {'code': {'$in': list(to_be_bought_codes)}, 'date': _date, 'index': False, 'is_trading': True},
                projection={'code': True, 'open': True, 'high_limit':True}
            )
            

            
            for buy_daily in buy_daily_cursor:

                # 若开盘价是涨停价不准买入
                open_price = buy_daily['open']
                high_limit = buy_daily['high_limit']
                
                if (cash > single_position) & (open_price < high_limit):
                    buy_price = buy_daily['open']
                    code = buy_daily['code']
                    volume = int(int(single_position / buy_price) / 100) * 100
                    buy_amount = buy_price * volume
                    cash -= buy_amount
                    holding_code_dict[code] = {
                        'volume': volume,
                        'cost': buy_amount,
                        'last_value': buy_amount}
                    
                    last_entry_dates[code] = _date

                    print('买入 %s, %6d, %6.2f, %8.2f' % (code, volume, buy_price, buy_amount))
                    
                    # 记录 交易记录
                    count += 1
                    _order = {'datetime':_date, 'code':code, 'price':buy_price,
                              'amount': volume, 'cash':cash}
                    temp = pd.DataFrame(data=_order, index=[count])
                    history_table = pd.concat([history_table, temp])
                    
        print('买入后，现金: %10.2f' % cash)

        # 持仓股代码列表
        holding_codes = list(holding_code_dict.keys())
        # 如果调整日，则获取新一期的股票列表
        if _date in adjust_dates:
            print('股票池调整日：%s，备选股票列表：' % _date, flush=True)

            # 暂存为上期的日期
            if this_phase_codes is not None:
                last_phase_codes = this_phase_codes
            this_phase_codes = date_codes_dict[_date]
            print(this_phase_codes, flush=True)

            # 找到所有调出股票代码，在第二日开盘时卖出
            if last_phase_codes is not None:
                out_codes = find_out_stocks(last_phase_codes, this_phase_codes)
                for out_code in out_codes:
                    if out_code in holding_code_dict:
                        to_be_sold_codes.add(out_code)

        # 检查是否有需要第二天卖出的股票
        for holding_code in holding_codes:
            if is_k_down_break_ma10(holding_code, _date):
                to_be_sold_codes.add(holding_code)
                
            """
            止损条件:
                1.当日亏损超过3%
                2.累计亏损超过10%
            满足其一就卖出
            注意，这里的回测逻辑是无法应用到模拟盘的，因为用了当天的收盘价去计算亏损；
            当然在回测里没问题，毕竟是第二天要卖出的股票，所以姑且当做收盘后的判断吧；
            """
            # 当天收盘价
            current_close = DB_CONN['daily'].find_one(
                {'code':holding_code, 'date':_date,'index':False})['close']
            
            # 买入时的价格和日期
            entry_date = last_entry_dates[holding_code]
            entry_daily_cursor = DB_CONN['daily'].find_one(
                {'code':holding_code, 'date':entry_date,'index':False}
            )
            entry_price = entry_daily_cursor['open']
            entry_date_close = entry_daily_cursor['close']
            
            if (entry_date == _date) & (((entry_price - entry_date_close) / entry_price) > CURRENT_MAX_DROP_RATE):
                to_be_sold_codes.add(holding_code)
                stop_lose_position_date_current.append(_date)
                
            elif ((entry_price - current_close) / entry_price) > MAX_DROP_RATE:
                to_be_sold_codes.add(holding_code)
                stop_lose_position_date.append(_date)

        # 检查是否有需要第二天买入的股票
        to_be_bought_codes.clear()
        if this_phase_codes is not None:
            for _code in this_phase_codes:
                if _code not in holding_codes and is_k_up_break_ma10(_code, _date):
                    to_be_bought_codes.add(_code)

        # 计算总资产
        total_value = 0
        holding_daily_cursor = DB_CONN['daily'].find(
            {'code': {'$in': holding_codes}, 'date': _date},
            projection={'close': True, 'code': True}
        )
        for holding_daily in holding_daily_cursor:
            code = holding_daily['code']
            holding_stock = holding_code_dict[code]
            value = holding_daily['close'] * holding_stock['volume']
            total_value += value

            profit = (value - holding_stock['cost']) * 100 / holding_stock['cost']
            one_day_profit = (value - holding_stock['last_value']) * 100 / holding_stock['last_value']

            holding_stock['last_value'] = value
            print('持仓: %s, %10.2f, %4.2f, %4.2f' %
                  (code, value, profit, one_day_profit))

        total_capital = total_value + cash

        hs300_current_value = DB_CONN['daily'].find_one(
            {'code': '000300', 'date': _date, 'index': True},
            projection={'close': True})['close']

        print('收盘后，现金: %10.2f, 总资产: %10.2f' % (cash, total_capital))
        last_date = _date
        net_value = round(total_capital / 1e7, 2)
        df_profit.loc[_date] = {
            'net_value': round(total_capital / 1e7, 4),
            'profit': round(100 * (total_capital - 1e7) / 1e7, 4),
            'hs300': round(100 * (hs300_current_value - hs300_begin_value) / hs300_begin_value, 4)
        }
        # 计算单日收益
        df_day_profit.loc[_date] = {
            'profit': round(100 * (total_capital - last_total_capital) / last_total_capital, 4),
            'hs300': round(100 * (hs300_current_value - last_hs300_close) / last_hs300_close, 4)
        }
        # 暂存当日的总资产和HS300，作为下一个交易日计算单日收益的基础
        last_total_capital = total_capital
        last_hs300_close = hs300_current_value
        
    Account['history_table'] = history_table    
    Account['net_value'] = df_profit['net_value']
    Account['final_net_value'] = net_value
    Account['profit'] = df_profit
    Account['day_profit'] = df_day_profit
    
    return Account
        


def account_analysis(Account, start, end):
    '''
    
    '''
    net_value = Account['net_value']
    final_net_value = Account['final_net_value']
    profit = Account['profit']
    day_profit = Account['day_profit']
    
    print('累积收益', flush=True)
    print(profit, flush=True)
    print('单日收益', flush=True)
    print(day_profit, flush=True)

    # 计算最大回撤
    drawdown = compute_drawdown(net_value)
    # 计算年化收益和夏普比率
    annual_profit, sharpe_ratio = compute_sharpe_ratio(final_net_value, day_profit)
    # 计算信息率
    ir = compute_ir(day_profit)

    print('回测结果 %s - %s，年化收益： %7.3f，最大回撤：%7.3f，夏普比率：%4.2f，信息率：%4.2f' %
          (start, end, annual_profit, drawdown, sharpe_ratio, ir))
#    print(np.sort(list(set(stop_lose_position_date))))
#    print(np.sort(list(set(stop_lose_position_date_current))))
    profit.index = pd.DatetimeIndex(profit.index, name = 'date')
    profit.plot(title='Backtest Result', y=['profit', 'hs300'], kind='line')
    plt.show()



if __name__ == "__main__":
    start = '2015-01-01'
    end = '2015-12-31'
    
    daily_hfq_col = DB_CONN['daily_hfq']
    if 'code_1_date_1_index_1_is_trading_1' not in daily_hfq_col.index_information().keys():
        daily_hfq_col.create_index(
                [('code', ASCENDING), ('date', ASCENDING), 
                 ('index', ASCENDING), ('is_tradingng', ASCENDING)]
                )
        
    
    daily_col = DB_CONN['daily']
    if 'code_1_date_1_index_1_is_trading_1' not in daily_col.index_information().keys():
        daily_col.create_index(
                [('code', ASCENDING), ('date', ASCENDING), 
                 ('index', ASCENDING), ('is_tradingng', ASCENDING)]
                )
    
    Account = backtest(start, end)
    
    account_analysis(Account, start, end)
    
    