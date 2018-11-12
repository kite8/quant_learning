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
import pickle
from pymongo import DESCENDING, ASCENDING
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from stock_pool_strategy import stock_pool, find_out_stocks
from database import DB_CONN
from factor.ma10_factor import is_k_up_break_ma10, is_k_down_break_ma10
from stock_util import get_trading_dates, compute_drawdown, dynamic_max_drawdown, compute_sharpe_ratio, compute_ir
	

	
plt.rcParams['figure.figsize'] = [14, 8]
plt.rcParams['image.interpolation'] = 'nearest'
plt.rcParams['image.cmap'] = 'gray'
plt.style.use('ggplot')

SINGLE_DAY_MAX_DROP_RATE = 0.03
MAX_DROP_RATE = 0.1
ATR_WIN = 14
ATR_RATIO = 2
RISK_RATIO = 0.01



def backtest(begin_date, end_date, stop_method=None, pos_method='equal'):
    """
    Arguments:
        begin_date: 回测开始日期
        end_date: 回测结束日期
        stop_method : 止损方式
            None : 无止损
            fixed : 固定比例止损
            float : 浮动止损
            ATR_float_dynamic : 动态ATR浮动止损
            ATR_float_static : 静态ATR浮动止损
        pos_method : 头寸分配方式
            equal : 均仓分配头寸
            atr : 按照ATR分配头寸
    
    Returns:
        Account: 数据类型,dict
            
            init_assets : 初始资产, 默认1E7
            history_table : 交割单
            net_value : 每日净值
            final_net_value : 最终日净值
            profit : 收益
            day_profit : 每日收益
            positions : 每日仓位
            stop_loss : 止损的方式和止损参数
            position_manage : 头寸管理方式和相关参数
    """
    # 记录止损时间点
#    stop_lose_position_date_current = []
#    stop_lose_position_date = []
    
    # 记录回测账户信息
    Account = {}
    
    # 仓位相关的初始化
    position_manage = {}
    if pos_method == 'equal':
        single_position = 2E5
        position_manage['头寸分配方式'] = '均仓'
        Account['position_manage'] = position_manage
    elif pos_method == 'atr':
        position_manage['头寸分配方式'] = 'ATR分配头寸'
        position_manage['ATR_WIN'] = ATR_WIN
        position_manage['RISK_RATIO'] = RISK_RATIO
        Account['position_manage'] = position_manage
        
    positions = pd.Series() # 记录每日仓位信息
    stop_loss = {}
    
    cash = 1E7
    init_assets = cash
    Account['init_assets'] = init_assets
    Account['start'] = begin_date
    Account['end'] = end_date
    
    if stop_method is None:
        Account['stop_loss'] = '无止损'
    elif stop_method == 'fixed':
        stop_loss['单日跌幅比例'] = SINGLE_DAY_MAX_DROP_RATE
        stop_loss['累计跌幅比例'] = MAX_DROP_RATE
        stop_loss['止损方式'] = '固定比例止损'
        Account['stop_loss'] = stop_loss
    elif stop_method == 'float':    
        stop_loss['跌幅比例'] = MAX_DROP_RATE
        stop_loss['止损方式'] = '浮动止损'
        Account['stop_loss'] = stop_loss
    elif (stop_method == 'ATR_float_dynamic') or (stop_method == 'ATR_float_static'):
        stop_loss['ATR_WIN'] = ATR_WIN
        stop_loss['ATR_RATIO'] = ATR_RATIO
        stop_loss['止损方式'] = '动态ATR浮动止损'
        Account['stop_loss'] = stop_loss


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
    
    last_entry_dates = {} # 用于记录入场时间
    history_table = pd.DataFrame() # 记录 交割单
    

    
    last_total_capital = 1e7 # 前一天的总资产值，初始值为初始总资产
    last_hs300_close = hs300_begin_value # 前一天的HS300值，初始值为第一天的值
    net_value = 1 # 净值
    
    count = 0
    # 按照日期一步步回测
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
                code = buy_daily['code']
                
                # ===========================ATR分配头寸 code start=========================
                if pos_method == 'atr':
                    ATR = calc_ATR(code, _date)
                    single_position = init_assets * RISK_RATIO / (ATR_RATIO * ATR) // 100 * 100
                
                if (cash > single_position) & (open_price < high_limit):
                    buy_price = buy_daily['open']
                    
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
            
            if stop_method is not None:
                stop_loss_positions(holding_code, _date, last_entry_dates, 
                                    to_be_sold_codes, stop_method)

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
        positions.loc[_date] = total_value / total_capital

        hs300_current_value = DB_CONN['daily'].find_one(
            {'code': '000300', 'date': _date, 'index': True},
            projection={'close': True})['close']

        print('收盘后，现金: %10.2f, 总资产: %10.2f' % (cash, total_capital))
        last_date = _date
        net_value = np.round(total_capital / 1e7, 4)
        df_profit.loc[_date] = {
            'net_value': np.round(total_capital / 1e7, 4),
            'profit': np.round(100 * (total_capital - 1e7) / 1e7, 4),
            'hs300': np.round(100 * (hs300_current_value - hs300_begin_value) / hs300_begin_value, 4)
        }
        # 计算单日收益
        df_day_profit.loc[_date] = {
            'profit': np.round(100 * (total_capital - last_total_capital) / last_total_capital, 4),
            'hs300': np.round(100 * (hs300_current_value - last_hs300_close) / last_hs300_close, 4)
        }
        # 暂存当日的总资产和HS300，作为下一个交易日计算单日收益的基础
        last_total_capital = total_capital
        last_hs300_close = hs300_current_value
        
    Account['history_table'] = history_table    
    Account['net_value'] = df_profit['net_value']
    Account['final_net_value'] = net_value
    Account['profit'] = df_profit
    Account['day_profit'] = df_day_profit
    Account['positions'] = positions
    
    return Account

def stop_loss_positions(holding_code, _date, last_entry_dates, to_be_sold_codes, method):
    """
    注意，这里回测中的止损逻辑，应当看做成收盘后的处理,因为盘中不可能知道收盘价的!!
    
    1.固定比例止损
        满足以下其一就进行全部止损:
        1.单日亏损超过3%;
        2.累计亏损超过10%
    
    
    2.固定比例浮动止损:
        回看区间 -- 自买入日到当前回测日
        条件 -- 回看区间内的最高价下跌超过一定比例, 就进行止损;
    
    3.动态波动率浮动止损:
        回看区间 -- 自买入日到当前回测日
        条件 -- 回看区间内的最高价下跌, 超过回测日ATR的倍数, 就进行止损;
    """
    # 当前收盘价,使用后复权
    current_cursor = DB_CONN['daily_hfq'].find_one(
        {'code':holding_code, 'date':_date,'index':False})
    
    # 买入时的价格和日期
    entry_date = last_entry_dates[holding_code]
    current_close = current_cursor['close']
    
    interval_cursor = DB_CONN['daily_hfq'].find(
            {'code':holding_code, 'date':{'$gte': entry_date, '$lte': _date}, 'index':False},
            projection={'high':True, '_id':False}
            )
    high = max([x['high'] for x in interval_cursor])
    
    # ===========================固定比例止损 code start=========================    
    if method == 'fixed':
        current_open = current_cursor['open']
        
        entry_daily_cursor = DB_CONN['daily_hfq'].find_one(
            {'code':holding_code, 'date':entry_date,'index':False}
        )
        entry_price = entry_daily_cursor['open']
        
        if ((current_open - current_close) / current_open) > SINGLE_DAY_MAX_DROP_RATE:
            to_be_sold_codes.add(holding_code)
            
        elif ((entry_price - current_close) / entry_price) > MAX_DROP_RATE:
            to_be_sold_codes.add(holding_code)
    
    # ===========================固定比例浮动止损 code start===================
    elif method == 'float':                
        if (high - current_close) > MAX_DROP_RATE:
            to_be_sold_codes.add(holding_code)
    
    # ===========================波动率浮动止损 code start=========================
    # 运用实时(动态)波动率浮动止损
    elif method == 'ATR_float_dynamic':
        ATR = calc_ATR(holding_code, _date)
        if ATR is not None:
            if (high - current_close) > ATR * ATR_RATIO:
                to_be_sold_codes.add(holding_code)
    elif method == 'ATR_float_static':
        ATR = calc_ATR(holding_code, entry_date)
        if ATR is not None:
            if (high - current_close) > ATR * ATR_RATIO:
                to_be_sold_codes.add(holding_code)
            
                    
        

def calc_ATR(code, date):
    ATR_cursor = DB_CONN['daily'].find(
            {'code':code, 'date':{'$lte': date}, 'index':False},
            projection={'open':True, 'high':True, 'low':True, 'close':True, '_id':False},
            limit = ATR_WIN+1)
    if ATR_cursor is None:
        return None
    df = pd.DataFrame([r for r in ATR_cursor])
    
    if len(df) != ATR_WIN+1:
        return None
    
    df = df.assign(pdc = df['close'].shift(1))
    tr = df.apply(lambda x : max( x['high'] - x['low'], abs(x["high"] - x["pdc"]), 
                                 abs(x['low'] - x['pdc'])), axis=1)
    ATR = tr[- ATR_WIN :].mean()
    return ATR

def account_analysis(Account, start, end):
    '''
    
    '''
    net_value = Account['net_value']
    final_net_value = Account['final_net_value']
    profit = Account['profit']
    day_profit = Account['day_profit']
    positions = Account['positions']
    
    print('累积收益', flush=True)
    print(profit, flush=True)
    print('单日收益', flush=True)
    print(day_profit, flush=True)

    # 计算最大回撤
#    drawdown = compute_drawdown(net_value)
    drawdown = dynamic_max_drawdown(net_value)
    # 计算年化收益和夏普比率
    annual_profit, sharpe_ratio = compute_sharpe_ratio(final_net_value, day_profit)
    # 计算信息率
    ir = compute_ir(day_profit)

    print('回测结果 %s - %s，年化收益： %7.3f，最大回撤：%7.3f，夏普比率：%4.2f，信息率：%4.2f' %
          (start, end, annual_profit, drawdown.max(), sharpe_ratio, ir))
#    print(np.sort(list(set(stop_lose_position_date))))
#    print(np.sort(list(set(stop_lose_position_date_current))))
    profit.index = pd.DatetimeIndex(profit.index, name = 'date')
    positions.index = pd.DatetimeIndex(positions.index, name = 'date')
    drawdown.index = pd.DatetimeIndex(positions.index, name = 'date')
    
    fig, axes = plt.subplots(3, 1, figsize=(16,20))
    
    axes[0] = plt.subplot2grid((5,3), (0,0), colspan=3, rowspan=3)
    axes[0].plot(profit.loc[:,['profit', 'hs300']])
    plt.setp(axes[0].get_xticklabels(), visible=False)
    axes[0].set(title='Backtest Result')
    axes[0].legend(['profit', 'hs300'], loc='best')
    
    axes[1] = plt.subplot2grid((5,3), (3,0), colspan=3, sharex=axes[0])
    axes[1].plot(positions)
    plt.setp(axes[1].get_xticklabels(), visible=False)
    axes[1].set_title('Daily Positions')
    axes[1].legend(['Positions'], loc='best')
    
    axes[2] = plt.subplot2grid((5,3), (4,0), colspan=3, sharex=axes[0])
    axes[2].plot(drawdown)
    axes[2].set_title('Dynamic Max Draw Down')
    axes[2].legend(['MaxDrawdown'], loc='best')
    
    plt.show()
    
def save_file(Account):
    
    with open('backtest--001.file', 'wb') as f:
        pickle.dump(Account, f)
    


if __name__ == "__main__":
    start = '2015-01-01'
    end = '2015-12-31'
    
    daily_hfq_col = DB_CONN['daily_hfq']
    if 'code_1_date_1_index_1_is_trading_1' not in daily_hfq_col.index_information().keys():
        daily_hfq_col.create_index(
                [('code', ASCENDING), ('date', ASCENDING), 
                 ('index', ASCENDING), ('is_trading', ASCENDING)]
                )
        
    
    daily_col = DB_CONN['daily']
    if 'code_1_date_1_index_1_is_trading_1' not in daily_col.index_information().keys():
        daily_col.create_index(
                [('code', ASCENDING), ('date', ASCENDING), 
                 ('index', ASCENDING), ('is_trading', ASCENDING)]
                )
    
    Account = backtest(start, end, 'fixed')
    
    account_analysis(Account, start, end)