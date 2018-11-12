# 导入函数库
from jqdata import *

MA_WIN_1 = 10
MA_WIN_2 = 30

# 初始化函数，设定基准等等
def initialize(context):
    
    set_benchmark('000300.XSHG')
    set_option('use_real_price', True)
    
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')
    
    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    
    # 开盘前运行
    run_daily(before_market_open, time='before_open',
                reference_security='000300.XSHG')
    run_daily(market_open, time='every_bar',
                reference_security='000300.XSHG')
    run_daily(after_market_close, time='after_close',
                reference_security='000300.XSHG')
    # 设定股票池
    g.stock_pool = get_index_stocks("000016.XSHG", date=context.current_dt)
    g.init_cash = context.portfolio.starting_cash
    
    
## 开盘前运行函数     
def before_market_open(context):
    look_ahead_n = max(MA_WIN_1, MA_WIN_2) + 1
    g.up_cross_signaled = set()
    g.down_cross_signaled = set()
    for code in g.stock_pool:
        df = attribute_history(code, look_ahead_n, "1d", ["close"],
            skip_paused=True) # 该函数返回结果不包括当天的数据
        
        if len(df) != look_ahead_n:
            continue
        close = df['close']
        ma_short = pd.rolling_mean(close, MA_WIN_1)
        ma_long = pd.rolling_mean(close, MA_WIN_2)
        
        uc_flags = (ma_short.shift(1) <= ma_long.shift(1)) & \
                    (ma_short > ma_long)
        dc_flags = (ma_short.shift(1) >= ma_long.shift(1)) & \
                    (ma_short < ma_long)
        
        if uc_flags.iloc[-1]:
            g.up_cross_signaled.add(code) 
        if dc_flags.iloc[-1]:
            g.down_cross_signaled.add(code)
        
## 开盘时运行函数
def market_open(context):
    cur_dt = context.current_dt.date()
    p = context.portfolio
    current_data = get_current_data()
    each_cash = g.init_cash / len(g.stock_pool) # 等金额分配资金
    
    # 卖出均线死叉信号的持仓股
    for code, pos in p.positions.items():
        if code in g.down_cross_signaled:
            order_target(code, 0)
    
    for code in g.up_cross_signaled:
        if code not in p.positions:
            if current_data[code].paused:
                continue
            open_price = current_data[code].day_open
            num_to_buy = each_cash / open_price // 100 * 100
            order(code, num_to_buy)
 
## 收盘后运行函数  
def after_market_close(context):
    p = context.portfolio
    pos_level = p.positions_value / p.total_value
    record(pos_level=pos_level)
