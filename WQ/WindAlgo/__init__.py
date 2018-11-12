# Type:        module
# String form: <module 'WindAlgo' from '/opt/conda/lib/python3.5/site-packages/WindAlgo/__init__.py'>
# File:        /opt/conda/lib/python3.5/site-packages/WindAlgo/__init__.py
# Source:     
from WindPy import *
from .BktData import *
import re
import getpass
from WindCharts import *
from datetime import datetime,timedelta
import traceback
import numpy as np
import signal
from functools import reduce
import ppdb
import redis
import requests
import json

class WQRedis(redis.Redis):
    def close(self):
        self.connection_pool.disconnect()
        
    def makekey(self, skey):
        return 'WIND_QUANT_' + skey
        
    def get(self, skey):
        return super(WQRedis, self).get(self.makekey(skey))
        
    def set(self, skey, sdata):
        return super(WQRedis, self).set(self.makekey(skey), sdata)
        
    def llen(self, skey):
        return super(WQRedis, self).llen(self.makekey(skey))

    def lindex(self, skey, idx):
        return super(WQRedis, self).lindex(self.makekey(skey), idx)

    def delete(self, skey):
        super(WQRedis, self).delete(self.makekey(skey))

    def rpush(self, skey, sdata):
        return super(WQRedis, self).rpush(self.makekey(skey), sdata)

    def expire(self, skey, sdata):
        return super(WQRedis, self).expire(self.makekey(skey), sdata)

    def lpop(self, skey):
        return super(WQRedis, self).lpop(self.makekey(skey))
        
    def lrem(self, skey, sdata, num=0):
        super(WQRedis, self).lrem(self.makekey(skey), sdata, num)        

# 注意IP问题
def log_elk(*arg, **kargs):
    import socket
    templates_log = 'log_service={} log_action={} log_usingTime={} priority={} :LogMessage:{}'
    address = ('192.168.161.181', 14560)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        if len(arg) > 0:
            if isinstance(arg[0], bytes):
                s.sendto(arg[0], address)
            else:
                s.sendto(arg[0].encode(), address)
        if len(kargs) > 0:
            s.sendto(templates_log.format(kargs.get('service', 'strategy'), kargs.get('action', 'request'),
                                          kargs.get('usingTime', 100), kargs.get('priority', 'INFO'),
                                          kargs.get('msg', 'strategy test elk')).encode(), address)
    except Exception:
        pass
    s.close()

#r = redis.Redis(host='10.102.17.59', password='abcd1234', port=6379)
r = WQRedis(host='192.168.161.158', password='redis2017', port=6379)
data_manager_url = 'http://192.168.161.181:20000'


class NotebookCell:
    def __init__(self):
        self.id = "dummycellid"
        self.strategy_id = ''

cell = NotebookCell()
load_debug_bkt = ""
TTL = 3 * 60 * 60
bkt_status = 0
b_interrupt = False
b_first_run = True

import getpass
userName = getpass.getuser()
wind_log_path = "/usr/local/log/"

class SlippageSetting():
    def __init__(self, stock_slippage_type='byRate', stock_slippage=0.0001,
                 future_slippage_type='byRate', future_slippage=0.0001):
        try:
            assert stock_slippage_type in ['byRate', 'byVolume'], 'stock_slippage_type should be either "byRate" or "byVolume"!'
            assert future_slippage_type in ['byRate', 'byVolume'], 'future_slippage_type should be either "byRate" or "byVolume"!'
            assert isinstance(stock_slippage, float) or isinstance(stock_slippage, int) or isinstance(stock_slippage, np.int64), 'stock_slippage should be a float or int!'
            assert isinstance(future_slippage, float) or isinstance(future_slippage, int) or isinstance(future_slippage, np.int64), 'future_slippage should be a float or int!'
        except AssertionError as e:
            print(e.args[0])
            raise

        self.stock_slippage_type = stock_slippage_type
        self.stock_slippage = stock_slippage
        self.future_slippage_type = future_slippage_type
        self.future_slippage = future_slippage

class Context:
    def __init__(self, slippage_setting = None):
        self.cellid = cell.id.lower()
        self.strategy_id = cell.strategy_id
        self.securities = []
        self.start_date = ""
        self.end_date = ""
        self.capital = 100000
        self.period = 'd' # 'd' or 'm', meaning day or minute
        self.benchmark = ''
        self.risk_free_rate = 0.03
        self.commission = 0.0003
        self.fee_multi = 3
        if isinstance(slippage_setting, SlippageSetting):
            self.slippage_setting = slippage_setting
        else:
            self.slippage_setting = SlippageSetting()

    def __str__(self):
        res = "securities = " + str(self.securities) + "\n" + \
              "start_date = " + str(self.start_date) + "\n" + \
              "end_date = " + str(self.end_date) + "\n" + \
              "capital = " + str(self.capital) + "\n" + \
              "period = " + str(self.period) + "\n" + \
              "benchmark = " + str(self.benchmark) + "\n" + \
              "risk_free_rate = " + str(self.risk_free_rate) + "\n" + \
              "commission = " + str(self.commission) + "\n" + \
              "fee_multi = " + str(self.fee_multi) + "\n"

        return res

    @property
    def position(self):
        return BackTest.bktobj.query_position()

    @property
    def bkt(self):
        return BackTest.bktobj

    def check(self):
        ctx = self
        try:
            assert isinstance(ctx.start_date, str) and re.fullmatch(
                r'^(?:(?!0000)[0-9]{4}(?:(?:0[1-9]|1[0-2])(?:0[1-9]|1[0-9]|2[0-8])|(?:0[13-9]|1[0-2])(?:29|30)|(?:0[13578]|1[02])31)|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)0229)$',
                ctx.start_date), "start_date should be a string like 20170101"
            assert isinstance(ctx.end_date, str) and re.fullmatch(
                r'^(?:(?!0000)[0-9]{4}(?:(?:0[1-9]|1[0-2])(?:0[1-9]|1[0-9]|2[0-8])|(?:0[13-9]|1[0-2])(?:29|30)|(?:0[13578]|1[02])31)|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)0229)$',
                ctx.end_date), "end_date should be a string like 20170102"
            assert int(ctx.end_date) >= int(ctx.start_date), "end_date should be later than start_date!"
            assert isinstance(ctx.securities, list) and len(
                ctx.securities) > 0, "securities should be a list with at least one security code!"
            assert ctx.period == 'd' or ctx.period == 'm', "period should be either 'd' or 'm'!"
            assert isinstance(ctx.benchmark, str), "benchmark should be a string!"
            assert isinstance(ctx.risk_free_rate, float) or isinstance(ctx.risk_free_rate, str), "risk_free_rate should be a float or a string!"
            assert isinstance(ctx.commission, float), "commission should be a float!"
            assert isinstance(ctx.capital, int) or isinstance(ctx.capital, np.int64), "capital should be an int!"
            assert isinstance(ctx.fee_multi, int) or isinstance(ctx.fee_multi, float) or isinstance(ctx.fee_multi, np.int64), "fee_multi should be an int or float!"

        except AssertionError as e:
            print(e.args[0])
            raise

context = None
bktlib = None

# 用于记录日志信息，并写入elk
# bar_datetime_log = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def write_log_file(name, data):
    with open('/wind/{}.txt'.format(name), 'a') as f:
        f.write(data + '\n')


def write_elk(name, sid):
    import os
    path = '/wind/{}.txt'.format(name)
    if os.path.exists(path):
        with open(path, 'r') as f:
            text = f.read()
            log_elk(msg=text, service='huice_'+name+'_'+sid)
        #os.remove(path)

def remove_datafile():
    import os
    for file in ['/wind/flow.txt', '/wind/log.txt', '/wind/trade.txt', '/wind/position.json']:
        if os.path.exists(file):
            os.remove(file)
    
    #if os.path.exists('/wind/flow.txt'):
    #    os.remove('/wind/flow.txt')
    #if os.path.exists('/wind/log.txt'):
    #    os.remove('/wind/log.txt')
    #if os.path.exists('/wind/trade.txt'):
    #    os.remove('/wind/trade.txt')
    #if os.path.exists('/wind/position.json'):
    #    os.remove('/wind/position.json')
        
        
def flow_log(level, msg):
    global context
    if context and context.strategy_id:
        shwotime = datetime.now().strftime('%Y-%m-%d %H:%m:%S')
        text = shwotime + ' [' + level + '] ' + msg
        print(text)
        r.rpush(context.strategy_id + '_user', text)
        write_log_file('flow', text)

#context = Context()
context_class = Context()
context = None
_callback_exception = False
_show_day = ''
_show_time = ''
g_bar_datetime = ''
class BackTest():
    @staticmethod
    def __bktcallback(bar_datetime, scheduletype, bktout):
        global _show_day
        global _show_time
        global b_first_run
        global g_bar_datetime
        if b_first_run:
            flow_log('Info', '初始化完成，开始回测')
            b_first_run = False

        try:
            assert (isinstance(scheduletype, int) or isinstance(scheduletype, np.int64)) and scheduletype >= 0, "scheduletype should be non-negative int!"
        except AssertionError as e:
            print (e.args[0])
            raise
        global context
        bar_datetime = bar_datetime.decode("utf8")
        g_bar_datetime = bar_datetime[:8]
        # bar_datetime_log = bar_datetime_log + bar_datetime + '\n'
        if scheduletype == 20001:
            #write_log_file('flow', bar_datetime)
            #print(bar_datetime)
            return 1
        if scheduletype == 20002:
            write_log_file('trade', bar_datetime)
            #print(bar_datetime)
            return 1
        if re.fullmatch('\d+\s\d+', bar_datetime):
            bar_datetime = datetime.strptime(bar_datetime, "%Y%m%d %H%M")
        else:
            bar_datetime = datetime.strptime(bar_datetime, "%Y%m%d")
        bar_datetime = bar_datetime + timedelta(microseconds=5000)

        showday = bar_datetime.strftime('%Y-%m-%d')
        if _show_day != showday:
            flow_log('Info', '回测日期：' + showday)
            _show_day = showday
            _show_time = bar_datetime.strftime('%Y-%m-%d %H:%M')
        if context.period == 'm':
            showtime = bar_datetime.strftime('%Y-%m-%d %H:%M')
            if _show_time != showtime:
                flow_log('Info', '回测时间：' + showtime)
                _show_time = showtime
        try:
            if ppdb.bkt_interrupt:
                global b_interrupt
                b_interrupt = True
                errmsg = c_char_p()
                bktlib.bktend(2, byref(errmsg))
                ppdb.bkt_interrupt = False
            else:
                if 0 == scheduletype:
                    flow_log('Info', '运行handle_data')
                else:
                    if BackTest.show_progress == True and scheduletype != (len(BackTest.__bktcallback.schedule_funcs)-1):
                        flow_log('Info', '运行计划任务 ' + BackTest.__bktcallback.schedule_funcs[scheduletype].__name__)
                BackTest.__bktcallback.schedule_funcs[scheduletype](bar_datetime, context, bktout[0].data)
        except Exception as e:
            global _callback_exception
            _callback_exception = True
            print('@@@@@@_strategy_callback_exception@@@@@@')
            if context.strategy_id:
                flow_log('Error', '策略错误：' + traceback.format_exc())
            else:
                print(traceback.format_exc())
            errmsg = c_char_p()
            bktlib.bktend(2, byref(errmsg))
            print(errmsg.value.decode('utf8'))
        return 1

    @staticmethod
    def __bktdaily(bar_datetime, context, bktdata):
        if BackTest.show_progress == True:
            res = context.bkt.query_nav()
            test_time = str(res.get_field("time")[0])
            if test_time.find('1970-01-01') != -1:
                pass
            else:
                bar_datetime = res.get_field("time")[0].strftime("%Y%m%d")
                nav = "[" + bar_datetime + ", " + str(round(res.get_field("nav")[0], 4)) + "]"
                bm = "[" + bar_datetime + ", " + str(round(res.get_field("benchmark")[0], 4)) + "]"

                if context.strategy_id:
                    diff = "[" + bar_datetime + ", " + str(round(res.get_field("navdiffratio")[0], 4)) + "]"
                    bar_datetime2 = res.get_field("time")[0].strftime("%Y-%m-%d")
                    nav2 = '["' + bar_datetime2 + '", ' + str(round(res.get_field("nav")[0], 4)) + ']'
                    bm2 = '["' + bar_datetime2 + '", ' + str(round(res.get_field("benchmark")[0], 4)) + ']'
                    diff2 = '["' + bar_datetime2 + '", ' + str(round(res.get_field("navdiffratio")[0], 4)) + ']'
                    
                    print("{'status':'running', 'nav':" + nav + ", 'benchmark':" + bm  + ", 'navdiffratio':" + diff +", 'cellid':'" + context.cellid + "'}", end='')

                    r.rpush(context.strategy_id, '{"nav":' + nav2 + ', "benchmark":' + bm2 + ', "navdiffratio":' + diff2 +'}')
                else:
                    print("{'status':'running', 'nav':" + nav + ", 'benchmark':" + bm  + ", 'cellid':'" + context.cellid + "'}", end='')
        pass

    def __init__(self, init_func, handle_data_func):
        try:
            assert hasattr(init_func, '__call__'), 'The first parameter should be a function!'
            assert hasattr(handle_data_func, '__call__'), 'The second parameter should be a function!'
        except AssertionError as e:
            print(e.args[0])
            raise
        self.initialize = init_func
        BackTest.__bktcallback.schedule_funcs = [handle_data_func]
        BackTest.bktobj = self

        BKTCBTYPE = CFUNCTYPE(c_int, c_char_p, c_int, POINTER(BKTData))
        self.bktcbptr = BKTCBTYPE(BackTest.__bktcallback)

        self.state = "init"
        self.scheduleperiod = ""
        self.batch_order = BackTest.BatchOrder(self.__order_result)

    def schedule(self, schedule_func, period, offset = 0, market='SH'):
        global context
        try:
            assert re.fullmatch(r'[qmwdh]', period), "The parameter period should be one of [qmwdh]!"
            assert isinstance(offset, int) or isinstance(offset, np.int64), "The parameter offset should be an int!"
            assert hasattr(schedule_func, '__call__'), 'The parameter, schedule_func, should be a function!'
            if period == "h":
                assert offset < 60 * 24, "The offset should be within 60*24!"
        except AssertionError as e:
            print(e.args[0])
            raise

        if period == 'd':
            scheduleperiod = period
        elif period == 'h':
            hour = int(offset/60)
            minute = int(offset%60)
            scheduleperiod = str(hour) + 'h' + "+" + str(minute) + "+" + str(market)
        else:
            scheduleperiod = period + "+" + str(offset)

        if self.scheduleperiod == "":
            self.scheduleperiod = scheduleperiod
        else:
            self.scheduleperiod = self.scheduleperiod + "," + scheduleperiod

        BackTest.__bktcallback.schedule_funcs.append(schedule_func)

    class Result():
        def __init__(self, bkt, filepath):
            resfile = readFile(filepath)
            try:
                self.returns = float('%.4f'%getJsonTag(resfile, "returns"))
            except TypeError:
                self.returns = "unknown"

            try:
                self.relative_returns = float('%.4f'%getJsonTag(resfile, "relative_returns"))
            except TypeError:
                self.relative_returns = "unknown"

            try:
                self.annualized_returns = float('%.4f'%getJsonTag(resfile, "annualized_returns"))
            except TypeError:
                self.annualized_returns = "unknown"

            try:
                self.alpha = float('%.4f'%getJsonTag(resfile, "alpha"))
            except TypeError:
                self.alpha = "unknown"

            try:
                self.beta = float('%.4f'%getJsonTag(resfile, "beta"))
            except TypeError:
                self.beta = "unknown"

            try:
                self.sharpe_ratio = float('%.4f'%getJsonTag(resfile, "sharpe_ratio"))
            except TypeError:
                self.sharpe_ratio = "unknown"

            try:
                self.info_ratio = float('%.4f'%getJsonTag(resfile, "info_ratio"))
            except TypeError:
                self.info_ratio = "unknown"

            try:
                self.max_drawdown = float('%.4f'%getJsonTag(resfile, "max_drawdown"))
            except TypeError:
                self.max_drawdown = "unknown"

            try:
                self.winning_rate = float('%.4f'%getJsonTag(resfile, "winning_rate"))
            except TypeError:
                self.winning_rate = "unknown"

            try:
                self.volatility = float('%.4f'%getJsonTag(resfile, "volatility"))
            except TypeError:
                self.volatility = "unknown"
                
            try:
                self.total_assets = float('%.4f'%getJsonTag(resfile, "total_assets"))
            except TypeError:
                self.total_assets = "unknown"
                
            try:
                self.available_capital = float('%.4f'%getJsonTag(resfile, "available_capital"))
            except TypeError:
                self.available_capital = "unknown"

            self.__bkt__ = bkt

        def __str__(self):
            res = "BKT reuslts: \n" + \
                "returns = " + str(self.returns) + "\n" + \
                "relative_returns = " + str(self.relative_returns) + "\n" + \
                "annualized_returns = " + str(self.annualized_returns) + "\n" + \
                "alpha = " + str(self.alpha) + "\n" + \
                "beta = " + str(self.beta) + "\n" + \
                "sharpe_ratio = " + str(self.sharpe_ratio) + "\n" + \
                "info_ratio = " + str(self.info_ratio) + "\n" + \
                "max_drawdown = " + str(self.max_drawdown) + "\n" + \
                "winning_rate = " + str(self.winning_rate) + "\n" + \
                "total_assets = " + str(self.total_assets) + "\n" + \
                "available_capital = " + str(self.available_capital) + "\n" + \
                "volatility = " + str(self.volatility) + "\n"

            return res

        def get_performance(self, bkt):
            return self

        def get_nav(self):
            return self.__bkt__.summary("nav")

        def get_positon(self):
            return self.__bkt__.summary("position")

        def get_order_detail(self):
            return self.__bkt__.summary("trade")

        def create_line_chart(self):
            df = self.get_nav()
            df.index = [int(x.strftime("%Y%m%d")) for x in df.index]
            chart = WELine(data=df, category=[x for x in list(df.T.index)])
            return chart

    class BackTestError():
        err_map = {
            '0': '成功',
            '-2333': '参数错误',
            '-2334': '可用资金不足',
            '-2335': '可用持仓不足',
            '-2343': '下单被拒绝',
            '-2444': '未知错误',
            '-2445': '系统出错',
            '-2446': '配置错误',
            '-2447': 'securities中无对应code',
            '-2448': '无效价格',
            '-2449': '目标股票中有停牌或者无法交易的股票',
            '104': '买卖量超过当日总成交量',
            '103': '价格超过涨跌停',
            '-1000': '回测未初始化',
            '-1001': '当前回测未结束',
            '-1002': '参数解码出错',
            '-1010': '策略函数设置无效',
            '-1011': 'pid无效',
            '-1012': '开始时间错误',
            '-1013': '结束时间错误',
            '-1014': '股票池设置错误',
            '-1015': '计划任务参数错误',
            '-1016': '资金参数错误',
            '-1017': '费率参数错误',
            '-1018': '期货乘数参数错误',
            '-1019': '股票滑点参数错误',
            '-1020': '期货滑点参数错误',
            '-1021': '业绩基准参数错误',
            '-1022': '保存路径参数错误',
            '-1023': '标的代码参数错误',
            '-1024': '交易方向参数错误',
            '-1025': '成交量参数错误',
            '-1026': '成交额参数错误',
            '-1027': 'percent参数错误',
            '-1028': 'position参数错误',
            '-1029': 'old_codes参数错误',
            '-1030': 'new_codes参数错误',
            '-1031': '（该错误信息不对外）',
            '-1032': '查询参数无效',
            '-1033': '查询参数无效',
            '-1034': 'bar_count参数错误',
            '-3000': '回调函数无效',
            '-3001': '数据库配置初始化失败',
            '-3002': '期货配置初始化失败',
            '-3003': '数股票池初始化失败',
            '-3004': '数股票池加载失败',
            '-3005': '交易日初始化失败',
            '-3006': '交易日为空',
            '-3007': '计划任务初始化失败',
            '-3008': '计划任务添加失败',
            '-3009': '行情初始化失败',
            '-3010': '回测结果计算初始化失败',
            '-3011': '回测还未运行或者已结束',
            '-3012': 'securities中无对应code',
            '-3013': '该函数/功能不支持期货',
            '-3014': '不支持该参数',
            '-3015': '回测还未结束',
            '-3016': '获取历史数据出错',
            '-3017': '没有原持仓',
            '-3018': '回测起始日期早于业绩基准的基日或业绩基准设置错误'
        }

        def __init__(self, err_code, additional_msg=""):
            self.err_code = err_code
            try:
                if additional_msg != "" and additional_msg != None:
                    self.err_msg = BackTest.BackTestError.err_map[str(err_code)] + ": " + additional_msg
                else:
                    self.err_msg = BackTest.BackTestError.err_map[str(err_code)]

            except KeyError as e:
                self.err_msg = "未知错误: " + str(err_code)

        def __str__(self):
            return "err_code: "+str(self.err_code) + ", err_msg: " + self.err_msg

    #中断信号处理
    # def Interrupt_handle(self, signum, frame):
    #     print("\nProgram  interrupted.")
    #     errmsg = c_char_p()
    #     bktlib.bktend(2, byref(errmsg))

    #获取指定日期的前一天
    def get_yesterday(self, today):
        import datetime
        yesterday = datetime.datetime(int(today[:4]), int(today[4:6]), int(today[6:])) + datetime.timedelta(days=-1)
        yesterday = str(yesterday)
        return '%s%s%s' % (yesterday[:4], yesterday[5:7], yesterday[8:10])

    def get_equity(self, e, day):
        for item in e.data:
            if item['day'] == day:
                return item
        return None

    def equity2map(self, e):
        ret = {}
        for item in e.data:
            ret[item['day']] = item
        return ret

    def get_tradedays(self):
        errmsg = c_char_p()
        bktdata = BKTData()
        # to get tradedays
        res = bktlib.bktTradeDays(int(context.start_date),
                                  int(context.end_date),
                                  byref(bktdata),
                                  byref(errmsg)
                                  )
        if res == 0:
            self.bkt_error = None
            tradedays = bktdata.data.get_field('date')
            option = "start_date=" + str(tradedays[0].strftime('%Y%m%d')) + "&&end_date=" + str(tradedays[-1].strftime('%Y%m%d'))
            bktdata_equity = BKTData()
            errmsg_equity = c_char_p()
            bktdata_position = BKTData()
            errmsg_posision = c_char_p()
            # to inquire the position
            res_equity = bktlib.bktsummary('equity'.encode("utf16") + b"\x00\x00",
                                                 option.encode("utf16") + b"\x00\x00",
                                                 byref(bktdata_equity),
                                                 byref(errmsg_equity)
                                                 )
            if res_equity != 0:
                write_log(str(errmsg_equity.value))
                write_log(str(res_equity))
                write_log('get_equity_error')
                return res
            
            bktdata_equity_map = self.equity2map(bktdata_equity)
            res_position = bktlib.bktsummary('position'.encode("utf16") + b"\x00\x00",
                                                 option.encode("utf16") + b"\x00\x00",
                                                 byref(bktdata_position),
                                                 byref(errmsg_posision)
                                                 )
            if res_position == 0:
                write_log('res_position = 0')
                # the data that will be written into the file
                write_data = {'day': 0, 'stocks': [], 'futures': []}
                cur_time = ''
                import os
                path = r'/wind/position.json'
                # clear history data
                if os.path.exists(path):
                    os.remove(path)
                with open(path, 'a') as f:
                    for i in bktdata_position.data: 
                        i['time'] = int(i['time'].strftime('%Y%m%d'))
                        # data written to the same dict when the time is the same
                        if i['time'] != cur_time:
                            if cur_time:
                                write_data = str(write_data)
                                write_data = write_data.replace("'", '"')
                                f.write(str(write_data) + '\n')
                            
                            write_data = {'day': '', 'stocks': [], 'futures': [], 'total_assets':0.0, 'available_capital':0.0, 'holding_value':0.0, 'future_margin':0.0, 'total_pl':0.0, 'day_transfee':0.0, 'close_position_revenue':0.0, 'float_revenue':0.0, 'stock_balance':0.0, 'future_balance':0.0}
                            #get equity data                                
                            # ev = self.get_equity(bktdata_equity, i['time'])
                            ev = bktdata_equity_map.get(i['time'], None)
                            if ev != None:
                                write_data['total_assets'] = ev['total_assets']
                                write_data['available_capital'] = ev['available_capital']
                                write_data['holding_value'] = ev['holding_value']
                                write_data['future_margin'] = ev['future_margin']
                                write_data['total_pl'] = ev['total_pl']
                                write_data['day_transfee'] = ev['day_transfee']
                                write_data['close_position_revenue'] = ev['close_position_revenue']
                                write_data['float_revenue'] = ev['float_revenue']
                                write_data['stock_balance'] = ev['stock_balance']
                                write_data['future_balance'] = ev['future_balance']
                            
                            cur_time = i['time']
                            write_data['day'] = i['time']
                        
                        if i['side'] == 'long':
                           side = 1
                        else:
                           side = -1
                        # change the format of data that will be written
                        change_data = {'lv':0, 's':side, 'a':i['amount'], 'n':i['name'], 'v':i['volume'], 'cd':i['code'], 'c':i['cost_price'], 'r': i['revenue'], 'fr': i['frevenue'], 'm': i['margin']}
                        # to judgment if the code is stock
                        if i['code'][-3:] == '.SH' or i['code'][-3:] == '.SZ':
                            write_data['stocks'].append(change_data)
                        else:
                            write_data['futures'].append(change_data)
                    if cur_time:
                        write_data = str(write_data)
                        write_data = write_data.replace("'", '"')
                        f.write(str(write_data) + '\n')
            else:
                write_log(str(errmsg_posision.value))
                write_log(str(res_position))
                write_log('get_position_error')
        else:
            self.bkt_error = BackTest.BackTestError(res)
            BackTest.show_progress = False
        return res

    def run(self, show_progress=False):
        # signal.signal(signal.SIGINT, self.Interrupt_handle)
        try:
            remove_datafile()
            global context
            global bktlib
            if context == None:
                context = Context()
            self.initialize(context)

            flow_log('Info', '开始初始化')

            if load_debug_bkt != "":
                bktlib = CDLL(load_debug_bkt)
            else:
                if context.strategy_id:
                    show_progress = True
                    bktlib= CDLL("/wind/serverapi/libbktcalc_moni.so")
                    # only execute once simulation
                    global bkt_status
                    if bkt_status == 0:
                        bkt_status = 1
                    else:
                        print('多次实例化回测对象')
                        return False
                else:
                    bktlib = CDLL("/wind/serverapi/libbktcalc.so")


            # if context == None:
            #     context = Context()

            # self.initialize(context)
            context.check()
            tradedays = None
            if show_progress:
                errmsg = c_char_p()
                bktdata = BKTData()
                res = bktlib.bktTradeDays(int(context.start_date),
                                    int(context.end_date),
                                    byref(bktdata),
                                    byref(errmsg)
                                    )

                if res == 0:
                    self.bkt_error = None
                    tradedays = bktdata.data.get_field('date')
                    BackTest.show_progress = True
                else:
                    self.bkt_error = BackTest.BackTestError(res)
                    BackTest.show_progress = False
            else:
                BackTest.show_progress = False

            if BackTest.show_progress:
                self.schedule(BackTest.__bktdaily, 'd')
                # strtdays = str(app.append(int(x.strftime('%Y%m%d')) for x in tradedays))
                strtdays = [int(x.strftime('%Y%m%d')) for x in tradedays]
                strtdays2 = [x.strftime("%Y-%m-%d") for x in tradedays]
                app = [' ']   #在time字段最前面加入空格字符，用作基准点名称
                app.extend(strtdays)
                strtdays = str(app)
                app2 = [' ']
                app2.extend(strtdays2)
                strtdays2 = json.dumps(app2)
                print("{'status':'start', 'time':" + strtdays + ", 'cellid':'" + context.cellid + "'}", end='')
                if context.strategy_id:
                    #r.rpush(context.strategy_id, "{'status':'start', 'time':" + strtdays + ", 'cellid':'" + context.cellid + "'}")
                    r.rpush(context.strategy_id, strtdays2)
            benchmark = ""
            if context.benchmark != '':
                try:
                    # context.start_date = self.get_yesterday(context.start_date)
                    w.start(show_welcome=False)
                    bmres = w.wsd(context.benchmark, "close", context.start_date, context.end_date, 'Days = Alldays')
                    benchmark = "|".join(list(map(lambda x: x[0].strftime('%Y-%m-%d') + ' '+ str(x[1]), zip(bmres.Times, bmres.Data[0]))))
                except:
                    benchmark = ""

                if benchmark.find('None') != -1 or benchmark.find('nan') != -1:
                    self.bkt_error = BackTest.BackTestError(-3018)
                    print(self.bkt_error)
                    write_log("bktstart err:" + str(self.bkt_error))
                    return False

            bktlib.bktstart.restype = c_int
            bktlib.bktorder.restype = c_int
            errmsg = c_char_p()
            bktlib.bktend(1, byref(errmsg))
            SavePath = "/wind/"
            option = str.format("Period={0}&&SchedulePeriod={1}&&InitialFund={2}&&FeeLevel={3}&&FeeMulti={4}&&"
                                "SlippageStk={5}&&SlippageFut={6}&&KPIBase={7}&&RiskFreeRate={8}&&SavePath={9}&&"
                                "SlippageTypeStk={10}&&SlippageTypeFut={11}",
                                context.period,
                                self.scheduleperiod,
                                context.capital,
                                context.commission,
                                context.fee_multi,
                                context.slippage_setting.stock_slippage,
                                context.slippage_setting.future_slippage,
                                benchmark,
                                context.risk_free_rate,
                                SavePath,
                                context.slippage_setting.stock_slippage_type,
                                context.slippage_setting.future_slippage_type
                                )
        except:
            print("An exception occured: ")
            raise

        if context.strategy_id:
            print("{'status':'running', 'nav':[' ', 1.0], 'benchmark':[' ', 1.0], 'navdiffratio':[' ', 0.0], 'cellid':'" + context.cellid + "'}", end='')
            r.rpush(context.strategy_id, '{"nav":[" ", 1.0], "benchmark":[" ", 1.0], "navdiffratio":[" ", 0.0]}')
        else:
            print("{'status':'running', 'nav':[' ', 1.0], 'benchmark':[' ', 1.0], 'cellid':'" + context.cellid + "'}", end='')
            
        write_log("call bkt start")
        self.state = "run"
        errmsg = c_char_p()
        res = bktlib.bktstart(context.cellid.encode("utf16") + b"\x00\x00",
                              context.start_date.encode("utf16") + b"\x00\x00",
                              context.end_date.encode("utf16") + b"\x00\x00",
                              "".join(list(map(lambda x: str.format("{0},", x), context.securities))).encode(
                                  "utf16") + b"\x00\x00",
                              self.bktcbptr,
                              option.encode("utf16") + b"\x00\x00",
                              byref(errmsg)
                              )

        if ppdb.bkt_interrupt:
            global b_interrupt
            b_interrupt = True
            errmsg = c_char_p()
            bktlib.bktend(2, byref(errmsg))
            ppdb.bkt_interrupt = False

        self.state = "end"
        if res != 0:
            self.bkt_error = BackTest.BackTestError(res)
            print(self.bkt_error)
            write_log("bktstart err:" + str(errmsg.value))
            return False
        else:
            self.bkt_error = None
            #查询回测明细信息-持仓数据
            # position = context.bkt.summary('position')
            # if position:
            #     for i in position:
            #         i["time"] = i.get('time').strftime("%Y-%m-%d %H:%M:%S")
            #         i = str(i)
            #         print("{'position':"+i+"}", end='')
            #查询交易明细数据
            # trade = context.bkt.summary('trade')
            # if trade:
            #     for k in trade:
            #         k["time"] = k.get('time').strftime("%Y-%m-%d %H:%M:%S")
            #         k = str(k)
            #         print("{'trade':"+k+"}", end='')
            res = self.Result(self, SavePath+context.cellid+".stat.json")
            if BackTest.show_progress:
                navres = context.bkt.query_nav()
                bar_datetime = navres.get_field("time")[0].strftime("%Y%m%d")
                nav = "[" + bar_datetime + ", " + str(navres.get_field("nav")[0]) + "]"
                bm = "[" + bar_datetime + ", " + str(navres.get_field("benchmark")[0]) + "]"
                
                # 将模拟回测中间数据写入缓存
                if context.strategy_id:
                    diff = "[" + bar_datetime + ", " + str(navres.get_field("navdiffratio")[0]) + "]"
                    
                    print("{'status':'running', 'nav':" + nav + ", 'benchmark':" + bm + ", 'navdiffratio':" + diff + ", 'cellid':'" + context.cellid + "'}", end='')
                    r.rpush(context.strategy_id, '{"status":"running", "nav":' + nav + ', "benchmark":' + bm + ', "navdiffratio":' + diff + ', "cellid":"' + context.cellid + '"}')
                else:
                    print("{'status':'running', 'nav':" + nav + ", 'benchmark':" + bm + ", 'cellid':'" + context.cellid + "'}", end='')
                    
                print("{'status':'end', "
                        "'returns':" + str(res.returns) + ", "
                        "'relative_returns':" + str(res.relative_returns) + ", "
                        "'annualized_returns':" + str(res.annualized_returns) + ", "
                        "'alpha':" + str(res.alpha) + ", "
                        "'beta':" + str(res.beta) + ", "
                        "'sharpe_ratio':" + str(res.sharpe_ratio) + ", "
                        "'info_ratio':" + str(res.info_ratio) + ", "
                        "'max_drawdown':" + str(res.max_drawdown) + ", "
                        "'winning_rate':" + str(res.winning_rate) + ", "
                        "'volatility':" + str(res.volatility) + ", "
                        "'total_assets':" + str(res.total_assets) + ", "
                        "'available_capital':" + str(res.available_capital) + ", "
                        "'cellid':'" + str(context.cellid) + "'}",
                      end='')
                if context.strategy_id:
                    r.rpush(context.strategy_id, "{'status':'end', "
                        "'returns':" + str(res.returns) + ", "
                        "'relative_returns':" + str(res.relative_returns) + ", "
                        "'annualized_returns':" + str(res.annualized_returns) + ", "
                        "'alpha':" + str(res.alpha) + ", "
                        "'beta':" + str(res.beta) + ", "
                        "'sharpe_ratio':" + str(res.sharpe_ratio) + ", "
                        "'info_ratio':" + str(res.info_ratio) + ", "
                        "'max_drawdown':" + str(res.max_drawdown) + ", "
                        "'winning_rate':" + str(res.winning_rate) + ", "
                        "'volatility':" + str(res.volatility) + ", "
                        "'total_assets':" + str(res.total_assets) + ", "
                        "'available_capital':" + str(res.available_capital) + ", "
                        "'cellid':'" + str(context.cellid) + "'}")
            # 获取模拟交易的数据
            try:
                global b_interrupt
                global _callback_exception
                write_log(context.strategy_id)
                write_log(type(context.strategy_id))
                if context.strategy_id:
                    if (not b_interrupt) and (not _callback_exception):
                        ##set saving status, use @@@@@@strategy_status_saving@@@@@@
                        flow_log('Info', '回测完成，开始保存数据')
                        r.set(context.strategy_id+'_moni', 'saving') ####r.set(context.strategy_id+'_moni', '6')
                        print('@@@@@@strategy_status_saving@@@@@@')
                        ##set saving status, use @@@@@@strategy_status_saving@@@@@@

                        a = self.get_tradedays()
                        trade_value = ''
                        import os
                        if os.path.exists(r'/wind/trade.txt'):
                            with open(r'/wind/trade.txt', 'r') as f:
                                trade_value = f.read()
                        write_log('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
                        write_log(trade_value)
                        pos_value = ''
                        if os.path.exists(r'/wind/position.json'):
                            with open(r'/wind/position.json', 'r') as f:
                                pos_value = f.read()
                        write_log(pos_value)
                        flow_value = ''
                        if os.path.exists(r'/wind/flow.txt'):
                            with open(r'/wind/flow.txt', 'r') as f:
                                flow_value = f.read()

                        kpi_data_result = c_char_p()
                        param = c_char_p()
                        # http_client = httpclient.HTTPClient()
                        bktlib.bktMoniGetSaveData.restype = POINTER(c_variant)
                        write_log('begin')
                        # a = self.get_tradedays()
                        res_moni = bktlib.bktMoniGetSaveData(byref(param), byref(kpi_data_result), byref(errmsg))
                        param_value = param.value.decode('utf8')
                        kpi_data_value = kpi_data_result.value.decode('utf8')
                        # r.rpush(context_class.strategy_id, "{'param':" + param_value + ", 'kpi_data':" + kpi_data_value + "}")
                        # 将各参数传递给datamanager
                        write_log(flow_value)
                        body = {'start_time': context.start_date, 'uid': userName, 'sid': context.strategy_id,
                                'end_time': context.end_date,
                                'exec_time': 0, 'last_run_day': context.end_date, 'content': '',
                                'param': param_value, 'last_kpi_data': kpi_data_value, 'trade': trade_value, 'position': pos_value, 'flow' : flow_value}

                        requests.post(data_manager_url + r'/commit/strategy', data=body)

                    r.delete(context.strategy_id)
                    r.delete(context.strategy_id+'_user')
                    b_interrupt = False
            except Exception:
                print(traceback.format_exc())
                flow_log('Error', '策略错误：' + traceback.format_exc())
            if context.strategy_id:
                write_elk('flow', context.strategy_id)
                write_elk('trade', context.strategy_id)
                if not _callback_exception:
                    flow_log('Info', '数据保存成功，回测结束')
                _callback_exception = False
            return res

    def __order_result(self, res, orderid, msg=""):
        if res != 0:
            self.bkt_error = BackTest.BackTestError(res, msg)
            flow_log('Warning', '错误码：' + str(res) + '，错误信息：' + self.bkt_error.err_msg)
            return {"order_id": orderid, "err_code": res, "err_msg": self.bkt_error.err_msg}
        else:
            self.bkt_error = None
            return {"order_id": orderid, "err_code": res, "err_msg": msg}

    def __parse_option(self, option):
        tmp = re.split('[;,\s]', option)
        option = ""
        for i in range(len(tmp)):
            if tmp[i] != '':
                if i != len(tmp) - 1:
                    option = option + tmp[i] + "&&"
                else:
                    option = option + tmp[i]
        return option

    def change_securities(self, code_list):
        try:
            if not isinstance(code_list, list):
                print("codespool should be an instance of []!")
                return False, None
            for item in code_list:
                if not isinstance(item, str):
                    print("code should be a string!")
                    return False, None

            global context
        except AssertionError as e:
            print(e.args[0])
            return False, None

        codes = str.join(',', code_list)
        errmsg = c_char_p()
        bktdata = BKTData()

        res = bktlib.bktcodespoolchg(codes.encode("utf16") + b"\x00\x00",
                                     byref(bktdata),
                                     byref(errmsg)
                                     )

        if res != 0:
            self.bkt_error = BackTest.BackTestError(res)
            flow_log('Error', '改变股票池失败:' + str(self.bkt_error.err_msg))
            return (False, None)
        else:
            flow_log('Info', '改变股票池')
            self.bkt_error = None
            context.securities = code_list
            return (True, bktdata.data)

    def order(self, code, volume, trade_side, price='close', volume_check=False):
        global bktlib

        try:
            if not isinstance(code, str):
                print("code should be a string")
                return self.__order_result(-2333, 0)

            if not (isinstance(trade_side, str) and re.fullmatch(r"(buy)|(cover)|(short)|(sell)", trade_side)):
                print("trade_side should be one of ['buy', 'short', 'cover', 'sell']")
                return self.__order_result(-2333, 0)

            if not ((isinstance(volume, int) or isinstance(volume, np.int64)) and volume >= 0):
                print("volume should be a non-negative int")
                return self.__order_result(-2333, 0)

            if not (isinstance(price, float) or isinstance(price, str) or isinstance(price, int) or isinstance(price, np.int64)):
                print("price should be a float, int or string!")
                return self.__order_result(-2333, 0)

            if not isinstance(volume_check, bool):
                print("option should be a boolean!")
                return self.__order_result(-2333, 0)

            if volume_check == True:
                vchk = "True"
            else:
                vchk = "False"
            option = "price="+str(price)+"&&volumecheck="+vchk
        except AssertionError as e:
            print(e.args[0])
            return self.__order_result(-2444, 0)

        errmsg = c_char_p()
        orderid = c_int32()
        res = bktlib.bktorder(code.encode("utf16") + b"\x00\x00",
                              trade_side.encode("utf16") + b"\x00\x00",
                              str(volume).encode("utf16") + b"\x00\x00",
                              option.encode("utf16") + b"\x00\x00",
                              byref(orderid),
                              byref(errmsg)
                              )

        return self.__order_result(res, orderid.value)

    def order_value(self, code, value, trade_side, price='close', volume_check=False):
        global bktlib
        try:
            if not isinstance(code, str):
                print("code should be a string")
                return self.__order_result(-2333, 0)

            if not isinstance(trade_side, str) and re.fullmatch(r"(buy)|(cover)|(short)|(sell)", trade_side):
                print("trade_side should be one of ['buy', 'short', 'cover', 'sell']")
                return self.__order_result(-2333, 0)

            if not (isinstance(value, int) or isinstance(value, np.int64) or isinstance(value, float)) and value >= 0.0:
                print("value should be a non-negative float")
                return self.__order_result(-2333, 0)

            if not (isinstance(price, float) or isinstance(price, str) or isinstance(price, int) or isinstance(price, np.int64)):
                print("price should be a float, int or string!")
                return self.__order_result(-2333, 0)

            if not isinstance(volume_check, bool):
                print("option should be a boolean!")
                return self.__order_result(-2333, 0)

            if volume_check == True:
                vchk = "True"
            else:
                vchk = "False"
            option = "price="+str(price)+"&&volumecheck="+vchk
        except AssertionError as e:
            print(e.args[0])
            return self.__order_result(-2444, 0)

        errmsg = c_char_p()
        orderid = c_int32()
        res = bktlib.bktorderValue(code.encode("utf16") + b"\x00\x00",
                              str(value).encode("utf16") + b"\x00\x00",
                              str(trade_side).encode("utf16") + b"\x00\x00",
                              option.encode("utf16") + b"\x00\x00",
                              byref(orderid),
                              byref(errmsg)
                              )

        return self.__order_result(res, orderid.value)


    def order_percent(self, code, percent, trade_side, price='close', volume_check=False):
        global bktlib
        try:
            if not isinstance(code, str):
                print("code should be a string")
                return self.__order_result(-2333, 0)

            if not (isinstance(trade_side, str) and re.fullmatch(r"(buy)|(cover)|(short)|(sell)", trade_side)):
                print("trade_side should be one of ['buy', 'short', 'cover', 'sell']")
                return self.__order_result(-2333, 0)

            if not (isinstance(percent, float) and percent >= 0 and percent <= 1.0):
                print("value should be a float between 0.0 and 1.0!")
                return self.__order_result(-2333, 0)

            if not (isinstance(price, float) or isinstance(price, str) or isinstance(price, int) or isinstance(price, np.int64)):
                print("price should be a float, int or string!")
                return self.__order_result(-2333, 0)

            if not isinstance(volume_check, bool):
                print("option should be a boolean!")
                return self.__order_result(-2333, 0)

            if volume_check == True:
                vchk = "True"
            else:
                vchk = "False"
            option = "price="+str(price)+"&&volumecheck="+vchk
        except AssertionError as e:
            print(e.args[0])
            return self.__order_result(-2444, 0)

        errmsg = c_char_p()
        orderid = c_int32()
        res = bktlib.bktorderPercent(code.encode("utf16") + b"\x00\x00",
                              str(percent).encode("utf16") + b"\x00\x00",
                              str(trade_side).encode("utf16") + b"\x00\x00",
                              option.encode("utf16") + b"\x00\x00",
                              byref(orderid),
                              byref(errmsg)
                              )

        return self.__order_result(res, orderid.value)

    def order_target_value(self, code, target_value, price='close', volume_check=False):
        global bktlib
        try:
            if not isinstance(code, str):
                print("code should be a string")
                return self.__order_result(-2333, 0)

            if not ((isinstance(target_value, float) or isinstance(target_value, int) or isinstance(target_value, np.int64)) and target_value >= 0.0):
                print("target_value should be a non-negative float")
                return self.__order_result(-2333, 0)

            if not (isinstance(price, float) or isinstance(price, str) or isinstance(price, int) or isinstance(price, np.int64)):
                print("price should be a float, int or string!")
                return self.__order_result(-2333, 0)

            if not isinstance(volume_check, bool):
                print("option should be a boolean!")
                return self.__order_result(-2333, 0)

            if volume_check == True:
                vchk = "True"
            else:
                vchk = "False"
            option = "price="+str(price)+"&&volumecheck="+vchk
        except AssertionError as e:
            print(e.args[0])
            return self.__order_result(-2444, 0)

        errmsg = c_char_p()
        orderid = c_int32()
        res = bktlib.bktorderTargetValue(code.encode("utf16") + b"\x00\x00",
                              str(target_value).encode("utf16") + b"\x00\x00",
                              "ignore tradeside".encode("utf16") + b"\x00\x00",
                              option.encode("utf16") + b"\x00\x00",
                              byref(orderid),
                              byref(errmsg)
                              )

        return self.__order_result(res, orderid.value)

    def order_target_percent(self, code, target_percent, price='close', volume_check=False):
        global bktlib
        try:
            if not isinstance(code, str):
                print("code should be a string")
                return self.__order_result(-2333, 0)

            if not (isinstance(target_percent, float) and target_percent >= 0 and target_percent <= 1.0):
                print("target_percent should be a float between 0.0 and 1.0!")
                return self.__order_result(-2333, 0)

            if not (isinstance(price, float) or isinstance(price, str) or isinstance(price, int) or isinstance(price, np.int64)):
                print("price should be a float, int or string!")
                return self.__order_result(-2333, 0)

            if not isinstance(volume_check, bool):
                print("option should be a boolean!")
                return self.__order_result(-2333, 0)

            if volume_check == True:
                vchk = "True"
            else:
                vchk = "False"
            option = "price="+str(price)+"&&volumecheck="+vchk
        except AssertionError as e:
            print(e.args[0])
            return self.__order_result(-2444, 0)

        errmsg = c_char_p()
        orderid = c_int32()
        res = bktlib.bktorderTargetPercent(code.encode("utf16") + b"\x00\x00",
                              str(target_percent).encode("utf16") + b"\x00\x00",
                              "ignore tradeside".encode("utf16") + b"\x00\x00",
                              option.encode("utf16") + b"\x00\x00",
                              byref(orderid),
                              byref(errmsg)
                              )

        return self.__order_result(res, orderid.value)

    class BatchOrder:
        def __init__(self, order_result):
            self.__order_result = order_result

        def change_to(self, code_list, position, weight=None, price='close', volume_check=False, no_quotation='error'):
            global bktlib
            try:
                if not (isinstance(code_list, str) or isinstance(code_list, list)):
                    print("code should be a string or list")
                    return self.__order_result(-2333, 0)

                if not (isinstance(weight, str) or isinstance(weight, list) or (weight is None)):
                    print('weight should be a string or list')
                    return self.__order_result(-2333, 0)

                if not (isinstance(position, float) or isinstance(position, int) and position >= 0 and position <= 1.0):
                    print("value should be a float between 0.0 and 1.0!")
                    return self.__order_result(-2333, 0)

                if not (isinstance(price, float) or isinstance(price, str) or isinstance(price, int) or isinstance(price, np.int64)):
                    print("price should be a float, int or string!")
                    return self.__order_result(-2333, 0)

                if not isinstance(volume_check, bool):
                    print("option should be a boolean!")
                    return self.__order_result(-2333, 0)

                if volume_check == True:
                    vchk = "True"
                else:
                    vchk = "False"

                if no_quotation not in ['error', 'skip']:
                    print("no_quotation should be 'error' or 'skip'")
                    return self.__order_result(-2333, 0)

                if isinstance(weight, list):
                    weight = ','.join(str(ww) for ww in weight)
                wei = ''
                if weight is not None:
                    wei = '&&weight='+weight
                option = "price=" + str(price) + "&&volumecheck=" + vchk + "&&no_quotation="+no_quotation + wei

                if isinstance(code_list, list):
                    code_list = ",".join(code_list)
            except AssertionError as e:
                print(e.args[0])
                return self.__order_result(-2444, 0)

            errmsg = c_char_p()
            orderid = c_int32()

            res = bktlib.bktorderChgTo(code_list.encode("utf16") + b"\x00\x00",
                                  str(position).encode("utf16") + b"\x00\x00",
                                  option.encode("utf16") + b"\x00\x00",
                                  byref(orderid),
                                  byref(errmsg)
                                  )

            tmp = errmsg.value.decode("utf8")
            old = str(getJsonTag(tmp, 'old'))
            new = str(getJsonTag(tmp, 'new'))
            code = re.findall('[\w\d\.]+',old).append(re.findall('[\w\d\.]+', new))
            if code is list and len(code) != 0:
                msg = str(code) + "没有行情"
            else:
                msg = None
            return self.__order_result(res, orderid.value, msg)

        def sell_all(self, price='close', volume_check=False, no_quotation='error'):
            global bktlib
            try:
                if not (isinstance(price, float) or isinstance(price, str) or isinstance(price, int) or isinstance(price, np.int64)):
                    print("price should be a float, int or string!")
                    return self.__order_result(-2333, 0)

                if not isinstance(volume_check, bool):
                    print("option should be a boolean!")
                    return self.__order_result(-2333, 0)

                if volume_check == True:
                    vchk = "True"
                else:
                    vchk = "False"

                if no_quotation not in ['error', 'skip']:
                    print("no_quotation should be 'error' or 'skip'")
                    return self.__order_result(-2333, 0)

                option = "price=" + str(price) + "&&volumecheck=" + vchk + "&&no_quotation="+no_quotation
            except AssertionError as e:
                print(e.args[0])
                return self.__order_result(-2444, 0)

            errmsg = c_char_p()
            orderid = c_int32()
            res = bktlib.bktorderSellAll(option.encode("utf16") + b"\x00\x00",
                                  byref(orderid),
                                  byref(errmsg)
                                  )

            tmp = errmsg.value.decode("utf8")
            code = str(getJsonTag(tmp, 'code'))
            if len(re.findall('[\w\d\.]+', code)) != 0:
                msg = str(re.findall('[\w\d\.]+', code)) + "没有行情"
            else:
                msg = None
            return self.__order_result(res, orderid.value, msg)

        def sell(self, code_list, price='close', volume_check=False, no_quotation='error'):
            global bktlib
            try:
                if not (isinstance(code_list, str) or isinstance(code_list, list)):
                    print("code should be a string or list")
                    return self.__order_result(-2333, 0)

                if not (isinstance(price, float) or isinstance(price, str) or isinstance(price, int) or isinstance(price, np.int64)):
                    print("price should be a float, int or string!")
                    return self.__order_result(-2333, 0)

                if not isinstance(volume_check, bool):
                    print("option should be a boolean!")
                    return self.__order_result(-2333, 0)

                if volume_check == True:
                    vchk = "True"
                else:
                    vchk = "False"
                if no_quotation not in ['error', 'skip']:
                    print("no_quotation should be 'error' or 'skip'")
                    return self.__order_result(-2333, 0)

                option = "price=" + str(price) + "&&volumecheck=" + vchk + "&&no_quotation=" + no_quotation

                if isinstance(code_list, list):
                    code_list = ",".join(code_list)
            except AssertionError as e:
                print(e.args[0])
                return self.__order_result(-2444, 0)

            errmsg = c_char_p()
            orderid = c_int32()
            res = bktlib.bktorderSellOld(code_list.encode("utf16") + b"\x00\x00",
                                  option.encode("utf16") + b"\x00\x00",
                                  byref(orderid),
                                  byref(errmsg)
                                  )

            tmp = errmsg.value.decode("utf8")
            code = str(getJsonTag(tmp, 'code'))
            if len(re.findall('[\w\d\.]+',code)) != 0:
                msg = str(re.findall('[\w\d\.]+',code)) + "没有行情"
            else:
                msg = None
            return self.__order_result(res, orderid.value, msg)

        def position_to(self, position, price='close', volume_check=False, no_quotation='error'):
            global bktlib
            try:
                if not (isinstance(position, float) and position >= 0 and position <= 1.0):
                    print("value should be a float between 0.0 and 1.0!")
                    return self.__order_result(-2333, 0)

                if not (isinstance(price, float) or isinstance(price, str) or isinstance(price, int) or isinstance(price, np.int64)):
                    print("price should be a float, int or string!")
                    return self.__order_result(-2333, 0)

                if not isinstance(volume_check, bool):
                    print("option should be a boolean!")
                    return self.__order_result(-2333, 0)

                if volume_check == True:
                    vchk = "True"
                else:
                    vchk = "False"
                if no_quotation not in ['error', 'skip']:
                    print("no_quotation should be 'error' or 'skip'")
                    return self.__order_result(-2333, 0)

                option = "price=" + str(price) + "&&volumecheck=" + vchk + "&&no_quotation=" + no_quotation
            except AssertionError as e:
                print(e.args[0])
                return self.__order_result(-2444, 0)

            errmsg = c_char_p()
            orderid = c_int32()
            res = bktlib.bktorderPosTo(str(position).encode("utf16")+b"\x00\x00",
                                    option.encode("utf16") + b"\x00\x00",
                                    byref(orderid),
                                    byref(errmsg)
                                    )

            tmp = errmsg.value.decode("utf8")
            code = str(getJsonTag(tmp, 'code'))
            if len(re.findall('[\w\d\.]+', code)) != 0:
                msg = str(re.findall('[\w\d\.]+', code)) + "没有行情"
            else:
                msg = None
            return self.__order_result(res, orderid.value, msg)

    def query_order(self, order_id):
        global bktlib
        try:
            assert isinstance(order_id, int) or isinstance(order_id, np.int64), "order_id should be an int!"
        except AssertionError as e:
            print(e.args[0])
            raise

        errmsg = c_char_p()
        bktdata = BKTData()

        option = str(order_id)
        res = bktlib.bktquery("order".encode("utf16")+b"\x00\x00",
                        option.encode("utf16")+b"\x00\x00",
                        byref(bktdata),
                        byref(errmsg)
                        )

        if res == 0:
            self.bkt_error = None
            return bktdata.data
        else:
            self.bkt_error = BackTest.BackTestError(res)
            write_log('error_query_order:' + str(self.bkt_error) + ',' + str(errmsg.value.decode('utf8')))
            return None

    def query_position(self):
        global bktlib

        errmsg = c_char_p()
        bktdata = BKTData()
        res = bktlib.bktquery("position".encode("utf16")+b"\x00\x00",
                        0,
                        byref(bktdata),
                        byref(errmsg)
                        )

        if res == 0:
            self.bkt_error = None
            return bktdata.data
        else:
            self.bkt_error = BackTest.BackTestError(res)
            write_log('error_query_position:' + str(self.bkt_error) + ',' + str(errmsg.value.decode('utf8')))
            return None

    def query_capital(self):
        global bktlib

        errmsg = c_char_p()
        bktdata = BKTData()
        res = bktlib.bktquery("capital".encode("utf16")+b"\x00\x00",
                        0,
                        byref(bktdata),
                        byref(errmsg)
                        )

        if res == 0:
            self.bkt_error = None
            return bktdata.data
        else:
            self.bkt_error = BackTest.BackTestError(res)
            write_log('error_query_capital:' + str(self.bkt_error) + ',' + str(errmsg.value.decode('utf8')))
            bktlib.bktend(2, byref(errmsg))
            raise Exception(str(self.bkt_error))
            return None

    def query_nav(self):
        global bktlib

        errmsg = c_char_p()
        bktdata = BKTData()
        res = bktlib.bktquery("nav".encode("utf16")+b"\x00\x00",
                        0,
                        byref(bktdata),
                        byref(errmsg)
                        )

        if res == 0:
            self.bkt_error = None
            return bktdata.data
        else:
            self.bkt_error = BackTest.BackTestError(res)
            write_log('error_query_nav:' + str(self.bkt_error) + ',' + str(errmsg.value.decode('utf8')))
            return None

    def query_current(self, code_list, field_list):
        global bktlib
        global context
        try:
            assert isinstance(code_list, list) or isinstance(code_list, str), "code_list should be a list or str!"
            assert isinstance(field_list, list) or isinstance(field_list, str), "field_list should be list like ['pre_close', 'open', 'high', 'low', 'close', 'volume', 'pct_chg', 'vwap']!"
            if isinstance(field_list, list):
                for item in field_list:
                    if context.period == "m":
                        assert item.lower() in ['open', 'high', 'low', 'close', 'volume'], \
                            "fields should be the combination of [open, high, low, close, volume]!"
                    else:
                        assert item.lower() in ['pre_close', 'open', 'high', 'low', 'close', 'volume', 'pct_chg', 'vwap'], \
                            "fields should be the combination of [pre_close, open, high, low, close, volume, pct_chg, vwap]!"

        except AssertionError as e:
            print(e.args[0])
            raise

        errmsg = c_char_p()
        bktdata = BKTData()
        if isinstance(code_list, list):
            code_list = ','.join(code_list)
        if isinstance(field_list, list):
            field_list = ','.join(field_list)
        option = "assets="+code_list.upper()+"&&fields="+field_list
        res = bktlib.bktquery("current".encode("utf16")+b"\x00\x00",
                        option.encode("utf16") + b"\x00\x00",
                        byref(bktdata),
                        byref(errmsg)
                        )

        if res == 0:
            self.bkt_error = None
            return bktdata.data
        else:
            self.bkt_error = BackTest.BackTestError(res)
            write_log('error_query_current:' + str(self.bkt_error) + ',' + str(errmsg.value.decode('utf8')))
            return None

    def summary(self, type, start_date = None, end_date = None):
        global bktlib
        global context
        try:
            if start_date:
                start_date = BackTest.parsedate(start_date)
            else:
                start_date = context.start_date

            if end_date:
                end_date = BackTest.parsedate(end_date)
            else:
                end_date = context.end_date

            if end_date is None or start_date is None:
                print('date should be a string like 20170101')
                return

            assert int(end_date) >= int(start_date), "end_date should be later than start_date!"
            assert int(start_date) >= int(context.start_date), "start_date should be later than the start date of backtest!"
            assert int(end_date) <= int(context.end_date), "end_date should be earlier than the end data of backtest!"
            assert self.state == "end", "The summary function should be called when the bkt is completed!"
            assert isinstance(type, str) and type in ["result", "nav", "trade", "position", "position_rate", "monthly_profit", "stat_year", "stat_month", "stat_quarter"], "The 'type' parameter should be one of 'nav','trade', 'monthly_profit', 'stat_year', 'stat_month', 'stat_quarter', 'position_rate' or 'position'!"
        except AssertionError as e:
            print(e.args[0])
            raise

        option = "start_date=" + start_date + "&&end_date=" + end_date
        bktdata = BKTData()
        errmsg = c_char_p()
        res = bktlib.bktsummary(type.encode("utf16")+b"\x00\x00",
                          option.encode("utf16")+b"\x00\x00",
                          byref(bktdata),
                          byref(errmsg)
                          )

        # tmp = bktdata.data.get_dataframe()
        # if type == "nav":
        #     df = tmp.ix[:, tmp.keys()[1]:]
        #     df.index = tmp[tmp.keys()[0]]
        #     tmp = df

        tmp = bktdata.data

        bktlib.bktfree(byref(bktdata))
        if res == 0:
            self.bkt_error = None
            return tmp
        else:
            self.bkt_error = BackTest.BackTestError(res)
            write_log('error_summary:' + str(type) + ':' + str(self.bkt_error) + ',' + str(errmsg.value.decode('utf8')))
            return None

    @staticmethod
    def parsedate(d, with_time=False):
        if d is None:
            d = datetime.today().strftime("%Y%m%d")
            return d
        elif isinstance(d, date):
            d = d.strftime("%Y%m%d")
            return d
        elif isinstance(d, datetime):
            d = d.strftime("%Y%m%d")
            return d
        elif isinstance(d, str):
            try:
                d = pure_num = ''.join(list(filter(str.isdigit, d)))
                if len(d) != 8 and len(d) != 14:
                    return None
                if len(pure_num) == 14:
                    d = pure_num[:8] + ' ' + pure_num[8:]
                    if int(d[9:11]) > 24 or int(d[9:11]) < 0 or \
                       int(d[11:13]) > 60 or int(d[11:13]) < 0 or \
                       int(d[13:15]) > 60 or int(d[13:15]) < 0:
                        return None
                if int(d[:4]) < 1000 or int(d[:4]) > 9999 or \
                   int(d[4:6]) < 1 or int(d[4:6]) > 12 or \
                   int(d[6:8]) < 1 or int(d[6:8]) > 31:
                    return None
                date_time = d.split(' ')
                YMD = date_time[0][:4] + date_time[0][4:6] + date_time[0][6:8]
                HMS = ''
                if with_time and len(date_time) == 2:
                    HMS = ' ' + date_time[1][:2] + ':' + date_time[1][2:4] + ':' + date_time[1][4:6]
                d = YMD + HMS
                return d
            except:
                return None
        return d

    def history(self, code, count, adjtype='0', period = None, bar_datetime = None):
        global bktlib
        global context
        try:
            assert self.state == "run", "The function 'history' should be called at running stage!"
            assert isinstance(code, str), "code should be a string"
            assert (isinstance(count, int) or isinstance(count, np.int64))and count <= 120, "The parameter count should be an int no longer than 120!"

            if period == None:
                period = context.period
            assert period in ['d','m'], "period should one of ['d', 'm']!"

            if bar_datetime == None:
                pass
            elif isinstance(bar_datetime, str):
                assert re.fullmatch(r'^(?:(?!0000)[0-9]{4}(?:(?:0[1-9]|1[0-2])(?:0[1-9]|1[0-9]|2[0-8])|(?:0[13-9]|1[0-2])(?:29|30)|(?:0[13578]|1[02])31)|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)0229)([\s](0[0-9]|1[0-9]|2[0-3])([0-5][0-9])?)?$',
                        bar_datetime), \
                    "bar_datetime should be a string in the form like 20170101 or 20170101 0930"
            else:
                assert isinstance(bar_datetime, datetime), 'Unsupported type of bar_datetime!'
                if period == 'd':
                    bar_datetime = bar_datetime.strftime("%Y%m%d")
                else:
                    bar_datetime = bar_datetime.strftime("%Y%m%d %H%M")
            if bar_datetime is None and context.period == 'm' and period == 'd':
                bar_datetime = g_bar_datetime

        except AssertionError as e:
            print(e.args[0])
            raise

        errmsg = c_char_p()
        bktdata = BKTData()
        if bar_datetime == None:
            res = bktlib.bkthistory2(code.encode("utf16")+b"\x00\x00",
                              str(count).encode("utf16")+b"\x00\x00",
                              str(adjtype).encode('utf16')+b"\x00\x00",
                              byref(bktdata),
                              byref(errmsg)
                              )
        else:
            res = bktlib.bkthistory(code.encode("utf16")+b"\x00\x00",
                              bar_datetime.encode("utf16")+b"\x00\x00",
                              str(count).encode("utf16")+b"\x00\x00",
                              str(adjtype).encode('utf16')+b"\x00\x00",
                              byref(bktdata),
                              byref(errmsg)
                              )

        if res == 0:
            self.bkt_error = None
            return bktdata.data
        else:
            self.bkt_error = BackTest.BackTestError(res)
            write_log('error_history:' + str(self.bkt_error) + ',' + str(errmsg.value.decode('utf8')))
            bktlib.bktend(2, byref(errmsg))
            raise Exception(str(self.bkt_error))
            return None


def pprint(*args, sep=' ', end='\n', file=None):
    temp_args = []
    for i in args:
        if isinstance(i, str):
            if len(i) > 10000:
                temp_args.append(i[:10000] + '...........')
                continue
        temp_args.append(i)
    args = tuple(temp_args)
    print(*args, sep=sep, end=end, file=file)

    count = len(args)
    str_format = '%s '*count
    strpt = str_format % args
    # strpt = str(args)
    # filter @@@@@@strategy_status_saving@@@@@@/@@@@@@_strategy_status_done@@@@@@ msg, not send to redis
    if (cell.strategy_id != '') and (strpt.find('@@@@@@strategy_status_saving@@@@@@') == -1) and (strpt.find('@@@@@@_strategy_status_done@@@@@@') == -1):
        r.rpush(cell.strategy_id + '_user', strpt)
        r.expire(cell.strategy_id + '_user', TTL)
        write_log_file('flow', strpt)
    return


def write_log(msg):
    from datetime import datetime
    try:
        log_name = wind_log_path + userName + "-" + datetime.today().strftime("%Y%m%d")
        msg_prefix = datetime.today().strftime("%Y-%m-%d %H:%M:%S ")
        with open(log_name, "a+") as f:
            f.write(msg_prefix + msg + "\n")
    except:
        return