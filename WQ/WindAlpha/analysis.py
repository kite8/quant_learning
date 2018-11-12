# Type:        module
# String form: <module 'WindAlpha.analysis' from '/opt/conda/lib/python3.5/site-packages/WindAlpha/analysis.py'>
# File:        /opt/conda/lib/python3.5/site-packages/WindAlpha/analysis.py
# Source:     
# -*- coding: utf-8 -*-
from __future__ import division
from collections import OrderedDict
from .util import *
from .data_type import *
from .metrics import return_perf_metrics
from sklearn.linear_model import LinearRegression
from tqdm import tqdm_notebook
from .get_data import *
from scipy.stats import pearsonr
from dateutil.parser import parse
# from numba import jit

# 准备分析用的原始数据
def prepare_raw_data(stock_code, 
                     ind_codes, 
                     start_date, 
                     end_date, 
                     period='M', 
                     is_index=True,
                     include_st=False, 
                     include_suspend=False, 
                     include_new_stock=False, 
                     ipo_days=60):
    """
    :param stock_code: list or str, 股票代码列表或股票池（指数、版块等）代码，如'000300.SH' 或['60000.SH', '600001.SH']
    :param ind_codes:  list or str, 因子代码或代码列表，查询API量化因子部分获取支持的因子代码
    :param start_date:         str, 数据开始日期，如'2015-01-01'
    :param end_date:           str, 数据结束日期，如'2015-01-01'
    :param period:             str, 数据周期，日:'D',周:'W',月:'M',季:'Q'，年:'Y’，默认为月'M'
    :param is_index:           bool, stock_code是否为指数代码，默认True
    :param include_st:         bool, 是否包含st股票，包含：True，不包含：False，默认：False
    :param include_suspend:    bool, 是否包含当天停牌股票，包含：True，不包含：False，默认：False
    :param include_new_stock:  bool, 是否包含当天新上市股票，包含：True，不包含：False，默认：False
    :param ipo_days:           int， 新股上市ipo_days天以内的去除，当include_new_stock为False时有效，默认60
    :return:                   pd.DataFrame，MultiIndex类型的DataFrame，level1_index为时间，level2_index为当期的股票代码，如下


                                                        TECH_AD20       MKT_CAP_ASHARE  NEXT_RET
                                -------------------------------------------------------------
                                2016-01-29      000001.SZ       0.587290        1.180405e+11    -0.044997
                                            000009.SZ   -0.379785       1.765258e+10    -0.076252
                                            000027.SZ   0.271688        9.300787e+09    -0.004648
                                            000039.SZ   -1.161553       1.663698e+10    0.015784
                                            000046.SZ   0.387505        4.545337e+10    -0.030490
                                            000060.SZ   -0.489812       1.865929e+10    0.239582
                                            000061.SZ   0.152975        2.417196e+10    -0.150774
                                            000063.SZ   0.335387        4.857702e+10    -0.047118
                                            000069.SZ   0.332492        2.102879e+10    0.007758
                                            000100.SZ   0.995547        3.257033e+10    0.029934
                                -------------------------------------------------------------
                                2016-02-28  601985.SH   0.483055        2.754828e+10    -0.040351
                                            601988.SH   0.839193        6.786650e+11    -0.015650
                                            601989.SH   0.471473        1.111591e+11    -0.059918
                                            601991.SH   0.821821        4.017733e+10    -0.032873
                                            601992.SH   0.882625        2.371200e+10    0.067242
                                            601998.SH   0.653886        1.770737e+11    -0.031110
                                            603000.SH   -0.831474       1.639740e+10    0.046121
                                            603288.SH   -0.290027       7.404761e+09    -0.057672
                                            603885.SH   -1.433724       3.283040e+09    -0.127513
                                            603993.SH   0.781285        4.145194e+10    0.006231
    """
    dict_df = OrderedDict()

    if isinstance(ind_codes, str):
        ind_codes = [ind_codes]

    ind_codes = [i.upper() for i in ind_codes]

    # 获取交易日期
    if start_date < end_date:
        dates_data = tdays(start_date, end_date, period=period)
    else:
        raise Exception("start_date must lower than end_date")

    dates = dates_data.Data[0]
    dates = [dt.strftime("%Y-%m-%d") for dt in dates]

    # 配置指标代码
    data_codes = [CAP_CODES, "TRADE_STATUS", "IPO_DATE"]
    data_codes.extend(ind_codes)
    sub_cols = ind_codes.copy()
    sub_cols.extend([CAP_CODES, "NEXT_RET"])
    stock_codes = stock_code

    terms_len = len(dates) - 1
    with tqdm_notebook(total=terms_len) as pbar:
        for i in range(terms_len):
            cur_date = dates[i]
            next_date = dates[i + 1]
            pbar.set_description('提取数据')
            pbar.set_description('{}'.format(cur_date))
            # 获取指数成分股数据
            if is_index:
                if stock_code[-2:] in ['SH', 'SZ', 'WI']:
                    stock_codes = wset("sectorconstituent", "date="+cur_date+";windcode="+stock_code).Data[1]
                else:
                    stock_codes = wset("sectorconstituent", "date=" + cur_date + ";sectorid=" + stock_code).Data[1]

            # 获取因子数据ind_codes, 交易状态态数据（TRADE_STATUS）、首次上市日期数据（IPO_DATE)
            _, df_raw = wss(stock_codes, ",".join(data_codes), tradeDate=cur_date, usedf=True)
            _, close = wss(stock_codes, "CLOSE", tradeDate=cur_date, priceAdj="F", cycle="1", usedf=True)
            _, close_next = wss(stock_codes, "CLOSE", tradeDate=next_date, priceAdj="F", cycle="1", usedf=True)

            df_raw["NEXT_RET"] = (close_next-close)/close

            # 去除新上市的股票（ipo_days天以内）
            if not include_new_stock:
                date_least = tdaysoffset(-ipo_days, cur_date, "").Data[0][0]
                df_raw = df_raw[df_raw['IPO_DATE'] <= date_least]

            # 去除停牌的股票
            if not include_suspend:
                df_raw = df_raw[df_raw['TRADE_STATUS'] == u'交易']

            # 去除ST的股票
            if not include_st:
                _, df_st = wset("sectorconstituent", "date=2018-07-13;sectorId=1000006526000000", usedf=True)
                not_st_lst = [code for code in df_raw.index if code not in df_st['wind_code'].tolist()]
                df_raw = df_raw.loc[not_st_lst]

            df_raw_ind = df_raw[sub_cols]
            dict_df[cur_date] = df_raw_ind.dropna()
            pbar.update(1)

            if i == terms_len-1:
                pbar.set_description('完成')

    df_res = pd.concat(dict_df.values(), keys=dict_df.keys())
    df_res.index.names = ['date', 'codes']
    return df_res


def process_raw_data(raw_ind_ret, extreme_num=3, extreme_method='mad', scale_method='normal', funcs=None):
    """
    处理原始因子数据

    :param raw_ind_ret:    pd.DataFrame, 原始因子数据，结构如prepare_raw_data返回的数据
    :param extreme_num:             int, 去极值的判断区间，如果extreme_method='std',extreme_num=3,则超过3个标准差的为极端值
    :param extreme_method:          str, 去极值的方法，可选参数'mad'：平均绝对离差法,'std':标准差法, 默认'mad'
    :param scale_method:            str, 标准化的方法，可选参数'normal': 正常标准化，因子均值为因子算法平均值；
                                                               'cap': 市值加权标准化,因子均值为市值加权均值。默认'normal'

    :param funcs:          list, 自定义数据处理函数，默认None
    :return:               pd.DataFrame
    """
    from functools import partial
    raw_data = raw_ind_ret.copy()

    if extreme_method and extreme_method not in ['mad','std']:
        raise ValueError("extreme_method must be one of ['mad','std'] or False")

    if scale_method and scale_method not in ['normal','cap']:
        raise ValueError("extreme_method must be one of ['normal','cap'] or False")

    p_extreme_process = partial(extreme_process, num=extreme_num, method=extreme_method)
    p_scale_process = partial(scale_process, method=scale_method)
    all_funcs = [p_extreme_process, p_scale_process]

    if not funcs:
        funcs = []
    all_funcs.extend(funcs)

    if not extreme_method:
        all_funcs.remove(p_extreme_process)

    if not scale_method:
        all_funcs.remove(p_scale_process)

    if all_funcs:
        for func in all_funcs:
            raw_data = raw_data.groupby(level=0).apply(func)

    if not all_funcs:
        print("未定义任何数据处理函数，返回原始数据！")

    return raw_data


def ic_analysis(ind_ret_data, ic_method='rank'):
    """
    对因子数据进行IC分析

    :param ind_ret_data: pd.DataFrame, 处理后的因子数据，结构如prepare_raw_data返回的数据
    :param ic_method:             str, ic计算方法：'rank':依据排序大小计算信息系数，
                                                  'normal'为依据数值大小计算的信息系数，
                                                  'risk_adj': 风险调整后的信息系数

    :return:               ICAnalysis，其中ic_series为每个因子ic时间序列，
                                       ic_decay为每个因子12期的IC衰减，
                                       ic_stats为ic的统计指标

    """
    ic_series = get_ic_series(ind_ret_data, ic_method)
    ic_decay = get_ic_decay(ind_ret_data, ic_method)

    ic_mean = ic_series.groupby(level=0).apply(lambda frame: frame['ic'].mean())
    ic_std = ic_series.groupby(level=0).apply(lambda frame: frame['ic'].std())
    ic_ir = ic_mean / ic_std

    ic_stats = pd.DataFrame({
        'IC_mean': ic_mean,
        'IC_std': ic_std,
        'IC_IR': ic_ir
    })

    ret = ICAnalysis()
    ret.ic_series = ic_series
    ret.ic_decay = ic_decay
    ret.ic_stats = ic_stats

    return ret


def get_ic_series(ind_ret_data, ic_method='rank'):
    """
    计算因子ic序列

    :param ind_ret_data: pd.DataFrame, 处理后的因子数据，结构如prepare_raw_data返回的数据
    :param ic_method:             str, ic计算方法，'rank': 依据排序大小计算信息系数，、
                                                 'normal': 为依据数值大小计算的信息系数
                                                 'risk_adj': 风险调整后的信息系数
    """
    ind_names = get_ind_names(ind_ret_data)
    dict_ic = OrderedDict()

    def _ic(frame, ic_method):
        cov = None
        if ic_method == 'risk_adj':
            dt = frame.index[0][0]
            codes = frame.index.get_level_values(1).tolist()
            st_dt = tdaysoffset(-30, dt).Data[0][0]
            st_dt = st_dt.strftime("%Y-%m-%d")
            dts =[i.strftime("%Y-%m-%d") for i in tdays(st_dt, dt).Data[0]]
            chg_list = []
            for tt in dts:
                temp = wss(codes, "pct_chg", tradeDate=tt, cycle="D", usedf=True)[1]
                temp.columns = [tt]
                chg_list.append(temp)
            df_ret = pd.concat(chg_list, axis=1)
            cov = df_ret.T.cov()
        return info_coeff(frame[ind], frame['NEXT_RET'], ic_method, cov)

    for ind in ind_names:
        ts_ic = ind_ret_data.groupby(level=0).apply(lambda frame: _ic(frame, ic_method))
        ic = ts_ic.map(lambda i: i[0])
        p_value = ts_ic.map(lambda i: i[1])
        dict_ic[ind] = pd.DataFrame({'ic': ic, "p_value": p_value})

    df_ic = pd.concat(dict_ic.values(), keys=dict_ic.keys())

    return df_ic


def get_ic_decay(ind_ret_data, ic_method='rank'):
    """
    计算因子ic衰减

    :param ind_ret_data: pd.DataFrame, 处理后的因子数据，结构如prepare_raw_data返回的数据
    :param ic_method:             str, ic计算方法，'rank': 依据排序大小计算信息系数，
                                                  'normal': 为依据数值大小计算的信息系数
                                                  'risk_adj': 风险调整后的信息系数
    """
    # 获取所有股票的价格数据
    temp_price_lst = []
    all_codes = list(set(ind_ret_data.index.get_level_values(1).tolist()))
    all_dates = list(set(ind_ret_data.index.get_level_values(0).tolist()))
    all_dates.sort()
    for dt in all_dates:
        _, df = wss(all_codes, "CLOSE", tradeDate=dt, priceAdj="F", cycle="1", usedf=True)
        df.columns = [dt]
        temp_price_lst.append(df)
    df_all_price = pd.concat(temp_price_lst, axis=1)

    # 计算IC_decay
    grouped = ind_ret_data.groupby(level=0)
    n = len(grouped)
    lag = min(n, 12)

    rets = OrderedDict()
    ind_names = get_ind_names(ind_ret_data)
    for ind in ind_names:
        rets[ind] = OrderedDict()
        for (dt, frame) in grouped:
            if dt != all_dates[-1]:
                rets[ind][dt] = []
                frame = frame.reset_index(level=0, drop=True)
                base_ind = frame[ind]  # 当期因子数据
                base_codes = frame.index.tolist()  # 当期所有待分析的股票代码
                base_close = df_all_price.ix[base_codes, dt]  # 当前期的close
                dt_idx = all_dates.index(dt)
                for idx in range(dt_idx + 1, dt_idx + 1 + lag):
                    if idx < len(all_dates):
                        lag_dt = all_dates[idx]
                        lag_close = df_all_price.ix[base_codes, lag_dt]
                        lag_ret = np.log(lag_close / base_close)
                        (ic, pvalue) = info_coeff(base_ind, lag_ret, ic_method, cov=None)
                        rets[ind][dt].append(ic)
                lt = len(rets[ind][dt])
                rets[ind][dt].extend([np.nan] * (lag - lt))
    df_dict = OrderedDict()
    for k, v in rets.items():
        df_dict[k] = pd.DataFrame(v).T
    df_ic_dec = pd.concat(df_dict.values(), keys=df_dict.keys())
    res = df_ic_dec.groupby(level=0).mean()
    res.columns = ["LAG" + str(i) for i in range(0, lag)]
    return res.T


def add_group(ind_ret_data,
              ind_name=None,
              group_num=5,
              direction='ascending',
              industry_neu=False,
              industry_type='sw',
              industry_level=1):
    """
    根据因子数值添加分组

    :param ind_ret_data: pd.DataFrame, 处理后的因子数据，结构如prepare_raw_data返回的数据
    :param ind_name:              str, 需要分子的因子名
    :param group_num:    int or float, 当为大于等于2的整数时，对股票平均分组；
                                       当为（0,0.5）之间的浮点数，对股票分为3组，前group_num%为G01，后group_num%为G02
                                       中间为G03
    :param direction:             str, 设置所有因子的排序方向，'ascending'表示因子值越大分数越高，'descending'表示因子值越小分数越高；

    :return:             pd.DataFrame, 如下，只返回进行分组的因子的数据（还包括市值及下期收益率数据），新增一列为GROUP，G01为得分最高的一组


                                                        TECH_AD20       MKT_CAP_ASHARE  NEXT_RET        GROUP
                                -------------------------------------------------------------------
                                2016-01-29      000001.SZ       0.587290        1.180405e+11    -0.044997       G02
                                            000009.SZ   -0.379785       1.765258e+10    -0.076252       G04
                                            000027.SZ   0.271688        9.300787e+09    -0.004648       G03
                                            000039.SZ   -1.161553       1.663698e+10    0.015784        G05
                                            000046.SZ   0.387505        4.545337e+10    -0.030490       G03
                                            000060.SZ   -0.489812       1.865929e+10    0.239582        G04
                                            000061.SZ   0.152975        2.417196e+10    -0.150774       G03
                                            000063.SZ   0.335387        4.857702e+10    -0.047118       G03
                                            000069.SZ   0.332492        2.102879e+10    0.007758        G03
                                            000100.SZ   0.995547        3.257033e+10    0.029934        G01
                                -------------------------------------------------------------------
                                2016-02-28  601985.SH   0.483055        2.754828e+10    -0.040351       G02
                                            601988.SH   0.839193        6.786650e+11    -0.015650       G01
                                            601989.SH   0.471473        1.111591e+11    -0.059918       G02
                                            601991.SH   0.821821        4.017733e+10    -0.032873       G01
                                            601992.SH   0.882625        2.371200e+10    0.067242        G01
                                            601998.SH   0.653886        1.770737e+11    -0.031110       G02
                                            603000.SH   -0.831474       1.639740e+10    0.046121        G05
                                            603288.SH   -0.290027       7.404761e+09    -0.057672       G04
                                            603885.SH   -1.433724       3.283040e+09    -0.127513       G05
                                            603993.SH   0.781285        4.145194e+10    0.006231        G01




    """
    dict_ascending = {
        'ascending': True,
        'descending': False
    }

    ascending = dict_ascending[direction]

    def __add_group(frame):
        num = group_num
        ind_name = get_ind_names(frame)[0]
        rnk = frame[ind_name].rank(
            ascending=ascending)
        rnk += rnk.isnull().sum()
        rnk = rnk.fillna(0)

        if num > 1:
            labels = ['G{:0>2}'.format(i) for i in range(1, num + 1)]
            num = int(num)
            category = pd.cut(-rnk, bins=num, labels=labels).astype(str)
            category.name = 'GROUP'
            new_frame = frame.join(category)

        elif num < 0.50 and num > 0:
            percentile_up = np.percentile(frame[ind_name], 100 * (1 - num))
            percentile_low = np.percentile(frame[ind_name], 100 * num)
            new_frame = frame.copy()
            new_frame['GROUP'] = ['G02'] * len(frame[ind_name])
            new_frame['GROUP'][frame[ind_name] >= percentile_up] = 'G01'
            new_frame['GROUP'][frame[ind_name] <= percentile_low] = 'G03'

        else:
            raise ValueError('num must be int greater than 1 or float in (0,0.5)')
        return new_frame

    def __add_group_neu(frame):
        new_frame = frame.groupby('INDUSTRY').apply(lambda x: __add_group(x))
        return new_frame

    if ind_name:
        ind_name = [ind_name]
    else:
        ind_names = get_ind_names(ind_ret_data)
        if len(ind_names) == 1:
            ind_name = ind_names
        else:
            raise ValueError('must specify ind_name')

    column = ind_name + [i for i in [CAP_CODES, 'NEXT_RET'] if i in ind_ret_data.columns]
    ind_ret_data = ind_ret_data[column]

    if not industry_neu:
        if isinstance(ind_ret_data.index, pd.MultiIndex):
            return ind_ret_data.groupby(level=0).apply(__add_group)
        elif isinstance(ind_ret_data.index, pd.Index) and not isinstance(ind_ret_data.index, pd.MultiIndex):
            return __add_group(ind_ret_data)
    else:
        if isinstance(ind_ret_data.index, pd.MultiIndex):
            stocks = sorted(ind_ret_data.index.get_level_values(1).unique())
            _, industries = w.wss(stocks, "industry_" + industry_type, "industryType=" + str(industry_level),
                                  usedf=True)
            industries.columns = ['INDUSTRY']
            ind_ret_data = ind_ret_data.groupby(level=0).apply(
                lambda frame: pd.concat([frame.reset_index(level=0, drop=True), industries], join='inner', axis=1))
            return ind_ret_data.groupby(level=0).apply(__add_group_neu)
        elif isinstance(ind_ret_data.index, pd.Index) and not isinstance(ind_ret_data.index, pd.MultiIndex):
            stocks = sorted(ind_ret_data.index.tolist())
            _, industries = w.wss(stocks, "industry_" + industry_type, "industryType=" + str(industry_level),
                                  usedf=True)
            industries.columns = ['INDUSTRY']
            ind_ret_data = pd.concat([ind_ret_data, industries], join='inner', axis=1)
            return __add_group_neu(ind_ret_data)


def return_analysis(ind_ret_data,
                    bench_code,
                    start_date,
                    end_date,
                    period='M',
                    ret_method='cap',
                    group_num=5,
                    ind_direction=None,
                    industry_neu=False,
                    industry_type='sw',
                    industry_level=1):
    """

    :param ind_ret_data: pd.DataFrame, 处理后的因子数据，结构如prepare_raw_data返回的数据
    :param bench_code:   str,          基准代码，如'000300.SH'
    :param start_date:   str,          数据开始日期，如'2015-01-01'
    :param end_date:     str,          数据结束日期，如'2015-01-01'
    :param ret_method:   str,          组合收益率加权方法， 'cap': 市值加权， 'equal':等权，默认'cap'
    :param group_num:    int or float, 当为大于等于2的整数时，对股票平均分组；
                                       当为（0,0.5）之间的浮点数，对股票分为3组，前group_num%为G01，后group_num%为G02
                                       中间为G03
    :param ind_direction: str,         设置所有因子的排序方向，'ascending'表示因子值越大分数越高，'descending'表示因子值越小分数越高；
                                       当为dict时，可以分别对不同因子的排序方向进行设置

    :return: ReturnAnalysis,           WindAlpha自带收益分析数据类型，不同属性的说明如下：
                                       group_mean_return：每组每期平均收益率
                                       return_stats：收益率的统计指标
                                       group_cum_return：每组每期的累计收益
                                       benchmark_return：基准收益率
    """
    def _return_analysis(group_data, bench_ret):
        """
        收益率分析
        """
        all_dates = list(set(group_data.index.get_level_values(0).tolist()))
        all_dates.sort()

        if ret_method == 'equal':
            group_mean = group_data.groupby(
                [group_data.index.get_level_values(0), group_data['GROUP']])[['NEXT_RET']].mean()

        elif ret_method == 'cap':
            fun = lambda x: (x[CAP_CODES] * x['NEXT_RET'] / x[CAP_CODES].sum()).sum()
            group_mean = group_data.groupby([group_data.index.get_level_values(0), group_data['GROUP']]).apply(fun)
            group_mean = group_mean.to_frame()
            group_mean.columns = ['NEXT_RET']

        else:
            raise ValueError("ret_method can only be one value in ['equal','cap']")

        group_mean = group_mean.unstack()['NEXT_RET']

        LSG_name = group_mean.columns[0] + '-' + group_mean.columns[-1]
        group_mean[LSG_name] = group_mean.ix[:, 0] - group_mean.ix[:, -1]
        group_mean['BENCH_RET'] = bench_ret

        return_stats = pd.DataFrame()
        for col in group_mean.columns:
            return_stats[col] = return_perf_metrics(group_mean[col], bench_ret)

        return group_mean, return_stats

    if start_date < end_date:
        dates_data = tdays(start_date, end_date, period=period)
    else:
        raise Exception("start_date must lower than end_date")

    dates = dates_data.Data[0]
    dates = [dt.strftime("%Y-%m-%d") for dt in dates]
    _, bench_close = wsd(bench_code, "close", dates[0], dates[-1], "Period="+period, usedf=True)
    bench_ret = bench_close['CLOSE'].shift(-1) / bench_close['CLOSE']-1
    bench_df = pd.DataFrame(data=bench_ret.values, index=[dt.strftime("%Y-%m-%d") for dt in bench_ret.index],
                            columns=['BENCH_RET'])
    bench_df['BENCH_CUM_RET'] = (bench_df['BENCH_RET'] + 1).cumprod()

    ret_gt_dict = OrderedDict()
    ret_gm_dict = OrderedDict()
    ret_cum_dict = OrderedDict()

    # data_dates = ind_ret_data.index.get_level_values(0).unique().tolist()
    # t_delta = parse(data_dates[1]) - parse(data_dates[0])

    for ind in get_ind_names(ind_ret_data):
        if not ind_direction:
            direction = 'ascending'
        elif ind_direction in ['ascending', 'descending']:
            direction = ind_direction
        else:
            direction = ind_direction.get(ind, 'ascending')

        group_data = add_group(ind_ret_data, ind, group_num, direction, industry_neu, industry_type, industry_level)
        gm, gt = _return_analysis(group_data, bench_df['BENCH_RET'])
        ret_gt_dict[ind] = gt
        ret_gm_dict[ind] = gm
        ret_cum_dict[ind] = (1 + gm).cumprod()

    ret = ReturnAnalysis()
    ret.group_mean_return = pd.concat(ret_gm_dict.values(), keys=ret_gm_dict.keys())
    ret.return_stats = pd.concat(ret_gt_dict.values(), keys=ret_gt_dict.keys())
    ret.group_cum_return = pd.concat(ret_cum_dict.values(), keys=ret_cum_dict.keys())
    ret.benchmark_return = bench_df

    return ret


def turnover_analysis(ind_ret_data, method='count', group_num=5, ind_direction=None):
    """
    因子换手率分析
    :param ind_ret_data: pd.DataFrame, 处理后的因子数据，结构如prepare_raw_data返回的数据
    :param method:                str, 换手率计算方法，'count'个数法，'cap',权重法，默认'count'
    :param group_num:    int or float, 当为大于等于2的整数时，对股票平均分组；
                                       当为（0,0.5）之间的浮点数，对股票分为3组，前group_num%为G01，后group_num%为G02
                                       中间为G03
    :param ind_direction: str,         设置所有因子的排序方向，'ascending'表示因子值越大分数越高，'descending'表示因子值越小分数越高；
                                       当为dict时，可以分别对不同因子的排序方向进行设置

    :return: TurnoverAnalysis,         WindAlpha自带换手率分析数据类型，不同属性的说明如下：
                                       turnover：每组换手率数据时间序列
                                       buy_signal：买入信号衰减与反转
                                       auto_corr: 因子自相关系数
    """

    def _turnover_analysis(ind_ret_data, method='count'):
        """
        换手率分析
        Args:
            ind_ret_data (DataFrame): 一个Multi index 数据框, 含有indicator, NEXT_RET, MARKET_CAP_ASHARE, GROUP列
            method (str): count or capwt
        Returns:
            TurnoverAnalysis
        """
        if method not in ['count', 'capwt']:
            raise ValueError("method can only be one value in ['count', 'capwt']")

        code_and_cap = (ind_ret_data.groupby([ind_ret_data.index.get_level_values(0), ind_ret_data.GROUP])
                        .apply(lambda frame: dict(zip(frame.index.get_level_values(1), frame[CAP_CODES])))
                        .unstack()
                        )

        method = count_turnover if method == 'count' else capwt_turnover

        dts = ind_ret_data.index.get_level_values(0).unique()[1:]
        results = OrderedDict()
        for group in code_and_cap.columns:
            group_ret = []
            for idx, dic in enumerate(code_and_cap.ix[:-1, group]):
                cur_dic = dic
                next_dic = code_and_cap.ix[idx + 1, group]
                group_ret.append(method(cur_dic, next_dic))
            results[group] = group_ret

        turnov = pd.DataFrame(results, index=dts)
        buy_signal = signal_decay_and_reversal(ind_ret_data)

        return turnov, buy_signal

    turnov_dict = OrderedDict()
    buy_sig_dict = OrderedDict()

    for ind in get_ind_names(ind_ret_data):
        if not ind_direction:
            direction = 'ascending'
        elif ind_direction in ['ascending', 'descending']:
            direction = ind_direction
        else:
            direction = ind_direction.get(ind, 'ascending')

        group_data = add_group(ind_ret_data, ind, group_num, direction)
        turnov, buy_signal = _turnover_analysis(group_data, method)
        turnov_dict[ind] = turnov
        buy_sig_dict[ind] = buy_signal

    ret = TurnOverAnalysis()
    ret.turnover = pd.concat(turnov_dict.values(), keys=turnov_dict.keys())
    ret.buy_signal = pd.concat(buy_sig_dict.values(), keys=buy_sig_dict.keys())
    ret.auto_corr = auto_correlation(ind_ret_data)

    return ret

# @jit
def signal_decay_and_reversal(ind_ret_data):
    """
    计算因子买入信号衰减与反转
    :param ind_ret_data: pd.DataFrame, 处理后的因子数据，结构如prepare_raw_data返回的数据
    :return: pd.DataFrame
    """
    data = (ind_ret_data.groupby([ind_ret_data.index.get_level_values(0), ind_ret_data.GROUP])
            .apply(lambda frame: frozenset(frame.index.get_level_values(1)))
            .unstack()
            )
    group_buy = data.iloc[:, 0]
    group_sell = data.iloc[:, -1]
    n = len(data)
    lag = min(n, 12)
    decay = []
    reversal = []
    for i in range(len(group_buy)):
        dec_temp_lst = []
        rev_temp_lst = []
        x = group_buy[i]
        for ii in range(i, i+lag):
            if ii < len(group_buy):
                y_dec = group_buy[ii]
                y_rev = group_sell[ii]
                dec_temp_lst.append(len(x.intersection(y_dec)) / len(x))
                rev_temp_lst.append(len(x.intersection(y_rev)) / len(x))

        lt=len(dec_temp_lst)
        dec_temp_lst.extend([np.nan]*(lag-lt))
        rev_temp_lst.extend([np.nan]*(lag-lt))
        decay.append(dec_temp_lst)
        reversal.append(rev_temp_lst)
    decay_mean = np.nanmean(np.array(decay), axis=0)
    reversal_mean = np.nanmean(np.array(reversal), axis=0)
    ret = pd.DataFrame({'decay': decay_mean, 'reversal': reversal_mean}, index=[''.join(['LAG', str(i)]) for i in range(lag)])
    return ret


def auto_correlation(ind_ret_data):

    ind_names = get_ind_names(ind_ret_data)

    grouped = ind_ret_data.groupby(level=0)
    n = len(grouped)
    lag = min(n, 12)
    dts = sorted(ind_ret_data.index.get_level_values(0).unique())
    group_names = sorted(grouped.groups.keys())

    dict_df = OrderedDict()
    for ind_name in ind_names:
        table = []
        for idx in range(0, n - lag):
            rows = []
            for l in range(idx + 1, idx + 1 + lag):
                temp = grouped.get_group(group_names[idx]).reset_index()
                # print(temp)
                current_frame = (grouped.get_group(group_names[idx])
                                 .reset_index()
                                 .set_index('codes')[ind_name].dropna())
                next_frame = (grouped.get_group(group_names[l])
                              .reset_index()
                              .set_index('codes')[ind_name].dropna())
                x, y = current_frame.align(next_frame, join='inner')
                rows.append(pearsonr(x.values, y.values)[0])
            table.append(rows)
        auto_corr = pd.DataFrame(
            table, index=dts[:(n - lag)], columns=['LAG'+str(i) for i in range(1, lag + 1)])
        dict_df[ind_name] = auto_corr.mean()

    return pd.DataFrame(dict_df)


def sector_analysis(ind_ret_data, group_num=5, ind_direction=None, industry_type='sw', industry_level=1):
    """
    股票行业与市值分析

    :param ind_ret_data: pd.DataFrame, 处理后的因子数据，结构如prepare_raw_data返回的数据
    :param method:                str, 换手率计算方法，'count'个数法，'cap',权重法，默认'count'
    :param group_num:    int or float, 当为大于等于2的整数时，对股票平均分组；
                                       当为（0,0.5）之间的浮点数，对股票分为3组，前group_num%为G01，后group_num%为G02
                                       中间为G03
    :param ind_direction:         str, 设置所有因子的排序方向，'ascending'表示因子值越大分数越高，'descending'表示因子值越小分数越高；
                                       当为dict时，可以分别对不同因子的排序方向进行设置
    :param industry_type:         str, sw表示申万行业分类，citic表示中信行业分类，默认'sw'
    :param industry_level:        int, 行业分类等级，默认1

    :return: SectorAnalysis,        WindAlpha自带选股结果分析数据类型，不同属性的说明如下：
                                  group_cap_mean：每组选出股票的市值均值
                                  group_industry_mean_ratio：每组所有时间的行业平均占比
                                  group_industry_ratio: 每组所行业占比时间序列
                                  group_stock_list：每组选出股票的代码
    """
    def _sector_analysis(ind_ret_data):

        from collections import Counter
        ret = OrderedDict()

        grouped = ind_ret_data.groupby(
            [ind_ret_data.index.get_level_values(0), ind_ret_data.GROUP])

        # index:dt, columns:group
        stocks_per_dt_group = grouped.apply(
            lambda frame_: tuple(frame_.index.get_level_values(1))).unstack()

        mean_cap_per_dt_group = grouped.apply(
            lambda frame_: frame_['MKT_CAP_ASHARE'].mean()).unstack()  # index:dt, columns:group

        stocks = sorted(ind_ret_data.index.get_level_values(1).unique())
        industries = wss(stocks, "industry_" + industry_type, "industryType=" + str(industry_level), usedf=True)

        if industries[0] == 0:
            industries = industries[1]['INDUSTRY_SW']

        industries_per_dt_group = stocks_per_dt_group.applymap(
            lambda tup: tuple(industries[t] for t in tup))

        counter = industries_per_dt_group.applymap(lambda tup: Counter(tup))
        counter_percent = counter.applymap(
            lambda dic: {k: v * 1.0 / sum(dic.values()) for k, v in dic.items()})

        dic_frame = OrderedDict()
        for col in counter_percent.columns:
            frame = pd.DataFrame(
                counter_percent[col].tolist(), index=counter_percent.index).fillna(0)
            frame = frame[
                list(frame.iloc[0, :].sort_values(ascending=False).index)]
            dic_frame[col] = frame.fillna(0)

        # 行业平均占比: 所有分组, 所有dt合并到一起
        industry_g = OrderedDict()
        for coll in industries_per_dt_group.columns:
            industries_total = Counter(industries_per_dt_group[coll].sum())
            industries_total = {str(k): v for k, v in industries_total.items()}
            industries_total = pd.Series(industries_total).sort_values(ascending=False)
            industry_g[coll] = industries_total

        df_indstry_g = pd.DataFrame(industry_g).fillna(0)
        df_indstry_mean = df_indstry_g / df_indstry_g.sum(axis=0)

        ret['group_cap_mean'] = mean_cap_per_dt_group  # 每一组股票每个时间的市值均值
        ret['group_industry_mean_ratio'] = df_indstry_mean
        ret['group_industry_ratio'] = pd.concat(dic_frame.values(), keys=dic_frame.keys())  # 每组每个时间行业占比
        ret['group_stock_list'] = stocks_per_dt_group  # 每一组每个时间的股票列表
        return ret

    group_cap_mean_dict = OrderedDict()
    group_industry_mean_ratio_dict = OrderedDict()
    group_industry_ratio_dict = OrderedDict()
    group_stock_list_dict = OrderedDict()

    for ind in get_ind_names(ind_ret_data):
        if not ind_direction:
            direction = 'ascending'
        elif ind_direction in ['ascending', 'descending']:
            direction = ind_direction
        else:
            direction = ind_direction.get(ind, 'ascending')

        group_data = add_group(ind_ret_data, ind, group_num, direction)
        ret_code = _sector_analysis(group_data)
        group_cap_mean_dict[ind] = ret_code['group_cap_mean']
        group_industry_mean_ratio_dict[ind] = ret_code['group_industry_mean_ratio']
        group_industry_ratio_dict[ind] = ret_code['group_industry_ratio']
        group_stock_list_dict[ind] = ret_code['group_stock_list']

    ret = SectorAnalysis()
    ret.group_cap_mean = pd.concat(group_cap_mean_dict.values(), keys=group_cap_mean_dict.keys())
    ret.group_industry_mean_ratio = pd.concat(group_industry_mean_ratio_dict.values(),
                                              keys=group_industry_mean_ratio_dict.keys())
    ret.group_industry_ratio = pd.concat(group_industry_ratio_dict.values(), keys=group_industry_ratio_dict.keys())
    ret.group_stock_list = pd.concat(group_stock_list_dict.values(), keys=group_stock_list_dict.keys())

    ret.group_industry_ratio = ret.group_industry_ratio.fillna(0)

    return ret


def score_indicators(ind_ret_data, score_method='equal', ind_direction='ascending', ic_window=12):
    """
    多因子打分，返回打分后的pd.DataFrame

    :param ind_ret_data: pd.DataFrame, 处理后的因子数据，结构如prepare_raw_data返回的数据
    :param score_method:          str，打分方法，可选有'equal':因子等权，'ic':因子ic加权，'icir':因子icir加权
    :param ind_direction:         str, 设置所有因子的排序方向，'ascending'表示因子值越大分数越高，'descending'表示因子值越小分数越高；
                                       当为dict时，可以分别对不同因子的排序方向进行设置
    :param ic_window:             int, ic或icir打分法时ic计算均值及标准差的数据量, 默认12

    :return:            pd.DataFrame, 返回数据为MultiIndex格式，SCORE为股票当期的打分

                                                SCORE       MKT_CAP_ASHARE      NEXT_RET
                        2016-01-29      000001.SZ       112.680909      1.180405e+11    -0.044997
                                    000009.SZ   94.820807       1.765258e+10    -0.076252
                                    000027.SZ   -23.072296      9.300787e+09    -0.004648
                                    000039.SZ   -127.123420     1.663698e+10    0.015784
                                    000046.SZ   472.080256      4.545337e+10    -0.030490
                                    ... ...     ...     ...     ...
                        2017-11-30      601966.SH       -114.439776     6.752128e+09    0.019665
                                    601985.SH   -24.761223      3.108793e+10    0.000000
                                    601988.SH   -8.482753       8.283085e+11    0.010127
                                    601989.SH   -185.353589     1.131079e+11    -0.021330
    """
    dict_ascending = {
        'ascending': True,
        'descending': False
    }

    ind_names = get_ind_names(ind_ret_data)
    # 非因子列名称
    other_names = sorted(set(ind_ret_data.columns) - set(ind_names))
    if len(ind_names) >= 1:
        ind_names = sorted(ind_names)
    else:
        raise ValueError('ind_ret_data must have at least 1 factor, got 0.')


    def get_rank():
        if isinstance(ind_direction, str) and ind_direction in ['ascending', 'descending']:
            direction = dict_ascending[ind_direction]
            rnk = ind_ret_data[ind_names].groupby(level=0).rank(ascending=direction)
        elif isinstance(ind_direction, dict):
            default_direction = dict(
                zip(ind_names, ['ascending'] * len(ind_names)))
            if len(ind_direction) != len(ind_names):
                print('direction 长度与因子数目不同, 未指明的将按照默认降序排序(大值排名靠前).')
            default_direction.update(ind_direction)
            rnk_list = [ind_ret_data[col].groupby(level=0).rank(ascending=dict_ascending[default_direction[col]])
                        for col in ind_names]
            rnk = pd.concat(rnk_list, axis=1)
        # 假设有k个NA, 未执行下句时, rank 值 从1..(N-k), 执行后, rnk值是从k+1..N
        rnk += rnk.isnull().sum()
        # fillna后, NA的rank被置为0.
        rnk = rnk.fillna(0.0)
        return rnk

    def equal_weighted():
        """
        因子打分:等权法
        Returns:
            DataFrame: 一个数据框,有score列,和ret, cap,等列
        """
        rnk = get_rank()
        score_ = rnk.mean(axis=1).to_frame().rename(
            columns={0: 'SCORE'})

        # score_ = ind_ret_data[ind_names].mul(rnk.mean(axis=1), axis=0).sum(axis=1).to_frame().rename(
        #     columns={0: 'SCORE'})
        return score_.join(ind_ret_data[other_names])

    def ic_weighted():
        ic = get_ic_series(ind_ret_data, ic_method='rank')['ic'].abs()
        rolling_ic = ic.rolling(ic_window, min_periods=1).mean()
        rolling_ic = rolling_ic.unstack().T
        weight = rolling_ic.divide(rolling_ic.sum(axis=1), axis=0)
        rank = get_rank()
        score_ = (rank * weight).sum(axis=1).to_frame().rename(columns={0: 'SCORE'})

        return score_.join(ind_ret_data[other_names])

    def icir_weighted():
        ic = get_ic_series(ind_ret_data, ic_method='rank')['ic'].abs()
        rolling_ic = ic.rolling(ic_window, min_periods=1).mean()
        rolling_std = ic.rolling(ic_window, min_periods=1).std().fillna(method='backfill')
        ic_ir = (rolling_ic / rolling_std).unstack().T

        weight = ic_ir.divide(ic_ir.sum(axis=1), axis=0)
        rank = get_rank()
        score_ = (rank * weight).sum(axis=1).to_frame().rename(columns={0: 'SCORE'})
        return score_.join(ind_ret_data[other_names])

    valid_method = {'equal': equal_weighted,
                    'ic': ic_weighted,
                    'icir': icir_weighted,
                    }
    try:
        if len(ind_names) == 1:
            rnk = get_rank()
            score_ = rnk.rename(columns={0: 'SCORE'})
            return score_.join(ind_ret_data[other_names])
        else:
            return valid_method[score_method]()
    except KeyError:
        print('{} is not a valid method. valid methods are: {}'.format(
            score_method, list(valid_method.keys())))
        raise


def regress_indicators(ind_ret_data):
    """

    回归法，返回打分后的pd.DataFrame

    :param ind_ret_data: pd.DataFrame, 处理后的因子数据，结构如prepare_raw_data返回的数据

    :return:            pd.DataFrame, 返回数据为MultiIndex格式，SCORE为股票当期的打分

                                                SCORE       MKT_CAP_ASHARE      NEXT_RET
                        2016-01-29      000001.SZ       112.680909      1.180405e+11    -0.044997
                                    000009.SZ   94.820807       1.765258e+10    -0.076252
                                    000027.SZ   -23.072296      9.300787e+09    -0.004648
                                    000039.SZ   -127.123420     1.663698e+10    0.015784
                                    000046.SZ   472.080256      4.545337e+10    -0.030490
                                    ... ...     ...     ...     ...
                        2017-11-30      601966.SH       -114.439776     6.752128e+09    0.019665
                                    601985.SH   -24.761223      3.108793e+10    0.000000
                                    601988.SH   -8.482753       8.283085e+11    0.010127
                                    601989.SH   -185.353589     1.131079e+11    -0.021330
    """
    all_dates = ind_ret_data.index.get_level_values(level=0).unique()
    if len(all_dates) < 2:
        raise ValueError('data error')

    ind_names = get_ind_names(ind_ret_data)
    not_ind = [col for col in ind_ret_data.columns if col not in ind_names]

    last_model = None
    dict_ret = OrderedDict()
    for dt, frame in ind_ret_data.groupby(level=0):
        frame = frame.loc[dt]
        frame_x = frame[ind_names]
        frame_y = frame['NEXT_RET']
        linreg = LinearRegression()
        linreg.fit(frame_x, frame_y)
        if dt != all_dates[0]:
            pred = last_model.predict(frame[ind_names])
            df = frame.loc[:, not_ind]
            df['PRED_RET'] = pred
            dict_ret[dt] = df
        last_model = linreg

    ret = pd.concat(dict_ret.values(), keys=dict_ret.keys())

    return ret


def code_analysis(ind_ret_data, group_num=5, ind_direction=None, industry_type='sw', industry_level=1):

    return sector_analysis(ind_ret_data, group_num, ind_direction, industry_type, industry_level)