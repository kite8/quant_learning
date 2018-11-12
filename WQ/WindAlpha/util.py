# Type:        module
# String form: <module 'WindAlpha.util' from '/opt/conda/lib/python3.5/site-packages/WindAlpha/util.py'>
# File:        /opt/conda/lib/python3.5/site-packages/WindAlpha/util.py
# Source:     
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from .constant import CAP_CODES
from scipy import stats, optimize
from functools import wraps, partial
from .get_data import *
import pandas as pd


def reduce_index_level(data):
    if isinstance(data, pd.MultiIndex):
        index_level0 = data.index.get_level_valuse(0).unique()
        if len(index_level0) == 1:
            data = data.reset_index(level=0, drop=True)
    return data


def process_ind_wrapper(func):
    @wraps(func)
    def wrapper(data, *args, **kwargs):
        ind_names = get_ind_names(data)
        ind_data = data[ind_names]

        processed_data = func(ind_data, *args, **kwargs)
        data.loc[:, ind_names] = processed_data
        return data

    return wrapper


@process_ind_wrapper
def extreme_process(data, num=3, method="mad"):
    """
    去极值处理，极值的判断可以根据标准差或者平均绝对离差（MAD)，如果数值超过num个判断标准则使其等于num个标准
    """
    if isinstance(data, pd.Series):
        data = data.to_frame()
    mu = data.mean(axis=0)
    ind = mean_abs_deviation(data) if method == 'mad' else data.std()

    try:
        data = data.clip(lower=mu - num * ind, upper=mu + num * ind, axis=1)
    except Exception as e:
        print(e)
    return data


def scale_process(data, method='normal', cap_col=None):
    """
    数据标准化处理
    """
    data_ = data.copy()
    ind_names = get_ind_names(data)
    if method == 'normal':
        data_.loc[:, ind_names] = (data_.loc[:, ind_names] - data_.loc[:, ind_names].mean()) / data_.loc[:,
                                                                                               ind_names].std()
    elif method == 'cap':
        if not cap_col:
            cap_col = CAP_CODES
        if cap_col not in data.columns:
            raise KeyError('Cannot found market capitializaton by cap_col: {}'.format(cap_col))
        cap_weight = data_.loc[:, cap_col] / data_.loc[:, cap_col].sum()
        avg = (data_.loc[:, ind_names] * cap_weight).sum()
        data_.loc[:, ind_names] = (data_.loc[:, ind_names] - avg) / data_.loc[:, ind_names].std()
    else:
        raise ValueError("method can only be one value in ['normal','cap']")
    return data_


def mean_abs_deviation(data, axis=0):
    """
    计算MAD平均绝对离差
    """
    return ((data - data.mean(axis=axis)).abs()).mean(axis=axis)


def get_ind_names(ind_ret_data, not_ind=[CAP_CODES, "NEXT_RET", "GROUP", "INDUSTRY"]):
    columns = ind_ret_data.columns.tolist()
    ind_names = [i for i in columns if i not in not_ind]
    return ind_names


def info_coeff(ind_data, ret_data, method='rank', cov=None):
    """
    :param ind_data: DataFrame or Series, current term factor data of each stock
    :param ret_data: DataFrame or Series, next term return data of each stock
    :param method: str, default 'normal', or you can choose 'rank' or 'risk_adj'
    :param cov: (optional) numpy.array, covirance matrix of stocks
    :return:
    """
    ind_data, ret_data = ind_data.align(ret_data, join="inner", axis=0)
    if method == 'normal':
        return stats.pearsonr(ind_data, ret_data)
    elif method == "rank":
        return stats.spearmanr(ind_data, ret_data)
    elif method == 'risk_adj':
        return _risk_IC(ind_data, ret_data, cov)


def _risk_IC(ind_data, ret_data, cov):
    """
    风险调整信息系数
    cov协方差矩阵
    """
    n = len(ind_data)
    W = np.ones([n]) / n
    rf = 0.02
    R = ret_data.values
    target = lambda W: 1 / \
                       ((sum(W * R) - rf) / np.sqrt(
                           np.dot(np.dot(W, cov), W)))
    b = [(0., 1.) for i in range(n)]  # boundary condition
    c = ({'type': 'eq', 'fun': lambda W: sum(W) - 1.})  # summation condition
    optimized = optimize.minimize(
        target, W, method='SLSQP', bounds=b, constraints=c)
    weights = optimized.x
    ret_data_w = ret_data * weights
    ret = stats.pearsonr(ind_data, ret_data_w)
    return list(ret)


def count_turnover(cur_codes, nex_codes):
    """
    个数法
    """
    current_codes = set(cur_codes.keys())
    next_codes = set(nex_codes.keys())
    try:
        ret = len(next_codes - current_codes) * 1.0 / len(current_codes)
    except ZeroDivisionError:
        ret = np.inf
    return ret


def capwt_turnover(cur_codes, nex_codes):
    """
    权重法
    """
    current_df = pd.Series(cur_codes, name=CAP_CODES).to_frame()
    current_weights = current_df[CAP_CODES] / current_df[CAP_CODES].sum()
    next_df = pd.Series(nex_codes, name=CAP_CODES).to_frame()
    next_weights = next_df[CAP_CODES] / next_df[CAP_CODES].sum()

    cur, nxt = current_weights.align(
        next_weights, join='outer', fill_value=0)
    ret = (cur - nxt).abs().sum() / 2
    return ret


def _filter_stocks(stocks, date, code_col=None, type=None, ipo_days=None):
    """
    股票过滤
    :param stocks:
    :param date:
    :param code_col:
    :param type:
    :return:
    """
    stock_lst = stocks
    if isinstance(stocks, pd.DataFrame):
        if not code_col:
            stock_lst = stocks.index.tolist()
        else:
            stock_lst = stocks[code_col].tolist()

    if type == 'st':
        _, df_st = wset("sectorconstituent", date=date, sectorId="1000006526000000", usedf=True)
        new_lst = [code for code in stock_lst if code not in df_st['wind_code'].tolist()]

    elif type == 'suspend':
        _, df_sus = wss(stock_lst, "TRADE_STATUS", tradeDate=date, usedf=True)
        new_lst = df_sus[df_sus['TRADE_STATUS'] == u'交易'].index.tolist()

    elif type == 'ipo':
        _, df_ipo = wss(stock_lst, "IPO_DATE", tradeDate=date, usedf=True)

        date_least = tdaysoffset(-ipo_days, date, "").Data[0][0]
        new_lst = df_ipo[df_ipo['IPO_DATE'] <= date_least].index.tolist()
    else:
        raise ValueError("type={}".format(type))

    ret = new_lst

    if isinstance(stocks, pd.DataFrame):
        if not code_col:
            ret = stocks.loc[new_lst]
        else:
            ret = stocks[stocks[code_col].isin(new_lst)]

    return ret


def filter_st_stocks(stocks, date, code_col=None):
    """
    去除st的股票
    :param stocks:  str or dataFrame
    :return:
    """
    return _filter_stocks(stocks, date, code_col, type='st', ipo_days=None)


def filter_suspend_stocks(stocks, date, code_col=None):
    """
    去除st的股票
    :param stocks:
    :return:
    """
    return _filter_stocks(stocks, date, code_col, type='sus', ipo_days=None)



def filter_new_stocks(stocks, date, code_col=None):
    """
    去除st的股票
    :param stocks:
    :return:
    """
    return _filter_stocks(stocks, date, code_col, type='ipo', ipo_days=60)