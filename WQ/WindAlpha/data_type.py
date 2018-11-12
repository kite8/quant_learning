# Type:        module
# String form: <module 'WindAlpha.data_type' from '/opt/conda/lib/python3.5/site-packages/WindAlpha/data_type.py'>
# File:        /opt/conda/lib/python3.5/site-packages/WindAlpha/data_type.py
# Source:     
# -*- coding: utf-8 -*-
class IndicatorAnalysis(object):
    def __init__(self, ind_name=None, return_analysis=None,
                 information_coefficient_analysis=None, turnover_analysis=None, sector_analysis=None):
        self.ind_name = ind_name
        self.return_analysis = return_analysis
        self.ic_analysis = information_coefficient_analysis
        self.turnover_analysis = turnover_analysis
        self.sector_analysis = sector_analysis


class ReturnAnalysis(object):
    def __init__(self, ind_name=None, return_stats=None, benchmark_return=None,
                 group_mean_return=None, group_cum_return=None):
        self.ind_name = ind_name
        self.return_stats = return_stats
        self.benchmark_return = benchmark_return
        self.group_mean_return = group_mean_return
        self.group_cum_return = group_cum_return


class ICAnalysis(object):
    def __init__(self, ind_name=None, IC_series=None, IC_statistics=None, IC_decay=None):
        self.ind_name = ind_name
        self.ic_series = IC_series
        self.ic_stats= IC_statistics
        self.ic_decay = IC_decay


class TurnOverAnalysis(object):
    def __init__(self, ind_name=None, buy_signal=None, auto_correlation=None,
                 turnover=None):
        self.ind_name = ind_name
        self.buy_signal = buy_signal
        self.auto_corr = auto_correlation
        self.turnover = turnover


class SectorAnalysis(object):
    def __init__(self, ind_name=None,group_industry_ratio=None, group_cap_mean=None, group_industry_mean_ratio=None, group_stock_list=None):
        self.ind_name = ind_name
        self.group_cap_mean = group_cap_mean
        self.group_industry_mean_ratio = group_industry_mean_ratio
        self.group_stock_list = group_stock_list
        self.group_industry_ratio = group_industry_ratio