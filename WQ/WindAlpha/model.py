Type:        module
String form: <module 'WindAlpha.model' from '/opt/conda/lib/python3.5/site-packages/WindAlpha/model.py'>
File:        /opt/conda/lib/python3.5/site-packages/WindAlpha/model.py
Source:     
from .analysis import *
from .data_type import IndicatorAnalysis
from datetime import datetime
from .constant import CAP_CODES
from tqdm import tqdm_notebook
from math import ceil


class AlphaModel(object):
    """

    """
    def __init__(self,
                 stock_code,
                 ind_codes,
                 start_date,
                 end_date,
                 period='M',
                 bench_code=None,
                 is_index=True,
                 include_st=False,
                 include_suspend=False,
                 include_new_stock=False,
                 ipo_days=60):

        self.stock_code = stock_code
        self.ind_codes = ind_codes
        self.start_date = start_date
        self.end_date = end_date
        self.period = period
        self.is_index = is_index
        self.include_st = include_st
        self.include_suspend = include_suspend
        self.include_new_stock = include_new_stock
        self.ipo_days = ipo_days
        self.bench_code = bench_code

        if is_index:
            if not bench_code:
                bench_code = stock_code
        else:
            bench_code = '000300.SH'

        self.bench_code = bench_code

        self._analysis_config = {
            # 'extreme_method': 'mad',
            # 'scale_method': 'normal',
            # 'ic_method': 'rank',
            # 'ret_method': 'cap',
            # 'turnover_method': 'count',
            # 'group_num': 5,
            # 'ind_direction': None,
            # 'industry_neu': False,
            # 'industry_type': 'sw',
            # 'industry_level': 1
        }

        self._model_config = {}

        self.set_analysis_config()
        self.set_model_config()

        # self._model_config = self._analysis_config
        # self._model_config['select_ind'] = None
        # self._model_config['model_type'] = 'score'
        # self._model_config['score_method'] = 'equal'
        self.raw_data = pd.DataFrame()

    def _get_data(self):
        self.raw_data = prepare_raw_data(self.stock_code,
                                         self.ind_codes,
                                         self.start_date,
                                         self.end_date,
                                         self.period,
                                         self.is_index,
                                         self.include_st,
                                         self.include_suspend,
                                         self.include_new_stock,
                                         self.ipo_days)
    @property
    def analysis_config(self):
        return self._analysis_config

    @property
    def model_config(self):
        return self._model_config

    def set_model_config(self,
                         model_type='score',
                         select_ind=None,
                         extreme_method='mad',
                         scale_method='normal',
                         ic_method='rank',
                         ret_method='cap',
                         turnover_method='count',
                         group_num=5,
                         ind_direction=None,
                         industry_neu=False,
                         industry_type='sw',
                         industry_level=1,
                         score_method='equal'
                         ):

        new_config = locals()
        del new_config['self']
        self._model_config.update(new_config)

    def set_analysis_config(self,
                            extreme_method='mad',
                            scale_method='normal',
                            ic_method='rank',
                            ret_method='cap',
                            turnover_method='count',
                            group_num=5,
                            ind_direction=None,
                            industry_neu=False,
                            industry_type='sw',
                            industry_level=1
                            ):
        new_config = locals()
        del new_config['self']
        self._analysis_config.update(new_config)

    def run_single_indicator_analysis(self,
                                      extreme_method=None,
                                      scale_method=None,
                                      ic_method=None,
                                      ret_method=None,
                                      turnover_method=None,
                                      group_num=None,
                                      ind_direction=None,
                                      industry_neu=None,
                                      industry_type=None,
                                      industry_level=None
                                      ):
        param_dict = locals()
        total_steps = 7

        with tqdm_notebook(total=total_steps) as pbar:

            pbar.set_description("参数设置")

            for k, v in param_dict.items():
                if v and k != 'self':
                    self._analysis_config.update({k: v})

            extreme_method = self._analysis_config['extreme_method']
            scale_method = self._analysis_config['scale_method']
            ic_method = self._analysis_config['ic_method']
            ret_method = self._analysis_config['ret_method']
            turnover_method = self._analysis_config['turnover_method']
            group_num = self._analysis_config['group_num']
            ind_direction = self._analysis_config['ind_direction']
            industry_neu = self._analysis_config['industry_neu']
            industry_type = self._analysis_config['industry_type']
            industry_level = self._analysis_config['industry_level']

            pbar.update(1)
            pbar.set_description("提取数据")

            if self.raw_data.empty:
                self._get_data()

            pbar.update(1)
            pbar.set_description('数据处理')
            ind_data = process_raw_data(self.raw_data,
                                        extreme_num=3,
                                        extreme_method=extreme_method,
                                        scale_method=scale_method)
            pbar.update(1)
            pbar.set_description('ic分析')

            ret = IndicatorAnalysis()
            ret.ic_analysis = ic_analysis(ind_data, ic_method)
            pbar.update(1)
            pbar.set_description('收益率分析')
            ret.return_analysis = return_analysis(ind_data,
                                                  self.bench_code,
                                                  self.start_date,
                                                  self.end_date,
                                                  self.period,
                                                  ret_method,
                                                  group_num,
                                                  ind_direction,
                                                  industry_neu,
                                                  industry_type,
                                                  industry_level)
            pbar.update(1)
            pbar.set_description('换手率分析')
            ret.turnover_analysis = turnover_analysis(ind_data,
                                                      turnover_method,
                                                      group_num,
                                                      ind_direction)
            pbar.update(1)
            pbar.set_description('版块分析')
            ret.sector_analysis = sector_analysis(ind_data,
                                                  group_num,
                                                  ind_direction,
                                                  industry_type,
                                                  industry_level)
            pbar.update(1)
            pbar.set_description('分析完成')
        return ret

    def run_multi_indicators_analysis(self,
                                  model_type='score',
                                  select_ind=None,
                                  extreme_method=None,
                                  scale_method=None,
                                  ic_method=None,
                                  ret_method=None,
                                  turnover_method=None,
                                  group_num=None,
                                  ind_direction=None,
                                  industry_neu=None,
                                  industry_type=None,
                                  industry_level=None
                                  ):
        param_dict = locals()
        total_steps = 8
        with tqdm_notebook(total=total_steps) as pbar:

            pbar.set_description("参数设置")
            for k, v in param_dict.items():
                if v and k != 'self':
                    self._model_config.update({k: v})

            pbar.update(1)
            pbar.set_description("提取数据")
            if self.raw_data.empty:
                self._get_data()
            ind_data = self.raw_data

            select_ind = self._model_config['select_ind']
            extreme_method = self._model_config['extreme_method']
            scale_method = self._model_config['scale_method']
            ic_method = self._model_config['ic_method']
            ret_method = self._model_config['ret_method']
            turnover_method = self._model_config['turnover_method']
            group_num = self._model_config['group_num']
            ind_direction = self._model_config['ind_direction']
            industry_neu = self._model_config['industry_neu']
            industry_type = self._model_config['industry_type']
            industry_level = self._model_config['industry_level']
            score_method = self._model_config['score_method']

            if select_ind:
                ind_data = ind_data[select_ind]

            pbar.update(1)
            pbar.set_description('数据处理')
            ind_data = process_raw_data(ind_data,
                                        extreme_num=3,
                                        extreme_method=extreme_method,
                                        scale_method=scale_method)

            pbar.update(1)
            pbar.set_description('创建模型')
            if model_type == 'score':
                score_data = score_indicators(ind_data, score_method, ind_direction='ascending', ic_window=12)
            elif model_type == 'regress':
                score_data = regress_indicators(ind_data)

            ret = IndicatorAnalysis()

            pbar.update(1)
            pbar.set_description('ic分析')

            ret.ic_analysis = ic_analysis(score_data, ic_method)

            pbar.update(1)
            pbar.set_description('收益率分析')
            ret.return_analysis = return_analysis(score_data,
                                                  self.bench_code,
                                                  self.start_date,
                                                  self.end_date,
                                                  ret_method,
                                                  group_num,
                                                  ind_direction,
                                                  industry_neu,
                                                  industry_type,
                                                  industry_level)

            pbar.update(1)
            pbar.set_description('换手率分析')
            ret.turnover_analysis = turnover_analysis(score_data,
                                                      turnover_method,
                                                      group_num,
                                                      ind_direction)

            pbar.update(1)
            pbar.set_description('版块分析')
            ret.sector_analysis = sector_analysis(score_data,
                                                  group_num,
                                                  ind_direction,
                                                  industry_type,
                                                  industry_level)

            pbar.update(1)
            pbar.set_description('分析完成')

        return ret

    def select_stocks_by_model(self, date=None, stock_num=0.1):
        """
        运行模型，获取指定日期选股结果
        :param date:
        :return:
        """

        if not date:
            date = datetime.today().date().strftime('%Y%m%d')

        ind_codes = self._model_config['select_ind']
        if not ind_codes:
            ind_codes = self.ind_codes
        stock_code = self.stock_code

        data_codes = [CAP_CODES, "TRADE_STATUS", "IPO_DATE"]
        data_codes.extend(ind_codes)
        sub_cols = ind_codes.copy()
        sub_cols.append(CAP_CODES)
        stock_codes = stock_code
        if self.is_index:
            stock_codes = wset("sectorconstituent", date=date, windcode=self.stock_code).Data[1]
        # 获取因子数据ind_codes, 交易状态态数据（TRADE_STATUS）、首次上市日期数据（IPO_DATE)
        _, df_raw = wss(stock_codes, ",".join(data_codes), tradeDate=date, usedf=True)

        # 去除新上市的股票（ipo_days天以内）
        if not self.include_new_stock:
            date_least = tdaysoffset(-self.ipo_days, date, "").Data[0][0]
            df_raw = df_raw[df_raw['IPO_DATE'] <= date_least]

        # 去除停牌的股票
        if not self.include_suspend:
            df_raw = df_raw[df_raw['TRADE_STATUS'] == u'交易']

        # 去除ST的股票
        if not self.include_st:
            _, df_st = wset("sectorconstituent", "date=2018-07-13;sectorId=1000006526000000", usedf=True)
            not_st_lst = [code for code in df_raw.index if code not in df_st['wind_code'].tolist()]
            df_raw = df_raw.loc[not_st_lst]

        df_raw_ind = df_raw[sub_cols]
        dict_df = OrderedDict()
        dict_df[date] = df_raw_ind.dropna()
        df_res = pd.concat(dict_df.values(), keys=dict_df.keys())

        model_type = self._model_config['model_type']

        if model_type == 'score':
            score = score_indicators(df_res, self._model_config['score_method'])
            score_col = 'SCORE'

        elif model_type == 'regress':
            score = regress_indicators(df_res)
            score_col = 'PRED_RET'

        gp_score = add_group(ind_ret_data=score,
                             group_num=2,
                             industry_neu=self._model_config['industry_neu'],
                             industry_type=self._model_config['industry_type'],
                             industry_level=self._model_config['industry_level'])

        stocks = gp_score[gp_score['GROUP']=='G01']

        stocks_list = stocks.sort_values(score_col).index.get_level_values(1).tolist()

        if stock_num<1:
            selected = stocks_list[0:ceil(len(stocks_list)*stock_num)]

        else:
            selected = stocks_list[0:stock_num]

        return selected