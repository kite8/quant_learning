# -*- coding: utf-8 -*-
"""
Created on Sun Oct 14 13:12:41 2018

@author: kite
"""
from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne
from database import DB_CONN
import tushare as ts
import pandas as pd
import datetime, time
import QUANTAXIS as QA

"""
从tushare获取日k数据，保存到本地的MongoDB数据库中
"""

class DailyCrawler:
    def __init__(self):
        self.daily = DB_CONN['daily']
        self.daily_hfq = DB_CONN['daily_hfq']
        self.daily.create_index(
            [('code',ASCENDING), ('date',ASCENDING), ('index',ASCENDING)])
        self.daily_hfq.create_index(
            [('code',ASCENDING), ('date',ASCENDING), ('index',ASCENDING)])
        
        self.daily.create_index(
            [('code',ASCENDING), ('date',ASCENDING)])
        self.daily_hfq.create_index(
            [('code',ASCENDING), ('date',ASCENDING)])
        
    def crawl_index(self, code, start, end=None):
        """
        
        """
        end = start if end is None else end
        start = str(start)[0:10]
        end = str(end)[0:10]
            
#        if type(code) == str:
#            df_daily = ts.get_k_data(code, start, end, index=True)
#        df = QA.QA_fetch_index_day_adv(code, start, end).data
#        df = df.reset_index()
#        df.date = df.date.apply(lambda x : x.strftime('%Y-%m-%d'))
#        self.save_data(code, df, self.daily, {'index':True})
        
#        else:
        total = len(code)
        for i, c in enumerate(code):
            df = QA.QA_fetch_index_day_adv(c, start, end).data
            df = df.reset_index()
            df.date = df.date.apply(lambda x : x.strftime('%Y-%m-%d'))
            self.save_data(c, df, self.daily, {'index':True})
            print('index日线数据获取进度: (%s/%s)' % (i+1, total))
        
    def crawl(self, code, start, end=None):
        """
        
        """
        end = start if end is None else end
        start = str(start)[0:10]
        end = str(end)[0:10]
        
        total = len(code)
        for i, c in enumerate(code):
            try:
                data = QA.QA_fetch_stock_day_adv(c, start, end)
                df = data.data
                df = df.reset_index()
                df.date = df.date.apply(lambda x : x.strftime('%Y-%m-%d'))
                self.save_data(c, df, self.daily, {'index':False})
                
                df_hfq = data.to_hfq().data
                df_hfq = df_hfq.reset_index()
                df_hfq.date = df_hfq.date.apply(lambda x : x.strftime('%Y-%m-%d'))
                self.save_data(c, df_hfq, self.daily_hfq, {'index':False})
            except:
                err_code.append(c)
                print('code : %s, 没有数据，请核实' % c)
            print('股票日线数据获取进度: (%s/%s)' % (i+1, total))
                    
    def save_data(self, code, df_daily, collection, extra_fields=None):
        """
        
        """
        update_requests = []
        
        for i in df_daily.index:
            doc = dict(df_daily.loc[i, ['code', 'date', 'open', 'high', 'low', 'close','volume',
                      ]])
            
            if extra_fields is not None:
                doc.update(extra_fields)
                
            update_requests.append(
                    UpdateOne(
                            {'code':doc['code'], 'date':doc['date'], 'index':doc['index']},
                            {'$set':doc},
                            upsert=True)
                    )
                    
        if len(update_requests) > 0:
            update_result = collection.bulk_write(update_requests, ordered=False)
            print('保存日线数据，代码： %s, 插入：%4d条, 更新：%4d条' %
              (code, update_result.upserted_count, update_result.modified_count),
              flush=True)
    

            
if __name__ == '__main__':
    dc = DailyCrawler()
    
    index_code_list = ['000001', '000300', '399001', '399005', '399006']
    _df = pd.read_excel('data/stock_basic.xlsx', header=0, dtype={'code':str})
    stock_code_list = _df.code.tolist()
#    stock_code_list = ['000001', '000300', '399001', '399005', '399006']
    
    start = '2015-01-01'
    end = '2018-09-30'
    err_code = []
    tic = time.process_time()
    dc.crawl_index(index_code_list, start, end)
    dc.crawl(stock_code_list, start, end)
    
    toc = time.process_time()
    print('cost time : %.2fs' % (toc-tic))
    print(err_code)

    
    
    