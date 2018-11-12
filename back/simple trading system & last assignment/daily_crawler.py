# -*- coding: utf-8 -*-
"""
Created on Sun Oct 14 13:12:41 2018

@author: kite
"""
from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne
from database import DB_CONN
import tushare as ts
import datetime


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
        
    def crawl_index(self, code, start, end=None):
        """
        
        """
        end = start if end is None else end
        start = str(start)[0:10]
        end = str(end)[0:10]
            
        if type(code) == str:
            df_daily = ts.get_k_data(code, start, end, index=True)
            self.save_data(code, df_daily, self.daily, {'index':True})
        
        else:
            for c in code:
                df_daily = ts.get_k_data(c, start, end, autype=None)
                self.save_data(c, df_daily, self.daily, {'index':True})
                    
    def save_data(self, code, df_daily, collection, extra_fields=None):
        """
        
        """
        update_requests = []
        
        for df_index in df_daily.index:
            daily_obj = df_daily.loc[df_index]
            doc = self.daily_obj_2_doc(code, daily_obj)
            
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
    
    def crawl(self, code, start, end=None):
        """
        
        """
        end = start if end is None else end
        start = str(start)[0:10]
        end = str(end)[0:10]
        
        for c in code:
            df_daily = ts.get_k_data(c, start, end, autype=None)
            self.save_data(c, df_daily, self.daily, {'index':False})
            
            df_daily_hfq = ts.get_k_data(c, start, end, autype='hfq')
            self.save_data(c, df_daily_hfq, self.daily_hfq, {'index':False})
    
    
    @staticmethod
    def daily_obj_2_doc(code, daily_obj):
        return {
            'code': code,
            'date': daily_obj['date'],
            'close': daily_obj['close'],
            'open': daily_obj['open'],
            'high': daily_obj['high'],
            'low': daily_obj['low'],
            'volume': daily_obj['volume']
        }    
        
if __name__ == '__main__':
    dc = DailyCrawler()
    
    index_code_list = ['000001', '000300', '399001', '399005', '399006']
    stock_code_list = ts.get_stock_basics().index.tolist()
    
    start = '2015-01-01'
    end = '2015-12-31'
    dc.crawl_index(index_code_list, start, end)
    dc.crawl(stock_code_list, start, end)
    
    
    