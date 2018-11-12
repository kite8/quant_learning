# -*- coding: utf-8 -*-

"""
Created on Sun Oct 14 13:09:41 2018

@author: kite
"""

from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne

DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['quant_01']