# -*- coding: utf-8 -*-
"""
Created on Thu Nov  8 11:49:46 2018

@author: kite
"""

#Type:        module
#String form: <module 'WindAlgo.BktData' from '/opt/conda/lib/python3.5/site-packages/WindAlgo/BktData.py'>
#File:        /opt/conda/lib/python3.5/site-packages/WindAlgo/BktData.py
#Source:     
from ctypes import *
from datetime import datetime, date, time, timedelta
from WindPy import *
import pandas as pd
import numpy as np
import re
import collections
import json


class WindFrame(object):
    def __init__(self, fields, index, datalist, rows=None):
        if isinstance(rows, list):
            self.rows = rows
        elif isinstance(rows, dict):
            self.rows = []
            self.rows.append(rows)
        else:
            self.rows = []

        self.fields = fields
        self.index = index
        self.datalist = datalist
        self.__filed2idx__ = {}

        for i in range(len(fields)):
            self.__filed2idx__[fields[i]] = i

        try:
            for i in range(len(index)):
                # item = dict()
                item = OrdDictSub()
                for j in range(len(fields)):
                    item[fields[j]] = datalist[j][i]

                self.rows.append(item)
        except:
            return

    def append(self, row):
        self.rows.append(row)

    def __len__(self):
        # print("in __len__")
        return len(self.rows)

    def __iter__(self):
        # print("in __iter__")
        return iter(self.rows)

    def __str__(self):
        limit = 4
        # if len(self.rows) < limit:
        # return "\n".join([str(self.rows[x]) for x in range(len(self.rows))])
        return "\n".join([json.dumps(self.rows[x], cls=DateEncoder) for x in range(len(self.rows))])
        # else:
        #     tmp = [json.dumps(self.rows[x]) for x in range(limit-1)]
        #     return "\n".join(tmp) + "\n......\n" + json.dumps(self.rows[len(self.rows)-1])

    __repr__ = __str__

    def get_field(self, field):
        try:
            if len(self.datalist) < 1:
                return []
            if isinstance(field, int):
                return self.datalist[field]
            elif isinstance(field, str):
                return self.datalist[self.__filed2idx__[field]]
            else:
                return None
        except KeyError:
            return None

    def get_rows(self, idx, first=False):
        res = []
        key = None
        value = None
        if isinstance(idx, str):
            try:
                dateformat1 = r'^(?:(?!0000)[0-9]{4}(?:(?:0[1-9]|1[0-2])(?:0[1-9]|1[0-9]|2[0-8])|(?:0[13-9]|1[0-2])' \
                              r'(?:29|30)|(?:0[13578]|1[02])31)|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|' \
                              r'(?:0[48]|[2468][048]|[13579][26])00)0229)$'
                dateformat2 = r'^(?:(?!0000)[0-9]{4}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-8])|' \
                              r'(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[0-9]{2}' \
                              r'(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)-02-29)$'
                dateformat3 = r'^(?:(?!0000)[0-9]{4}(?:(?:0[1-9]|1[0-2])(?:0[1-9]|1[0-9]|2[0-8])|' \
                              r'(?:0[13-9]|1[0-2])(?:29|30)|(?:0[13578]|1[02])31)|(?:[0-9]{2}' \
                              r'(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)0229)' \
                              r'([\s](0[0-9]|1[0-9]|2[0-3])([0-5][0-9])?)?$'
                dateformat4 = r'^(?:(?!0000)[0-9]{4}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-8])|' \
                              r'(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[0-9]{2}' \
                              r'(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)-02-29)' \
                              r'([\s](0[0-9]|1[0-9]|2[0-3])([0-5][0-9])?)?$'
                if re.fullmatch(dateformat1, idx):
                    value = datetime.strptime(idx, '%Y%m%d')
                    key = 'time'
                elif re.fullmatch(dateformat2, idx):
                    value = datetime.strptime(idx, '%Y-%m-%d')
                    key = 'time'
                elif re.fullmatch(dateformat3, idx):
                    value = datetime.strptime(idx, '%Y%m%d %H%M')
                    key = 'time'
                elif re.fullmatch(dateformat4, idx):
                    value = datetime.strptime(idx, '%Y-%m-%d %H%M')
                    key = 'time'
                else:
                    value = idx
                    key = 'code'
            except:
                return None
        elif isinstance(idx, int):
            # try:
            return self.rows[idx]
        # except IndexError:
        #     return None
        elif isinstance(idx, datetime):
            key = 'time'
            value = idx
        else:
            return None

        try:
            for item in self.rows:
                if isinstance(item[key], str):
                    if item[key].lower() == value.lower():
                        if not first:
                            res.append(item)
                        else:
                            return item
                else:
                    if item[key] == value:
                        if not first:
                            res.append(item)
                        else:
                            return item
            if len(res) > 0:
                return res
            else:
                print('keyError: ' + idx)
                raise KeyError(idx)
                # return None
        except:
            return None

    def __getitem__(self, idx):
        return self.get_rows(idx, first=True)

    def get_dataframe(self):
        if self.index == 0 or self.index == '0':
            df = pd.DataFrame([], index=self.fields)
        else:
            df = pd.DataFrame(self.datalist, columns=self.index)
            df.index = self.fields
        return df.T


class SMatrix(Structure):
    _fields_ = [("var", c_variant),
                ("safe", c_safearray)]


class BKTData(Structure):
    _fields_ = [("ArrCodes", c_char_p),
                ("ArrFields", c_char_p),
                ("Data", POINTER(SMatrix))]

    def __getVarientValue(self, index, totalCount):
        ltype = self.Data[index].var.vt
        if ltype == VT_ARRAY | VT_I4:
            return [int(x) for x in self.Data[index].safe.plVal[0:totalCount]]

        if ltype == VT_ARRAY | VT_I8:
            return [int(x) for x in self.Data[index].safe.pllVal[0:totalCount]]

        if ltype == VT_ARRAY | VT_R8:
            return [float('%.4f' % x) for x in self.Data[index].safe.pdblVal[0:totalCount]]

        if ltype == VT_ARRAY | VT_DATE:
            return [datetime.fromtimestamp(x) for x in self.Data[index].safe.plVal[0:totalCount]]

        if ltype == VT_ARRAY | VT_BSTR:
            return [x.decode("utf8") for x in self.Data[index].safe.pbstrVal[0:totalCount]]
        return []

    @property
    def data(self):
        try:
            fields = self.ArrFields.decode("utf8").split("&&")
            fields = list(filter(lambda x: x != '', fields))

            if self.Data[0].safe.rgsabound[0].cElements == 0:
                return WindFrame(fields, 0, [])

            index = self.ArrCodes.decode("utf8").split("&&")
            index = list(filter(lambda x: x != '', index))
            try:
                index = list(map(lambda x: int(x), index))
            except:
                pass

            datalist = [self.__getVarientValue(x, len(index)) for x in range(len(fields))]
            ret = WindFrame(fields, index, datalist)
        except:
            ret = None

        return ret


class OrdDictSub(collections.OrderedDict):
    def __init__(self):
        super(OrdDictSub, self).__init__()

    def __str__(self):
        return json.dumps(self, cls=DateEncoder)


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('datetime.datetime(%Y, %m, %d, %H, %M)')
        elif isinstance(obj, date):
            return obj.strftime("datetime.date(%Y, %m, %d)")
        else:
            return json.JSONEncoder.default(self, obj)