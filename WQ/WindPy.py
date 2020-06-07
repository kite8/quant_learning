# Type:        module
# String form: <module 'WindPy' from '/opt/conda/lib/python3.6/WindPy.py'>
# File:        /opt/conda/lib/python3.6/WindPy.py
# Source:     
from ctypes import *
import threading
import traceback
from datetime import datetime, date, time, timedelta
import time as t
import re
from WindData import *
from WindBktData import *
from XMLParser import XMLReader
import pandas as pd
import logging
import getpass

r = XMLReader("/wind/serverapi/wsq_decode.xml")
# import speedtcpclient as client

expolib = None
speedlib = None
TDB_lib = None
c_lib = None
# For test use! Should be replaced with a real userID
# userID = "1214779"
api_retry = 1
interval = 2

userName = getpass.getuser()
authDataPath = "/home/" + userName + "/.wind/authData"
authString = readFile(authDataPath)
# userID = str(getJsonTag(authString, 'accountID'))
# if userID == '':
#     userID = "1214779"

wind_log_path = "/usr/local/log/"


def DemoWSQCallback(out):
    print("DemoWSQCallback")
    print(out)


wsq_items = []
    
def g_wsq_callback(reqID, indata):
    out = WindData()
    out.set(indata, 3)
    out.RequestID = reqID
    
    id2rtField = {}
    for item in wsq_items:
        id2rtField[item['id']] = item['funname'].upper()

    tmp = [id2rtField[str(val)] for val in out.Fields]
    out.Fields = tmp

    out.Times = datetime.now().strftime('%Y%m%d %H:%M:%S')

    try:
        g_wsq_callback.callback_funcs[reqID](out)
    except:
        print(out)
    
SPDCBTYPE = CFUNCTYPE(None, c_int, POINTER(c_apiout))
spdcb = SPDCBTYPE(g_wsq_callback)
g_wsq_callback.callback_funcs = {}

REQUEST_ID_CANCELALL = 0
REQUEST_ID_SYNC = 1
REQUEST_ID_MAX_RESQUEST = 9999
REQUEST_ID_MIN_RESQUEST = 3

g_requestID = REQUEST_ID_MIN_RESQUEST  # The minimum id of NONE BLOCKING MODE


def retry(func):
    def wrapper(*args, **kargs):
        out = func(*args, **kargs)
        if not out:
            return out
        error_code = type_check(out)
        if error_code == -10:
            for i in range(api_retry):
                out = func(*args, **kargs)
                error_code = type_check(out)
                if error_code != -10:
                    break
        return out

    # 判断out类型，若带usedf参数则为tuple
    def type_check(out):
        if isinstance(out, tuple):
            error_code = out[0]
        else:
            error_code = out.ErrorCode
        return error_code

    return wrapper


class WindQnt:
    b_start = False

    def __static_var(var_name, inital_value):
        def _set_var(obj):
            setattr(obj, var_name, inital_value)
            return obj

        return _set_var

    def __stringify(arg):
        if arg is None:
            tmp = [""]
        elif arg == "":
            tmp = [""]
        elif isinstance(arg, str):
            a_l = arg.strip().split(',')
            arg = ','.join([a.strip() for a in a_l])
            tmp = [arg]
        elif isinstance(arg, list):
            tmp = [str(x) for x in arg]
        elif isinstance(arg, tuple):
            tmp = [str(x) for x in arg]
        elif isinstance(arg, float) or isinstance(arg, int):
            tmp = [str(arg)]
        elif str(type(arg)) == "<type 'unicode'>":
            tmp = [arg]
        else:
            tmp = None

        if tmp is None:
            return None
        else:
            return ";".join(tmp)

    def __parseoptions(self, arga=None, argb=None):
        options = WindQnt._WindQnt__stringify(self)

        if options is None:
            return None

        if isinstance(arga, tuple):
            for i in range(len(arga)):
                v = WindQnt._WindQnt__stringify(arga[i])
                if v is None:
                    continue
                else:
                    if options == "":
                        options = v
                    else:
                        options = options + ";" + v

        if isinstance(argb, dict):
            keys = argb.keys()
            for key in keys:
                v = WindQnt._WindQnt__stringify(argb[key])
                if v is None:
                    continue
                else:
                    if options == "":
                        options = str(key) + "=" + v
                    else:
                        options = options + ";" + str(key) + "=" + v

        return options

    @staticmethod
    def format_option(options):
        if options is None:
            return None
        option_f = options.replace(';', '&&')
        return option_f

    # with_time param means you can format hours:minutes:seconds, but not must be
    def __parsedate(self, with_time=False):
        d = self
        if d is None:
            d = datetime.today().strftime("%Y-%m-%d")
            return d
        elif isinstance(d, date):
            d = d.strftime("%Y-%m-%d")
            return d
        elif isinstance(d, datetime):
            d = d.strftime("%Y-%m-%d")
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
                YMD = date_time[0][:4] + '-' + date_time[0][4:6] + '-' + date_time[0][6:8]
                HMS = ''
                if with_time and len(date_time) == 2:
                    HMS = ' ' + date_time[1][:2] + ':' + date_time[1][2:4] + ':' + date_time[1][4:6]
                d = YMD + HMS
                return d
            except:
                return None
        return d

    # def __parsedate(d):
    #     if d is None:
    #         d = datetime.today().strftime("%Y-%m-%d")
    #         return d
    #     elif isinstance(d, date):
    #         d = d.strftime("%Y-%m-%d")
    #         return d
    #     elif isinstance(d, str):
    #         try:
    #             #Try to get datetime object from the user input string.
    #             #We will go to the except block, given an invalid format.
    #             if re.match(r'^(?:(?!0000)[0-9]{4}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)-02-29)$',d, re.I|re.M):
    #                 d = datetime.strptime(d, "%Y-%m-%d")
    #                 return d.strftime("%Y-%m-%d")
    #             elif re.match(r'^(?:(?!0000)[0-9]{4}(?:(?:0[1-9]|1[0-2])(?:0[1-9]|1[0-9]|2[0-8])|(?:0[13-9]|1[0-2])(?:29|30)|(?:0[13578]|1[02])31)|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)0229)$', d, re.I|re.M):
    #                 d = datetime.strptime(d, "%Y%m%d")
    #                 return d.strftime("%Y-%m-%d")
    #             else:
    #                 return None
    #         except:
    #             return None
    #     else:
    #         return None
    #
    #     return d

    def use_debug_file(self, debug_expo='/wind/serverapi/libExpoWrapperDebug.so',
                       debug_speed='/wind/serverapi/libSpeedWrapperDebug.so'):
        WindQnt.debug_expo = debug_expo
        WindQnt.debug_speed = debug_speed

    @staticmethod
    def format_wind_data(error_codes, msg):
        out = WindData()
        out.ErrorCode = error_codes
        out.Codes = ['ErrorReport']
        out.Fields = ['OUT MESSAGE']
        out.Times = datetime.now().strftime('%Y%m%d %H:%M:%S')
        out.Data = [[msg]]
        return out

    @staticmethod
    def to_dataframe(out):
        if out.ErrorCode != 0:
            return pd.DataFrame([out.ErrorCode], columns=['ErrorCode'])

        col = out.Times
        if len(out.Codes) == len(out.Fields) == 1:
            idx = out.Fields
        elif len(out.Codes) > 1 and len(out.Fields) == 1:
            idx = out.Codes
        elif len(out.Codes) == 1 and len(out.Fields) > 1:
            idx = out.Fields
        else:
            idx = None
        df = pd.DataFrame(out.Data, columns=col)
        if idx:
            df.index = idx

        return df.T.infer_objects()

    def isconnected(self):
        return 0

    class __start:
        def __init__(self):
            self.restype = c_int32
            self.argtypes = [c_wchar_p, c_wchar_p, c_int32]
            self.lastCall = 0

        def __call__(self, show_welcome=True, retry=1):
            global expolib
            global speedlib
            global TDB_lib
            global c_lib
            global api_retry
            if t.time() - self.lastCall > interval:
                if WindQnt.b_start:
                    return
                WindQnt.b_start = True
                self.lastCall = t.time()
                TDB_lib = CDLL("/wind/serverapi/libtdb.so")
                c_lib = CDLL("/wind/serverapi/libtradeapi.so")
                c_lib.tLogon.restype = POINTER(c_variant)
                c_lib.tQuery.restype = POINTER(c_variant)
                c_lib.tLogout.restype = POINTER(c_variant)
                c_lib.tSendOrder.restype = POINTER(c_variant)
                c_lib.tCancelOrder.restype = POINTER(c_variant)
                if hasattr(WindQnt, "debug_expo"):
                    expolib = CDLL(WindQnt.debug_expo)
                else:
                    expolib = CDLL("/wind/serverapi/libExpoWrapper.so")
                expolib.SendMsg2Expo.restype = POINTER(c_apiout)

                if hasattr(WindQnt, "debug_speed"):
                    speedlib = CDLL(WindQnt.debug_speed)
                else:
                    speedlib = CDLL("/wind/serverapi/libSpeedWrapper.so")

                speedlib.SendMsg2SpeedAsyc.restype = POINTER(c_apiout)

                api_retry = int(retry) if int(retry) < 6 else 5
                if show_welcome:
                    print("COPYRIGHT (C) 2017 Wind Information Co., Ltd. ALL RIGHTS RESERVED.\n"
                          "IN NO CIRCUMSTANCE SHALL WIND BE RESPONSIBLE FOR ANY DAMAGES OR LOSSES\n"
                          "CAUSED BY USING WIND QUANT API FOR PYTHON.")
                return
            else:
                # print ("wait a while to start!")
                return ERR_WAIT

        def __str__(self):
            return ("Start the Wind Quant API")

    start = __start()

    class __wses:
        def __init__(self):
            self.restype = POINTER(c_apiout)
            self.argtypes = [c_wchar_p,c_wchar_p,c_wchar_p,c_wchar_p,c_wchar_p]
            self.lastCall = 0

        @retry
        def __call__(self, codes, fields, beginTime=None, endTime=None, options=None, *arga, **argb):
            # write_log('call wsd')
            s = int(t.time()*1000)
            if expolib is None:
                return WindQnt.format_wind_data(-103, '')

            if t.time() - self.lastCall < interval:
                t.sleep(interval)

            if isinstance(endTime, str):
                # 判断是否为日期宏，若不是，则调用parsedate方法
                endTime_compile = re.findall('\d\d\d\d\d\d\d\d', endTime.replace('-', ''))
                if endTime_compile:
                    endTime = WindQnt._WindQnt__parsedate(endTime)
            else:
                # 处理datetime类型日期
                endTime = WindQnt._WindQnt__parsedate(endTime)
            if endTime == None:
                print("Invalid date format of endTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01")
                return

            if isinstance(beginTime, str):
                beginTime_compile = re.findall('\d\d\d\d\d\d\d\d', beginTime.replace('-', ''))
                if beginTime_compile:
                    beginTime = WindQnt._WindQnt__parsedate(beginTime)
            else:
                beginTime = WindQnt._WindQnt__parsedate(beginTime)
            if beginTime == None:
                print("Invalid date format of beginTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01")
                return
            if(endTime==None):  endTime = datetime.today().strftime("%Y-%m-%d")
            if(beginTime==None):  beginTime = endTime

            # chech if the endTime is before than the beginTime
            # endD = datetime.strptime(endTime, "%Y-%m-%d")
            # beginD = datetime.strptime(beginTime, "%Y-%m-%d")
            # if (endD-beginD).days < 0:
            #     print("The endTime should be later than or equal to the beginTime!")
            #     return

            codes = WindQnt._WindQnt__stringify(codes)
            fields = WindQnt._WindQnt__stringify(fields)
            options = WindQnt._WindQnt__parseoptions(options, arga, argb)

            if codes == None or fields == None or options == None:
                print("Insufficient arguments!")
                return

            userID = str(getJsonTag(authString, 'accountID'))
            if userID == '':
                userID = "1214779"
            tmp = "wses|"+codes+"|"+fields+"|"+beginTime+"|"+endTime+"|"+options+"|"+userID
            tmp = tmp.encode("utf16") + b"\x00\x00"
            
            apiOut = expolib.SendMsg2Expo(tmp, len(tmp))
            self.lastCall = t.time()

            if apiOut.contents.ErrorCode == -1 or apiOut.contents.ErrorCode == -40521010:
                msg = 'Request Timeout'
                e = int(t.time()*1000)
                write_log(str(e-s) + ' call wses')
                return WindQnt.format_wind_data(-40521010, msg)
            else:
                out = WindData()
                out.set(apiOut, 1, asdate = True)
                if 'usedf' in argb.keys():
                    usedf = argb['usedf']
                    if usedf:
                        if not isinstance(usedf, bool):
                            print('the sixth parameter is usedf which should be the Boolean type!')
                            return
                        try:
                            if out.ErrorCode != 0:
                                df = pd.DataFrame(out.Data, index=out.Fields)
                                df.columns = [x for x in range(df.columns.size)]
                                return out.ErrorCode, df.T.infer_objects()
                            col = out.Times
                            if len(out.Codes) == len(out.Fields) == 1:
                                idx = out.Fields
                            elif len(out.Codes) > 1 and len(out.Fields) == 1:
                                idx = out.Codes
                            elif len(out.Codes) == 1 and len(out.Fields) > 1:
                                idx = out.Fields
                            else:
                                idx = None
                            df = pd.DataFrame(out.Data, columns=col)
                            if idx:
                                df.index = idx
                            e = int(t.time()*1000)
                            write_log(str(e-s) + ' call wsd')
                            return out.ErrorCode, df.T.infer_objects()
                        except Exception:
                            print(traceback.format_exc())
                            return
                if out.ErrorCode != 0:
                    if len(out.Data) != 0 and len(out.Data[0]) > 100:
                        if len(out.Data) > 1:
                            print(str(out.Data)[:10] + '...]...]')
                        else:
                            print(str(out.Data)[:10] + '...]]')
                    else:
                        print(out.Data)
                e = int(t.time()*1000)
                write_log(str(e-s) + ' call wses')
                return out

        def __str__(self):
            return ("WSES")

    wses = __wses()

    class __wsee:
        def __init__(self):
            self.restype = POINTER(c_apiout)
            self.argtypes = [c_wchar_p,c_wchar_p,c_wchar_p] #codes,fields,options
            self.lastCall = 0

        @retry
        def __call__(self, codes, fields, options=None, *arga, **argb):
            # write_log('call wsee')
            s = int(t.time()*1000)

            if expolib is None:
                return WindQnt.format_wind_data(-103, '')

            if t.time() - self.lastCall < interval:
                t.sleep(interval)
     
            codes = WindQnt._WindQnt__stringify(codes)
            fields = WindQnt._WindQnt__stringify(fields)
            options = WindQnt._WindQnt__parseoptions(options, arga, argb)
           
            if fields == None or options == None:
                print("Insufficient arguments!")
                return
             
            userID = str(getJsonTag(authString, 'accountID'))
            if userID == '':
                userID = "1214779"
            tmp = "wsee|"+codes+"|"+fields+"|"+options+"|"+userID
            tmp = tmp.encode("utf16") + b"\x00\x00"

            apiOut = expolib.SendMsg2Expo(tmp, len(tmp))
 
            self.lastCall = t.time()
            if apiOut.contents.ErrorCode == -1 or apiOut.contents.ErrorCode == -40521010:
                msg = 'Request Timeout'
                e = int(t.time()*1000)
                write_log(str(e-s) + ' call wsee')
                return WindQnt.format_wind_data(-40521010, msg)
            else:
                out = WindData()
                out.set(apiOut, 1, asdate=True)

                #将winddata类型数据改为dataframe格式
                if 'usedf' in argb.keys():
                    usedf = argb['usedf']
                    if usedf:
                        if not isinstance(usedf, bool):
                            print('the fourth parameter is usedf which should be the Boolean type!')
                            return
                        try:
                            if out.ErrorCode != 0:
                                df = pd.DataFrame(out.Data, index=out.Fields)
                                df.columns = [x for x in range(df.columns.size)]
                                return out.ErrorCode, df.T.infer_objects()
                            if out.Codes == 1 or out.Fields == 1:
                                return out.ErrorCode, WindQnt.to_dataframe(out)
                            else:
                                df = pd.DataFrame(out.Data, columns=out.Codes, index=out.Fields)
                                e = int(t.time()*1000)
                                write_log(str(e-s) + ' call wsee')
                                return out.ErrorCode, df.T.infer_objects()
                        except Exception as e:
                            print(traceback.format_exc())
                            return
                if out.ErrorCode != 0:
                    if len(out.Data) != 0 and len(out.Data[0]) > 100:
                        if len(out.Data) > 1:
                            print(str(out.Data)[:10] + '...]...]')
                        else:
                            print(str(out.Data)[:10] + '...]]')
                    else:
                        print(out.Data)
                e = int(t.time()*1000)
                write_log(str(e-s) + ' call wsee')
                return out
                
        def __str__(self):
            return ("wsee")
            
    wsee = __wsee()


    class __wsi:
        def __init__(self):
            self.restype = POINTER(c_apiout)
            self.argtypes = [c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p]
            self.lastCall = 0

        @retry
        def __call__(self, codes, fields, beginTime=None, endTime=None, options=None, usedf=False, *arga, **argb):
            # write_log('call wsi')
            s = int(t.time() * 1000)
            if expolib is None:
                return WindQnt.format_wind_data(-103, '')

            if t.time() - self.lastCall < interval:
                t.sleep(interval)

            # endTime = WindQnt._WindQnt__parsedate(endTime)
            # if endTime is None:
            #     print("Invalid date format of endTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01")
            #     return
            #
            # beginTime = WindQnt._WindQnt__parsedate(beginTime)
            # if beginTime is None:
            #     print("Invalid date format of beginTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01")
            #     return
            if (endTime is None):  endTime = datetime.today().strftime("%Y-%m-%d")
            if (beginTime is None):  beginTime = endTime

            # chech if the endTime is before than the beginTime
            # endD = datetime.strptime(endTime, "%Y-%m-%d")
            # beginD = datetime.strptime(beginTime, "%Y-%m-%d")
            # if (endD-beginD).days < 0:
            #     print("The endTime should be later than or equal to the beginTime!")
            #     return

            codes = WindQnt._WindQnt__stringify(codes)
            fields = WindQnt._WindQnt__stringify(fields)
            options = WindQnt._WindQnt__parseoptions(options, arga, argb)

            if codes is None or fields is None or options is None:
                print("Insufficient arguments!")
                return

            userID = str(getJsonTag(authString, 'accountID'))
            if userID == '':
                userID = "1214779"
            tmp = "wsi|" + codes + "|" + fields + "|" + beginTime + "|" + endTime + "|" + options + "|" + userID
            tmp = tmp.encode("utf16") + b"\x00\x00"

            apiOut = expolib.SendMsg2Expo(tmp, len(tmp))
            self.lastCall = t.time()

            if apiOut.contents.ErrorCode == -1 or apiOut.contents.ErrorCode == -40521010:
                msg = 'Request Timeout'
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call wsi')
                return WindQnt.format_wind_data(-40521010, msg)
            else:
                out = WindData()
                out.set(apiOut, 1, asdate=False)

                if usedf:
                    if not isinstance(usedf, bool):
                        print('the sixth parameter is usedf which should be the Boolean type!')
                        return
                    try:
                        if out.ErrorCode != 0:
                            df = pd.DataFrame(out.Data, index=out.Fields)
                            df.columns = [x for x in range(df.columns.size)]
                            return out.ErrorCode, df.T.infer_objects()
                        col = out.Times
                        if len(out.Codes) == len(out.Fields) == 1:
                            idx = out.Fields
                        elif len(out.Codes) > 1 and len(out.Fields) == 1:
                            idx = out.Codes
                        elif len(out.Codes) == 1 and len(out.Fields) > 1:
                            idx = out.Fields
                        else:
                            idx = None
                        df = pd.DataFrame(out.Data, columns=col)
                        if idx:
                            df.index = idx

                        e = int(t.time() * 1000)
                        write_log(str(e - s) + ' call wsi')
                        return out.ErrorCode, df.T.infer_objects()
                    except Exception:
                        print(traceback.format_exc())
                        return
                if out.ErrorCode != 0:
                    if len(out.Data) != 0 and len(out.Data[0]) > 100:
                        if len(out.Data) > 1:
                            print(str(out.Data)[:10] + '...]...]')
                        else:
                            print(str(out.Data)[:10] + '...]]')
                    else:
                        print(out.Data)
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call wsi')
                return out

        def __str__(self):
            return ("WSI")

    wsi = __wsi()

    class __wsd:
        def __init__(self):
            self.restype = POINTER(c_apiout)
            self.argtypes = [c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p]
            self.lastCall = 0

        @retry
        def __call__(self, codes, fields, beginTime=None, endTime=None, options=None, usedf=False, *arga, **argb):
            # write_log('call wsd')
            s = int(t.time() * 1000)
            if expolib is None:
                return WindQnt.format_wind_data(-103, '')

            if t.time() - self.lastCall < interval:
                t.sleep(interval)

            # endTime = WindQnt._WindQnt__parsedate(endTime)
            # if endTime is None:
            #     print("Invalid date format of endTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01")
            #     return
            #
            # beginTime = WindQnt._WindQnt__parsedate(beginTime)
            # if beginTime is None:
            #     print("Invalid date format of beginTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01")
            #     return
            if (endTime is None):  endTime = datetime.today().strftime("%Y-%m-%d")
            if (beginTime is None):  beginTime = endTime

            # chech if the endTime is before than the beginTime
            # endD = datetime.strptime(endTime, "%Y-%m-%d")
            # beginD = datetime.strptime(beginTime, "%Y-%m-%d")
            # if (endD-beginD).days < 0:
            #     print("The endTime should be later than or equal to the beginTime!")
            #     return

            codes = WindQnt._WindQnt__stringify(codes)
            fields = WindQnt._WindQnt__stringify(fields)
            options = WindQnt._WindQnt__parseoptions(options, arga, argb)

            if codes is None or fields is None or options is None:
                print("Insufficient arguments!")
                return

            userID = str(getJsonTag(authString, 'accountID'))
            if userID == '':
                userID = "1214779"
            tmp = "wsd|" + codes + "|" + fields + "|" + beginTime + "|" + endTime + "|" + options + "|" + userID
            tmp = tmp.encode("utf16") + b"\x00\x00"

            apiOut = expolib.SendMsg2Expo(tmp, len(tmp))
            self.lastCall = t.time()

            if apiOut.contents.ErrorCode == -1 or apiOut.contents.ErrorCode == -40521010:
                msg = 'Request Timeout'
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call wsd')
                return WindQnt.format_wind_data(-40521010, msg)
            else:
                out = WindData()
                out.set(apiOut, 1, asdate=True)

                if usedf:
                    if not isinstance(usedf, bool):
                        print('the sixth parameter is usedf which should be the Boolean type!')
                        return
                    try:
                        if out.ErrorCode != 0:
                            df = pd.DataFrame(out.Data, index=out.Fields)
                            df.columns = [x for x in range(df.columns.size)]
                            return out.ErrorCode, df.T.infer_objects()
                        col = out.Times
                        if len(out.Codes) == len(out.Fields) == 1:
                            idx = out.Fields
                        elif len(out.Codes) > 1 and len(out.Fields) == 1:
                            idx = out.Codes
                        elif len(out.Codes) == 1 and len(out.Fields) > 1:
                            idx = out.Fields
                        else:
                            idx = None
                        df = pd.DataFrame(out.Data, columns=col)
                        if idx:
                            df.index = idx

                        e = int(t.time() * 1000)
                        write_log(str(e - s) + ' call wsd')
                        return out.ErrorCode, df.T.infer_objects()
                    except Exception:
                        print(traceback.format_exc())
                        return
                if out.ErrorCode != 0:
                    if len(out.Data) != 0 and len(out.Data[0]) > 100:
                        if len(out.Data) > 1:
                            print(str(out.Data)[:10] + '...]...]')
                        else:
                            print(str(out.Data)[:10] + '...]]')
                    else:
                        print(out.Data)
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call wsd')
                return out

        def __str__(self):
            return ("WSD")

    wsd = __wsd()

    class __wst:
        def __init__(self):
            self.restype = POINTER(c_apiout)
            self.argtypes = [c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p]
            self.lastCall = 0

        @retry
        def __call__(self, codes, fields, beginTime=None, endTime=None, options=None, usedf=False, *arga, **argb):
            # write_log('call wst')
            s = int(t.time() * 1000)
            if expolib is None:
                return WindQnt.format_wind_data(-103, '')

            if t.time() - self.lastCall < interval:
                t.sleep(interval)

            if (endTime is None):  endTime = datetime.today().strftime("%Y-%m-%d")
            if (beginTime is None):  beginTime = endTime

            codes = WindQnt._WindQnt__stringify(codes)
            fields = WindQnt._WindQnt__stringify(fields)
            options = WindQnt._WindQnt__parseoptions(options, arga, argb)

            if codes is None or fields is None or options is None:
                print("Insufficient arguments!")
                return

            userID = str(getJsonTag(authString, 'accountID'))
            if userID == '':
                userID = "1214779"
            tmp = "wst|" + codes + "|" + fields + "|" + beginTime + "|" + endTime + "|" + options + "|" + userID
            tmp = tmp.encode("utf16") + b"\x00\x00"

            apiOut = expolib.SendMsg2Expo(tmp, len(tmp))
            self.lastCall = t.time()

            if apiOut.contents.ErrorCode == -1 or apiOut.contents.ErrorCode == -40521010:
                msg = 'Request Timeout'
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call wst')
                return WindQnt.format_wind_data(-40521010, msg)
            else:
                out = WindData()
                out.set(apiOut, 1, asdate=False)

                if usedf:
                    if not isinstance(usedf, bool):
                        print('the sixth parameter is usedf which should be the Boolean type!')
                        return
                    try:
                        if out.ErrorCode != 0:
                            df = pd.DataFrame(out.Data, index=out.Fields)
                            df.columns = [x for x in range(df.columns.size)]
                            return out.ErrorCode, df.T.infer_objects()
                        col = out.Times
                        if len(out.Codes) == len(out.Fields) == 1:
                            idx = out.Fields
                        elif len(out.Codes) > 1 and len(out.Fields) == 1:
                            idx = out.Codes
                        elif len(out.Codes) == 1 and len(out.Fields) > 1:
                            idx = out.Fields
                        else:
                            idx = None
                        df = pd.DataFrame(out.Data, columns=col)
                        if idx:
                            df.index = idx

                        e = int(t.time() * 1000)
                        write_log(str(e - s) + ' call wst')
                        return out.ErrorCode, df.T.infer_objects()
                    except Exception:
                        print(traceback.format_exc())
                        return
                if out.ErrorCode != 0:
                    if len(out.Data) != 0 and len(out.Data[0]) > 100:
                        if len(out.Data) > 1:
                            print(str(out.Data)[:10] + '...]...]')
                        else:
                            print(str(out.Data)[:10] + '...]]')
                    else:
                        print(out.Data)
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call wst')
                return out

        def __str__(self):
            return ("WST")

    wst = __wst()

    class __wss:
        def __init__(self):
            self.restype = POINTER(c_apiout)
            self.argtypes = [c_wchar_p, c_wchar_p, c_wchar_p]  # codes,fields,options
            self.lastCall = 0

        @retry
        def __call__(self, codes, fields, options=None, usedf=None, *arga, **argb):
            # write_log('call wss')
            s = int(t.time() * 1000)

            if expolib is None:
                return WindQnt.format_wind_data(-103, '')

            if t.time() - self.lastCall < interval:
                t.sleep(interval)

            codes = WindQnt._WindQnt__stringify(codes)
            fields = WindQnt._WindQnt__stringify(fields)
            options = WindQnt._WindQnt__parseoptions(options, arga, argb)

            if fields is None or options is None:
                print("Insufficient arguments!")
                return

            userID = str(getJsonTag(authString, 'accountID'))
            if userID == '':
                userID = "1214779"
            tmp = "wss|" + codes + "|" + fields + "|" + options + "|" + userID
            tmp = tmp.encode("utf16") + b"\x00\x00"

            apiOut = expolib.SendMsg2Expo(tmp, len(tmp))

            self.lastCall = t.time()
            if apiOut.contents.ErrorCode == -1 or apiOut.contents.ErrorCode == -40521010:
                msg = 'Request Timeout'
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call wss')
                return WindQnt.format_wind_data(-40521010, msg)
            else:
                out = WindData()
                out.set(apiOut, 1, asdate=True)

                # 将winddata类型数据改为dataframe格式
                if usedf:
                    if not isinstance(usedf, bool):
                        print('the fourth parameter is usedf which should be the Boolean type!')
                        return
                    try:
                        if out.ErrorCode != 0:
                            df = pd.DataFrame(out.Data, index=out.Fields)
                            df.columns = [x for x in range(df.columns.size)]
                            return out.ErrorCode, df.T.infer_objects()
                        if out.Codes == 1 or out.Fields == 1:
                            return out.ErrorCode, WindQnt.to_dataframe(out)
                        else:
                            df = pd.DataFrame(out.Data, columns=out.Codes, index=out.Fields)
                            e = int(t.time() * 1000)
                            write_log(str(e - s) + ' call wss')
                            return out.ErrorCode, df.T.infer_objects()
                    except Exception as e:
                        print(traceback.format_exc())
                        return
                if out.ErrorCode != 0:
                    if len(out.Data) != 0 and len(out.Data[0]) > 100:
                        if len(out.Data) > 1:
                            print(str(out.Data)[:10] + '...]...]')
                        else:
                            print(str(out.Data)[:10] + '...]]')
                    else:
                        print(out.Data)
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call wss')
                return out

        def __str__(self):
            return ("wss")

    wss = __wss()

    class __wset:
        def __init__(self):
            self.restype = POINTER(c_apiout)
            self.argtypes = [c_wchar_p, c_wchar_p]  # tablename,options
            self.lastCall = 0

        @retry
        def __call__(self, tablename, options=None, usedf=None, *arga, **argb):
            # write_log('call wset')
            s = int(t.time() * 1000)
            if expolib is None:
                return WindQnt.format_wind_data(-103, '')

            if t.time() - self.lastCall < interval:
                t.sleep(interval)

            tablename = WindQnt._WindQnt__stringify(tablename)
            options = WindQnt._WindQnt__parseoptions(options, arga, argb)

            if tablename is None or options is None:
                msg = "Insufficient arguments!"
                return WindQnt.format_wind_data(-40520001, msg)

            userID = str(getJsonTag(authString, 'accountID'))
            if userID == '':
                userID = "1214779"
            tmp = "wset|" + tablename + "|" + options + "|" + userID
            tmp = tmp.encode("utf16") + b"\x00\x00"

            apiOut = expolib.SendMsg2Expo(tmp, len(tmp))

            self.lastCall = t.time()
            if apiOut.contents.ErrorCode == -1 or apiOut.contents.ErrorCode == -40521010:
                msg = 'Request Timeout'
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call wset')
                return WindQnt.format_wind_data(-40521010, msg)
            else:
                out = WindData()
                out.set(apiOut, 1, asdate=True)

                # 将winddata类型数据改为dataframe格式
                if usedf:
                    if not isinstance(usedf, bool):
                        print('the third parameter is usedf which should be the Boolean type!')
                        return
                    try:
                        if out.ErrorCode != 0:
                            df = pd.DataFrame(out.Data, index=out.Fields)
                            df.columns = [x for x in range(df.columns.size)]
                            return out.ErrorCode, df.T.infer_objects()
                        if out.Codes == 1 or out.Fields == 1:
                            return out.ErrorCode, WindQnt.to_dataframe(out)
                        else:
                            df = pd.DataFrame(out.Data, columns=out.Codes, index=out.Fields)
                            e = int(t.time() * 1000)
                            write_log(str(e - s) + ' call wset')
                            return out.ErrorCode, df.T.infer_objects()
                    except Exception as e:
                        print(traceback.format_exc())
                        return
                if out.ErrorCode != 0:
                    if len(out.Data) != 0 and len(out.Data[0]) > 100:
                        if len(out.Data) > 1:
                            print(str(out.Data)[:10] + '...]...]')
                        else:
                            print(str(out.Data)[:10] + '...]]')
                    else:
                        print(out.Data)
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call wset')
                return out

        def __str__(self):
            return ("wset")

    wset = __wset()

    class __edb:
        def __init__(self):
            self.restype = POINTER(c_apiout)
            self.argtypes = [c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p]  # code,begintime,endtime,option
            self.lastCall = 0

        def __call__(self, codes, beginTime=None, endTime=None, options=None, usedf=None, *arga, **argb):
            s = int(t.time() * 1000)
            if expolib is None:
                return WindQnt.format_wind_data(-103, '')

            if t.time() - self.lastCall < interval:
                t.sleep(interval)

            endTime = WindQnt._WindQnt__parsedate(endTime)
            if endTime is None:
                print("Invalid date format of endTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01")
                return

            beginTime = WindQnt._WindQnt__parsedate(beginTime)
            if beginTime is None:
                print("Invalid date format of beginTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01")
                return

            # chech if the endTime is before than the beginTime
            endD = datetime.strptime(endTime, "%Y-%m-%d")
            beginD = datetime.strptime(beginTime, "%Y-%m-%d")
            if (endD - beginD).days < 0:
                print("The endTime should be later than or equal to the beginTime!")
                return

            codes = WindQnt._WindQnt__stringify(codes)
            options = WindQnt._WindQnt__parseoptions(options, arga, argb)
            if (codes is None or options is None):
                print('Invalid arguments!')
                return

            userID = str(getJsonTag(authString, 'accountID'))
            if userID == '':
                userID = "1214779"
            tmp = "edb|" + codes + "|" + beginTime + "|" + endTime + "|" + options + "|" + userID
            tmp = tmp.encode("utf16") + b"\x00\x00"

            apiOut = expolib.SendMsg2Expo(tmp, len(tmp))

            self.lastCall = t.time()
            if apiOut.contents.ErrorCode == -1 or apiOut.contents.ErrorCode == -40521010:
                out = WindData()
                out.ErrorCode = -40521010
                out.Codes = ['ErrorReport']
                out.Fields = ['OUT MESSAGE']
                out.Times = datetime.now().strftime('%Y%m%d %H:%M:%S')
                out.Data = [['Request Timeout']]
                return out
            else:
                out = WindData()
                out.set(apiOut, 1, asdate=True)
                
                # 将winddata类型数据改为dataframe格式
                if usedf:
                    if not isinstance(usedf, bool):
                        print('the third parameter is usedf which should be the Boolean type!')
                        return
                    try:
                        if out.ErrorCode != 0:
                            df = pd.DataFrame(out.Data, index=out.Fields)
                            df.columns = [x for x in range(df.columns.size)]
                            return out.ErrorCode, df.T.infer_objects()
                        if out.Codes == 1 or out.Fields == 1:
                            return out.ErrorCode, WindQnt.to_dataframe(out)
                        else:
                            df = pd.DataFrame(out.Data, columns=out.Codes, index=out.Fields)
                            e = int(t.time() * 1000)
                            write_log(str(e - s) + ' call edb')
                            return out.ErrorCode, df.T.infer_objects()
                    except Exception as e:
                        print(traceback.format_exc())
                        return

                if out.ErrorCode != 0:
                    if len(out.Data) != 0 and len(out.Data[0]) > 100:
                        if len(out.Data) > 1:
                            print(str(out.Data)[:10] + '...]...]')
                        else:
                            print(str(out.Data)[:10] + '...]]')
                    else:
                        print(out.Data)
                return out

        def __str__(self):
            return ("edb")

    edb = __edb()

    class __tdays:
        def __init__(self):
            self.restype = POINTER(c_apiout)
            self.argtypes = [c_wchar_p, c_wchar_p, c_wchar_p]  # begintime,endtime,options
            self.lastCall = 0

        def __call__(self, beginTime=None, endTime=None, options=None, *arga, **argb):
            # write_log('call tdays')
            s = int(t.time() * 1000)

            if expolib is None:
                return WindQnt.format_wind_data(-103, '')

            if t.time() - self.lastCall < interval:
                t.sleep(interval)

            endTime = WindQnt._WindQnt__parsedate(endTime)
            if endTime is None:
                print("Invalid date format of endTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01")
                return

            beginTime = WindQnt._WindQnt__parsedate(beginTime)
            if beginTime is None:
                print("Invalid date format of beginTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01")
                return

            # chech if the endTime is before than the beginTime
            endD = datetime.strptime(endTime, "%Y-%m-%d")
            beginD = datetime.strptime(beginTime, "%Y-%m-%d")
            if (endD - beginD).days < 0:
                print("The endTime should be later than or equal to the beginTime!")
                return

            options = WindQnt._WindQnt__parseoptions(options, arga, argb)
            if options is None:
                print("Invalid options")
                return

            userID = str(getJsonTag(authString, 'accountID'))
            if userID == '':
                userID = "1214779"
            tmp = "tdays|" + beginTime + "|" + endTime + "|" + options + "|" + userID
            tmp = tmp.encode("utf16") + b"\x00\x00"

            apiOut = expolib.SendMsg2Expo(tmp, len(tmp))

            self.lastCall = t.time()
            if apiOut.contents.ErrorCode == -1 or apiOut.contents.ErrorCode == -40521010:
                out = WindData()
                out.ErrorCode = -40521010
                out.Codes = ['ErrorReport']
                out.Fields = ['OUT MESSAGE']
                out.Times = datetime.now().strftime('%Y%m%d %H:%M:%S')
                out.Data = [['Request Timeout']]
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdays')
                return out
            else:
                out = WindData()
                out.set(apiOut, 1, asdate=True)

                if out.ErrorCode != 0:
                    if len(out.Data) != 0 and len(out.Data[0]) > 100:
                        if len(out.Data) > 1:
                            print(str(out.Data)[:10] + '...]...]')
                        else:
                            print(str(out.Data)[:10] + '...]]')
                    else:
                        print(out.Data)
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdays')
                return out

        def __str__(self):
            return ("tdays")

    tdays = __tdays()

    class __tdayscount:
        def __init__(self):
            self.restype = POINTER(c_apiout)
            self.argtypes = [c_wchar_p, c_wchar_p, c_wchar_p]  # begintime,endtime,options
            self.lastCall = 0

        def __call__(self, beginTime=None, endTime=None, options=None, *arga, **argb):
            # write_log('call tdayscount')
            s = int(t.time() * 1000)

            if expolib is None:
                return WindQnt.format_wind_data(-103, '')

            if t.time() - self.lastCall < interval:
                t.sleep(interval)

            endTime = WindQnt._WindQnt__parsedate(endTime)
            if endTime is None:
                print("Invalid date format of endTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01")
                return

            beginTime = WindQnt._WindQnt__parsedate(beginTime)
            if beginTime is None:
                print("Invalid date format of beginTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01")
                return

            # chech if the endTime is before than the beginTime
            endD = datetime.strptime(endTime, "%Y-%m-%d")
            beginD = datetime.strptime(beginTime, "%Y-%m-%d")
            if (endD - beginD).days < 0:
                print("The endTime should be later than or equal to the beginTime!")
                return

            options = WindQnt._WindQnt__parseoptions(options, arga, argb)
            if options is None:
                print("Invalid options")
                return

            userID = str(getJsonTag(authString, 'accountID'))
            if userID == '':
                userID = "1214779"
            tmp = "tdayscount|" + beginTime + "|" + endTime + "|" + options + "|" + userID
            tmp = tmp.encode("utf16") + b"\x00\x00"

            apiOut = expolib.SendMsg2Expo(tmp, len(tmp))

            self.lastCall = t.time()
            if apiOut.contents.ErrorCode == -1 or apiOut.contents.ErrorCode == -40521010:
                out = WindData()
                out.ErrorCode = -40521010
                out.Codes = ['ErrorReport']
                out.Fields = ['OUT MESSAGE']
                out.Times = datetime.now().strftime('%Y%m%d %H:%M:%S')
                out.Data = [['Request Timeout']]
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdayscount')
                return out
            else:
                out = WindData()
                #               pdbb.set_trace()
                out.set(apiOut, 1, asdate=True)
                #               pdbb.set_trace()

                if out.ErrorCode != 0:
                    if len(out.Data) != 0 and len(out.Data[0]) > 100:
                        if len(out.Data) > 1:
                            print(str(out.Data)[:10] + '...]...]')
                        else:
                            print(str(out.Data)[:10] + '...]]')
                    else:
                        print(out.Data)
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdayscount')
                return out

        def __str__(self):
            return ("tdayscount")

    tdayscount = __tdayscount()

    class __tdaysoffset:
        def __init__(self):
            self.restype = POINTER(c_apiout)
            self.argtypes = [c_wchar_p, c_int32, c_wchar_p]  # begintime,offset,options
            self.lastCall = 0

        def __call__(self, offset, beginTime=None, options=None, *arga, **argb):
            # write_log('call tdaysoffset')
            s = int(t.time() * 1000)

            if expolib is None:
                return WindQnt.format_wind_data(-103, '')

            if t.time() - self.lastCall < interval:
                t.sleep(interval)

            offset = WindQnt._WindQnt__stringify(offset)
            options = WindQnt._WindQnt__parseoptions(options, arga, argb)
            beginTime = WindQnt._WindQnt__parsedate(beginTime)
            if beginTime is None:
                print("Invalid date format of beginTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01")
                return

            userID = str(getJsonTag(authString, 'accountID'))
            if userID == '':
                userID = "1214779"
            tmp = "tdaysoffset|" + beginTime + "|" + offset + "|" + options + "|" + userID
            tmp = tmp.encode("utf16") + b"\x00\x00"

            apiOut = expolib.SendMsg2Expo(tmp, len(tmp))

            self.lastCall = t.time()
            if apiOut.contents.ErrorCode == -1 or apiOut.contents.ErrorCode == -40521010:
                out = WindData()
                out.ErrorCode = -40521010
                out.Codes = ['ErrorReport']
                out.Fields = ['OUT MESSAGE']
                out.Times = datetime.now().strftime('%Y%m%d %H:%M:%S')
                out.Data = [['Request Timeout']]
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdaysoffset')
                return out
            else:
                out = WindData()
                #               pdbb.set_trace()
                out.set(apiOut, 1, asdate=True)
                #               pdbb.set_trace()

                if out.ErrorCode != 0:
                    if len(out.Data) != 0 and len(out.Data[0]) > 100:
                        if len(out.Data) > 1:
                            print(str(out.Data)[:10] + '...]...]')
                        else:
                            print(str(out.Data)[:10] + '...]]')
                    else:
                        print(out.Data)
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdaysoffset')
                return out

        def __str__(self):
            return ("tdaysoffset")

    tdaysoffset = __tdaysoffset()

    class __wsq:
        def __init__(self):
            self.restype = POINTER(c_apiout)
            self.argtypes = [c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p]
            self.lastCall = 0
            self.__class__.wsq_callback.callback_funcs = {}

        def __call__(self, codes, fields, options=None, func=None, *arga, **argb):
            # write_log('call wsq')
            s = int(t.time() * 1000)

            if expolib is None:
                return WindQnt.format_wind_data(-103, '')

            # if t.time() - self.lastCall < interval:
            # speed 的间隔时间最小的2秒，否则服务器会延迟响应或者拒绝
            if t.time() - self.lastCall < 2:
                t.sleep(interval)

            codes = WindQnt._WindQnt__stringify(codes)
            fields = WindQnt._WindQnt__stringify(fields)
            options = WindQnt._WindQnt__parseoptions(options, arga, argb)

            if codes is None or codes == '' or fields is None or options is None:
                print("Insufficient arguments!")
                return

            # covert "codesRaw" to the format supported by Speed
            fields = re.split(r'[;,\s]\s*', fields)
            fieldsname = ",".join(fields)
            nfields = len(fields)
            items = r.GetEleByTagAttrVal("item", "funname", fields)
            
            global g_wsq_callback
            global wsq_items
            if len(wsq_items) < 1:
                wsq_items = items
                
            fields = []
            for item in items:
                fields.append(int(item["id"]))

            if nfields != len(fields):
                out = WindData()
                out.ErrorCode = -40522007
                out.Codes = ['ErrorReport']
                out.Fields = ['OUTMESSAGE']
                out.Times = datetime.now().strftime('%Y%m%d %H:%M:%S')
                out.Data = [['Argument Error']]
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call wsq')
                return out

            codes = codes.upper()
            fields = WindQnt._WindQnt__stringify(fields)
            fields = fields.encode("utf8") + b"\x00"
            codes = WindQnt._WindQnt__stringify(codes)
            codes = codes.encode("utf8") + b"\x00"
            fieldsname = fieldsname.encode("utf8") + b"\x00"

            #SPDCBTYPE = CFUNCTYPE(None, c_int, POINTER((c_apiout)))
            #spdcb = SPDCBTYPE(w.wsq.wsq_callback)
            reqID = c_int()

            if not callable(func):
                apiOut = speedlib.SendMsg2SpeedAsyc(codes, len(codes), fields, len(fields), spdcb, 0, fieldsname,
                                                    byref(reqID))
            else:
                w.wsq.wsq_callback.items = items
                speedlib.SendMsg2SpeedAsyc(codes, len(codes), fields, len(fields), spdcb, 1, fieldsname, byref(reqID))
                #w.wsq.wsq_callback.callback_funcs[reqID.value] = func
                g_wsq_callback.callback_funcs[reqID.value] = func
                out = WindData()
                out.ErrorCode = 0
                out.Codes = ['']
                out.Fields = ['']
                out.RequestID = reqID.value
                out.Times = datetime.now().strftime('%Y%m%d %H:%M:%S')
                out.Data = [['']]

                self.lastCall = t.time()
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call wsq')
                return out

            self.lastCall = t.time()

            if apiOut.contents.ErrorCode == -1 or apiOut.contents.ErrorCode == -40520008:
                out = WindData()
                out.ErrorCode = -40520008
                out.Codes = ['ErrorReport']
                out.Fields = ['OUT MESSAGE']
                out.Times = datetime.now().strftime('%Y%m%d %H:%M:%S')
                out.Data = [['Request Timeout']]
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call wsq')
                return out
            else:
                out = WindData()
                out.set(apiOut, 3)

                id2rtField = {}
                for item in items:
                    id2rtField[item['id']] = item['funname'].upper()

                if out.Fields[0] != "error message":
                    out.Fields = [id2rtField[str(val)] for val in out.Fields]

                out.Times = datetime.now().strftime('%Y%m%d %H:%M:%S')

                if out.ErrorCode != 0:
                    if len(out.Data) != 0 and len(out.Data[0]) > 100:
                        if len(out.Data) > 1:
                            print(str(out.Data)[:10] + '...]...]')
                        else:
                            print(str(out.Data)[:10] + '...]]')
                    else:
                        print(out.Data)
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call wsq')
                return out

        @staticmethod
        def wsq_callback(reqID, indata):
            out = WindData()
            out.set(indata, 3)
            out.RequestID = reqID

            items = w.wsq.wsq_callback.items
            id2rtField = {}
            for item in items:
                id2rtField[item['id']] = item['funname'].upper()

            tmp = [id2rtField[str(val)] for val in out.Fields]
            out.Fields = tmp
            out.Times = datetime.now().strftime('%Y%m%d %H:%M:%S')

            try:
                w.wsq.wsq_callback.callback_funcs[reqID](out)
            except:
                print(out)

        def __str__(self):
            return ("WSQ")

    wsq = __wsq()

    class __tdi():
        def __init__(self):
            self.restype = POINTER(WBKTData)
            self.argtypes = [c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p]
            self.lastCall = 0

        def __call__(self, codes, fields, begin_time, end_time, options, *args, **kwargs):
            # write_log('call tdi')
            s = int(t.time() * 1000)

            if expolib is None:
                return WindQnt.format_wind_data(-103, '')

            if t.time() - self.lastCall < interval:
                t.sleep(interval)

            end_time = WindQnt._WindQnt__parsedate(end_time, True)
            if end_time is None:
                msg = "Invalid date format of endTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01"
                return WindQnt.format_wind_data(-40522011, msg)

            begin_time = WindQnt._WindQnt__parsedate(begin_time, True)
            if begin_time is None:
                msg = "Invalid date format of beginTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01"
                return WindQnt.format_wind_data(-40522011, msg)

            # check if the endTime is before than the beginTime
            end_p = datetime.strptime(end_time.split(' ')[0], "%Y-%m-%d")
            begin_p = datetime.strptime(begin_time.split(' ')[0], "%Y-%m-%d")
            if (end_p - begin_p).days < 0:
                msg = "The endTime should be later than or equal to the beginTime!"
                return WindQnt.format_wind_data(-40522010, msg)
            if fields.find('time') == -1:
                fields += ',time'
            if fields.find('code') == -1:
                fields += ',code'
            if fields.find('date') == -1:
                fields += ',date'

            codes = WindQnt._WindQnt__stringify(codes)
            fields = WindQnt._WindQnt__stringify(fields)
            options = WindQnt.format_option(WindQnt._WindQnt__parseoptions(options, args, kwargs))

            if codes is None or fields is None or options is None:
                msg = "Insufficient arguments!"
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdi')
                return WindQnt.format_wind_data(-40520001, msg)
            codes = codes.encode("utf8") + b"\x00"
            fields = fields.encode("utf8") + b"\x00"
            options = options.encode('utf8') + b"\x00"
            end_time = end_time.encode('utf8') + b"\x00"
            begin_time = begin_time.encode('utf8') + b"\x00"

            bkt_data = WBKTData()
            errmsg = c_char_p()
            res = TDB_lib.apiTDBGetKLine(byref(bkt_data), fields, codes, begin_time, end_time, options, byref(errmsg))

            self.lastCall = t.time()
            out = WindData()
            if res != 0:
                if errmsg.value is not None:
                    msg = errmsg.value.decode('utf8')
                else:
                    msg = ''
                TDB_lib.apiTDBFreeData(byref(bkt_data))
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdi')
                return WindQnt.format_wind_data(-40521005, msg)
            else:
                out.set_bkt_data(bkt_data, asdate=True)
                TDB_lib.apiTDBFreeData(byref(bkt_data))
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdi')
                return out

        def __str__(self):
            return "TDI"

    tdi = __tdi()

    class __tdtx():
        def __init__(self):
            self.restype = POINTER(WBKTData)
            self.argtypes = [c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p]
            self.lastCall = 0

        def __call__(self, codes, fields, begin_time, end_time, options, *args, **kwargs):
            # write_log('call tdtx')
            s = int(t.time() * 1000)

            if expolib is None:
                return WindQnt.format_wind_data(-103, '')

            if t.time() - self.lastCall < interval:
                t.sleep(interval)

            end_time = WindQnt._WindQnt__parsedate(end_time, True)
            if end_time is None:
                msg = "Invalid date format of endTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01"
                return WindQnt.format_wind_data(-40522011, msg)

            begin_time = WindQnt._WindQnt__parsedate(begin_time, True)
            if begin_time is None:
                msg = "Invalid date format of beginTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01"
                return WindQnt.format_wind_data(-40522011, msg)

            # check if the endTime is before than the beginTime
            end_p = datetime.strptime(end_time.split(' ')[0], "%Y-%m-%d")
            begin_p = datetime.strptime(begin_time.split(' ')[0], "%Y-%m-%d")
            if (end_p - begin_p).days < 0:
                msg = "The endTime should be later than or equal to the beginTime!"
                return WindQnt.format_wind_data(-40522010, msg)
            if fields.find('time') == -1:
                fields += ',time'
            if fields.find('code') == -1:
                fields += ',code'
            if fields.find('date') == -1:
                fields += ',date'

            codes = WindQnt._WindQnt__stringify(codes)
            fields = WindQnt._WindQnt__stringify(fields)
            options = WindQnt.format_option(WindQnt._WindQnt__parseoptions(options, args, kwargs))
            if codes is None or fields is None or options is None:
                msg = "Insufficient arguments!"
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdtx')
                return WindQnt.format_wind_data(-40520001, msg)
            codes = codes.encode("utf8") + b"\x00"
            fields = fields.encode("utf8") + b"\x00"
            options = options.encode('utf8') + b"\x00"
            end_time = end_time.encode('utf8') + b"\x00"
            begin_time = begin_time.encode('utf8') + b"\x00"

            bkt_data = WBKTData()
            errmsg = c_char_p()
            res = TDB_lib.apiTDBGetTransaction(byref(bkt_data), fields, codes, begin_time, end_time, byref(errmsg))

            out = WindData()
            if res != 0:
                if errmsg.value is not None:
                    msg = errmsg.value.decode('utf8')
                else:
                    msg = ''
                TDB_lib.apiTDBFreeData(byref(bkt_data))
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdtx')
                return WindQnt.format_wind_data(-40521005, msg)
            else:
                out.set_bkt_data(bkt_data, asdate=True)
                TDB_lib.apiTDBFreeData(byref(bkt_data))
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdtx')
                return out

        def __str__(self):
            return "TDTX"

    tdtx = __tdtx()

    class __tdt():
        def __init__(self):
            self.restype = POINTER(WBKTData)
            self.argtypes = [c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p]
            self.lastCall = 0

        def __call__(self, codes, fields, begin_time, end_time, options, *args, **kwargs):
            # write_log('call tdt')
            s = int(t.time() * 1000)

            if expolib is None:
                return WindQnt.format_wind_data(-103, '')

            if t.time() - self.lastCall < interval:
                t.sleep(interval)

            end_time = WindQnt._WindQnt__parsedate(end_time, True)
            if end_time is None:
                msg = "Invalid date format of endTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01"
                return WindQnt.format_wind_data(-40522011, msg)

            begin_time = WindQnt._WindQnt__parsedate(begin_time, True)
            if begin_time is None:
                msg = "Invalid date format of beginTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01"
                return WindQnt.format_wind_data(-40522011, msg)

            # chech if the endTime is before than the beginTime
            end_p = datetime.strptime(end_time.split(' ')[0], "%Y-%m-%d")
            begin_p = datetime.strptime(begin_time.split(' ')[0], "%Y-%m-%d")
            if (end_p - begin_p).days < 0:
                msg = "The endTime should be later than or equal to the beginTime!"
                return WindQnt.format_wind_data(-40522010, msg)
            if fields.find('time') == -1:
                fields += ',time'
            if fields.find('code') == -1:
                fields += ',code'
            if fields.find('date') == -1:
                fields += ',date'

            codes = WindQnt._WindQnt__stringify(codes)
            fields = WindQnt._WindQnt__stringify(fields)
            options = WindQnt.format_option(WindQnt._WindQnt__parseoptions(options, args, kwargs))
            if codes is None or fields is None or options is None:
                msg = "Insufficient arguments!"
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdt')
                return WindQnt.format_wind_data(-40520001, msg)
            codes = codes.encode("utf8") + b"\x00"
            fields = fields.encode("utf8") + b"\x00"
            options = options.encode('utf8') + b"\x00"
            end_time = end_time.encode('utf8') + b"\x00"
            begin_time = begin_time.encode('utf8') + b"\x00"

            bkt_data = WBKTData()
            errmsg = c_char_p()
            res = TDB_lib.apiTDBGetTick(byref(bkt_data), fields, codes, begin_time, end_time, options, byref(errmsg))

            out = WindData()
            if res != 0:
                if errmsg.value is not None:
                    msg = errmsg.value.decode('utf8')
                else:
                    msg = ''
                TDB_lib.apiTDBFreeData(byref(bkt_data))
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdt')
                return WindQnt.format_wind_data(-40521005, msg)
            else:
                out.set_bkt_data(bkt_data, asdate=True)
                TDB_lib.apiTDBFreeData(byref(bkt_data))
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdt')
                return out

        def __str__(self):
            return "TDT"

    tdt = __tdt()

    class __tdo():
        def __init__(self):
            self.restype = POINTER(WBKTData)
            self.argtypes = [c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p]
            self.lastCall = 0

        def __call__(self, codes, fields, begin_time, end_time, options, *args, **kwargs):
            # write_log('call tdo')
            s = int(t.time() * 1000)

            if expolib is None:
                return WindQnt.format_wind_data(-103, '')

            if t.time() - self.lastCall < interval:
                t.sleep(interval)

            end_time = WindQnt._WindQnt__parsedate(end_time, True)
            if end_time is None:
                msg = "Invalid date format of endTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01"
                return WindQnt.format_wind_data(-40522011, msg)

            begin_time = WindQnt._WindQnt__parsedate(begin_time, True)
            if begin_time is None:
                msg = "Invalid date format of beginTime! Please use the '%Y-%m-%d' format! E.g. 2016-01-01"
                return WindQnt.format_wind_data(-40522011, msg)

            # chech if the endTime is before than the beginTime
            end_p = datetime.strptime(end_time.split(' ')[0], "%Y-%m-%d")
            begin_p = datetime.strptime(begin_time.split(' ')[0], "%Y-%m-%d")
            if (end_p - begin_p).days < 0:
                msg = "The endTime should be later than or equal to the beginTime!"
                return WindQnt.format_wind_data(-40522010, msg)
            if fields.find('time') == -1:
                fields += ',time'
            if fields.find('code') == -1:
                fields += ',code'
            if fields.find('date') == -1:
                fields += ',date'

            codes = WindQnt._WindQnt__stringify(codes)
            fields = WindQnt._WindQnt__stringify(fields)
            options = WindQnt.format_option(WindQnt._WindQnt__parseoptions(options, args, kwargs))
            if codes is None or fields is None or options is None:
                msg = "Insufficient arguments!"
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdo')
                return WindQnt.format_wind_data(-40520001, msg)
            codes = codes.encode("utf8") + b"\x00"
            fields = fields.encode("utf8") + b"\x00"
            options = options.encode('utf8') + b"\x00"
            end_time = end_time.encode('utf8') + b"\x00"
            begin_time = begin_time.encode('utf8') + b"\x00"

            bkt_data = WBKTData()
            errmsg = c_char_p()
            res = TDB_lib.apiTDBGetOrder(byref(bkt_data), fields, codes, begin_time, end_time, byref(errmsg))

            out = WindData()
            if res != 0:
                if errmsg.value is not None:
                    msg = errmsg.value.decode('utf8')
                else:
                    msg = ''
                TDB_lib.apiTDBFreeData(byref(bkt_data))
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdo')
                return WindQnt.format_wind_data(-40521005, msg)
            else:
                out.set_bkt_data(bkt_data, asdate=True)
                TDB_lib.apiTDBFreeData(byref(bkt_data))
                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdo')
                return out

        def __str__(self):
            return "TDO"

    tdo = __tdo()

    class tdf():
        def __init__(self):
            self.restype = POINTER(WBKTData)
            self.argtypes = [c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p]
            self.lastCall = 0
            self.__class__.tdf_callback.callback_funcs = {}

        def __call__(self, codes=None, fields=None, options=None, func=None, *args, **kwargs):
            # write_log('call tdf')
            s = int(t.time() * 1000)

            if speedlib is None:
                return WindQnt.format_wind_data(-103, '')

            if codes is None and fields is None and options is None:
                msg = "Insufficient arguments!"
                return WindQnt.format_wind_data(-40520001, msg)

            if codes is None and fields is None and options.find('reqID') != -1:
                bkt_data = WBKTData()
                errmsg = c_char_p()
                rid = ''
                option_list = options.split(';')
                for option in option_list:
                    if option.find('reqID') != -1:
                        rid = int(option.split('=')[1])
                        break
                options = WindQnt._WindQnt__parseoptions(options, args, kwargs)
                options = options.encode('utf8') + b"\x00"
                res = speedlib.apiTDFGetLast(byref(bkt_data), rid, options, byref(errmsg))
                if res != 0:
                    msg = errmsg.value.decode('utf8')
                    speedlib.apiTDFFreeData(byref(bkt_data))
                    e = int(t.time() * 1000)
                    write_log(str(e - s) + ' call tdf')
                    return WindQnt.format_wind_data(-40521005, msg)
                else:
                    out = WindData()
                    out.set_bkt_data(bkt_data, asdate=True)
                    speedlib.apiTDFFreeData(byref(bkt_data))
                    e = int(t.time() * 1000)
                    write_log(str(e - s) + ' call tdf')
                    return out

            if fields.find('time') == -1:
                fields += ',time'
            if fields.find('code') == -1:
                fields += ',code'
            if fields.find('date') == -1:
                fields += ',date'

            codes = WindQnt._WindQnt__stringify(codes)
            fields = WindQnt._WindQnt__stringify(fields)
            options = WindQnt.format_option(WindQnt._WindQnt__parseoptions(options, args, kwargs))
            if codes is None or fields is None or options is None:
                msg = "Insufficient arguments!"
                return WindQnt.format_wind_data(-40520001, msg)
            if (options == '' or options.find('mode=2') != -1) and func is None:
                msg = 'func should be required'
                return WindQnt.format_wind_data(-9999, msg)
            if func is not None and not callable(func):
                msg = 'func should be a function'

                e = int(t.time() * 1000)
                write_log(str(e - s) + ' call tdf')
                return WindQnt.format_wind_data(-9999, msg)

            codes = codes.encode("utf8") + b"\x00"
            fields = fields.encode("utf8") + b"\x00"
            options = options.encode('utf8') + b"\x00"

            TDF_CALLBACK = CFUNCTYPE(None, c_int, POINTER(WBKTData))
            tdfcb = TDF_CALLBACK(w.tdf.tdf_callback)
            errmsg = c_char_p()
            rid = speedlib.apiTDFSubscribe(codes, fields, tdfcb, options, byref(errmsg))
            if rid > 0:
                w.tdf.tdf_callback.callback_funcs[rid] = func
            else:
                print('invalid request id')
                return None
            out = WindData()
            out.RequestID = rid
            e = int(t.time() * 1000)
            write_log(str(e - s) + ' call tdf')
            return out

        @staticmethod
        def tdf_callback(mix, indata):
            rid = mix % 100000000
            left_count = mix // 100000000

            out = WindData()
            out.set_bkt_data(indata[0], asdate=True)
            speedlib.apiTDFFreeData(indata)
            out.RequestID = rid
            try:
                w.tdf.tdf_callback.callback_funcs[rid](out, left_count)
            except:
                print('func error')
                return

    tdq = tdf()

    class __cancelRequest():
        def __init__(self):
            pass

        def __call__(self, reqID):
            if not isinstance(reqID, int):
                print("The resqID parament should be an integer")
                return

            if reqID >= REQUEST_ID_MIN_RESQUEST or reqID == 0:
                speedlib.CancelRequest(reqID)

    cancelRequest = __cancelRequest()

    class __getReqData():
        def __init__(self):
            pass

        def __call__(self, req_id, options=None, *args, **kwargs):
            if not isinstance(req_id, int):
                return

            options = WindQnt.format_option(WindQnt._WindQnt__parseoptions(options, args, kwargs))
            if options is None:
                msg = "Insufficient arguments!"
                return WindQnt.format_wind_data(-40520001, msg)
            options = options.encode('utf8') + b"\x00"

            errmsg = c_char_p()
            bkt_data = WBKTData()
            speedlib.apiTDFGetLast(byref(bkt_data), req_id, options, byref(errmsg))
            out = WindData()
            if bkt_data.data is None or len(bkt_data.data) == 0:
                out = WindQnt.format_wind_data(-9999, 'No data')
                return out
            out.set_bkt_data(bkt_data, asdate=True)
            speedlib.apiTDFFreeData(byref(bkt_data))
            return out

    getReqData = __getReqData()

    class __tlogon:
        def __init__(self):
            pass

        def __call__(self, BrokerID, DepartmentID, AccountID, Password, AccountType, Options=None, *args, **kwargs):
            BrokerID = BrokerID.encode("utf16") + b"\x00\x00"
            DepartmentID = DepartmentID.encode("utf16") + b"\x00\x00"
            AccountID = AccountID.encode("utf16") + b"\x00\x00"
            Password = Password.encode("utf16") + b"\x00\x00"
            AccountType = AccountType.encode("utf16") + b"\x00\x00"
            out_fields = c_char_p()
            if Options is None:
                Options = ''
            Options = Options.encode("utf16") + b"\x00\x00"

            if (
                    BrokerID is None or DepartmentID is None or AccountID is None or Password is None or AccountType is None):
                print('Invalid arguments!')
                return

            out = WindData()
            tradedata = c_lib.tLogon(BrokerID, DepartmentID, AccountID, Password, AccountType, Options,
                                     byref(out_fields))
            out_fields_value = out_fields.value.decode('utf8')
            out_fields_value = out_fields_value.split('&&')
            out_fields_value.pop()
            out.trade(tradedata, out_fields_value)
            return out

    tlogon = __tlogon()

    class __tlogout:
        def __init__(self):
            pass

        def __call__(self, options, *args, **kwargs):
            if (options is None):
                print('Invalid arguments!')
                return
            options = options.encode("utf16") + b"\x00\x00"
            out_fields = c_char_p()

            out = WindData()
            tradedata = c_lib.tLogout(options, byref(out_fields))
            out_fields_value = out_fields.value.decode('utf8')
            out_fields_value = out_fields_value.split('&&')
            out_fields_value.pop()
            out.trade(tradedata, out_fields_value)
            return out

    tlogout = __tlogout()

    class __torder:
        def __init__(self):
            pass

        def __call__(self, WindCode, TradeSide, OrderPrice, OrderVolume, options=None, *arga, **argb):

            out_fields = c_char_p()
            WindCode = WindCode.encode("utf16") + b"\x00\x00"
            TradeSide = TradeSide.encode("utf16") + b"\x00\x00"
            OrderPrice = OrderPrice.encode("utf16") + b"\x00\x00"
            OrderVolume = OrderVolume.encode("utf16") + b"\x00\x00"
            if options is None:
                options = ''
            options = options.encode("utf16") + b"\x00\x00"

            if (WindCode is None or TradeSide is None or OrderPrice is None or OrderVolume is None):
                print('Invalid arguments!')
                return

            out = WindData()
            tradedata = c_lib.tSendOrder(WindCode, TradeSide, OrderPrice, OrderVolume, options, byref(out_fields))
            out_fields_value = out_fields.value.decode('utf8')
            out_fields_value = out_fields_value.split('&&')
            out_fields_value.pop()
            out.trade(tradedata, out_fields_value)

            return out

    torder = __torder()

    class __tcancel:
        def __init__(self):
            pass

        def __call__(self, OrderNumber, options=None, *args, **kwargs):
            OrderNumber = OrderNumber.encode("utf16") + b"\x00\x00"
            out_fields = c_char_p()
            if options is None:
                options = ''
            options = options.encode("utf16") + b"\x00\x00"

            if (OrderNumber is None):
                print('Invalid arguments!')
                return

            out = WindData()
            tradedata = c_lib.tCancelOrder(OrderNumber, options, byref(out_fields))
            out_fields_value = out_fields.value.decode('utf8')
            out_fields_value = out_fields_value.split('&&')
            out_fields_value.pop()
            out.trade(tradedata, out_fields_value)

            return out

    tcancel = __tcancel()

    class __tquery:
        def __init__(self):
            pass

        def __call__(self, qrycode, options=None, *args, **kwargs):
            qrylist = ['capital', 'trade', 'position', 'order', 'logonid']
            if qrycode is None:
                print('Invalid arguments!')
                return
            qrycode = qrycode.lower()
            if qrycode not in qrylist:
                print('the querycode should be in the list', qrylist)
                return
            qrycode = qrycode.encode("utf16") + b"\x00\x00"
            out_fields = c_char_p()
            if options is None:
                options = ''
            options = options.encode("utf16") + b"\x00\x00"

            out = WindData()
            tradedata = c_lib.tQuery(qrycode, options, byref(out_fields))
            out_fields_value = out_fields.value.decode('utf8')
            out_fields_value = out_fields_value.split('&&')
            out_fields_value.pop()
            out.tquery(tradedata, out_fields_value)

            return out

    tquery = __tquery()


def write_log(msg):
    from datetime import datetime
    try:
        log_name = wind_log_path + userName + "-" + datetime.today().strftime("%Y%m%d")
        msg_prefix = datetime.today().strftime("%Y-%m-%d %H:%M:%S ")
        with open(log_name, "a+") as f:
            f.write(msg_prefix + 'info windpy ' + msg + ' ' + userName + "\n")
    except:
        return


w = WindQnt()