# Signature:   w.wsd(*args, **kargs)
# Type:        __wsd
# String form: WSD
# File:        /opt/conda/lib/python3.6/WindPy.py
# Source:     
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