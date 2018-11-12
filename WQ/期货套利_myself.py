def handle_data(bar_datetime, context, bar_data):
    trade_flag = context.trade_flag
    count_days = 60
    hist_data_df = pd.DataFrame()
    nums = len(context.securities)
    for i in range(nums):
        hist_data = wa.history(context.securities[nums-i-1], count_days+1)
        temp_data_df = pd.DataFrame(hist_data.get_field('close'), index = hist_data.get_field('time'),
                                   columns=[context.securities[nums-i-1]])
        hist_data_df = temp_data_df.join(hist_data_df)
#         hist_data_df = pd.concat([hist_data_df, temp_data_df])
    
    Y_DCE = hist_data_df['Y.DCE'] # 豆油
    P_DCE = hist_data_df['P.DCE'] # 棕榈油
    OI_CZC = hist_data_df['OI.CZC'] # 菜籽油
    
    hist_data_df['indicator'] = ((Y_DCE - P_DCE) - (OI_CZC - Y_DCE)) / (OI_CZC - P_DCE)
    ind_rec = hist_data_df['indicator'][-20:].mean()
    ind_mid = hist_data_df['indicator'][-30:].mean()
    ind_lon = hist_data_df['indicator'][-40:].mean()
    ind_cur = float(hist_data_df['indicator'][-1:])
    
    # trading
    position = wa.query_position()
    # if 棕榈油和菜籽油差值越近的时间点越小， 难道是做多OI.CZC, 做空P.DCE?
    # 貌似不是，假定分母不变，考虑分子，近期的值越来越大，则 Y_DCE - P_DCE 变大的幅度超过 OI_CZC - Y_DCE
    # 变大的幅度，于是做空Y.DCE, 做多P.DCE
    if (ind_rec > ind_mid) and (ind_mid > ind_lon) and (ind_cur > 1.05 * ind_rec): # 这里是否改成1.02?
        if (trade_flag != 1):
            if len(position) > 0:
                res = wa.batch_order.sell_all(price='close', volume_check=False, no_quotation='error')
            res_1 = wa.order_percent('P.DCE', 0.1, 'buy', volume_check=True)
            res_2 = wa.order_percent('Y.DCE', 0.1, 'short', volume_check=True)
            trade_flag = 1
    
    elif (ind_rec < ind_mid) and (ind_mid < ind_lon) and (ind_cur < 0.98 * ind_rec):
        if (trade_flag != -1):
            if len(position) > 0:
                res = wa.batch_order.sell_all(price='close', volume_check=False, no_quotation='error')
            res_1 = wa.order_percent('Y.DCE', 0.1, 'buy', volume_check=True)
            res_2 = wa.order_percent('OI.CZC', 0.1, 'short', volume_check=True)
            trade_flag = -1
    
    elif (ind_cur < 1.05 * ind_rec) and (ind_cur > 0.98 * ind_rec):
        if len(position) > 0:
            res = wa.batch_order.sell_all(price='close', volume_check=False, no_quotation='error')
        trade_flag = 0
        
    context.trade_flag = trade_flag