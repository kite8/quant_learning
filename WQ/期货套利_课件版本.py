def handle_data(bar_datetime, context, bar_data):         #定义策略函数
    trade_flag = context.trade_flag
    count_days = 60
    his_data_df = pd.DataFrame()
    for i in range(len(context.securities)):
        his_data = wa.history(context.securities[len(context.securities)-i-1], count_days+1)
        temp_data_df = pd.DataFrame(his_data.get_field('close'),index=his_data.get_field('time'),columns=[context.securities[len(context.securities)-i-1]])
        his_data_df = temp_data_df.join(his_data_df)
    
    his_data_df['indicator'] = ((his_data_df['Y.DCE']-his_data_df['P.DCE'])-(his_data_df['OI.CZC']-his_data_df['Y.DCE']))/(his_data_df['OI.CZC']-his_data_df['P.DCE'])
    ind_rec = his_data_df['indicator'][-20:].mean()
    ind_mid = his_data_df['indicator'][-30:].mean()
    ind_lon = his_data_df['indicator'][-40:].mean()
    ind_cur = float(his_data_df['indicator'][-1:])
    
    #交易区
    position = wa.query_position()
    if(ind_rec > ind_mid and ind_mid > ind_lon and ind_cur > 1.05*ind_rec):
        if(trade_flag!=1):
            if len(position)>0:
                res = wa.batch_order.sell_all(price='close', volume_check=False, no_quotation='error')
            res_l = wa.order_percent('P.DCE', 0.1, 'buy',volume_check=True)
            res_s = wa.order_percent('Y.DCE', 0.1, 'short',volume_check=True)
            trade_flag=1
    elif(ind_rec < ind_mid and ind_mid < ind_lon and ind_cur < 0.98*ind_rec):
        if(trade_flag!=-1):
            if len(position)>0:
                res = wa.batch_order.sell_all(price='close', volume_check=False, no_quotation='error')
            
            res_l = wa.order_percent('Y.DCE', 0.1, 'buy',volume_check=True)
            res_s = wa.order_percent('OI.CZC', 0.1, 'short',volume_check=True)
            trade_flag=-1
    elif(ind_cur < 1.02*ind_rec and ind_cur > 0.98*ind_rec):
        if len(position)>0:
            res = wa.batch_order.sell_all(price='close', volume_check=False, no_quotation='error')
        trade_flag=0
        
    context.trade_flag = trade_flag