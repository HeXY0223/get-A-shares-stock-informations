import tushare as ts
from data_fetchers.base import FetcherBase
from tqdm import tqdm
import pandas as pd
from utils.utils import easyConnect, upsert_to_mysql

basic_info_query = "select ts_code from stock_basic_info where list_status='L'"
engine = easyConnect()
ts_codes = pd.read_sql(basic_info_query, con=engine)['ts_code'].tolist()
length = len(ts_codes)
for i in tqdm(range(length)):
    test = FetcherBase(
        start_date='20250101',
        end_date='20250801',
        ts_codes= [ts_codes[i]],
        queries=[{'api':'daily_basic', 'fields':'turnover_rate,turnover_rate_f,total_mv,pe_ttm,pb,ps_ttm,dv_ttm'},
                 {'api': 'margin_detail', 'fields':'rzye'},
                 {'api': 'pro_bar', 'fields': 'open,high,low,close,vol,amount,pct_chg', 'adj': 'qfq'},

                 #{'api':'stock_basic','fields':'name,symbol'},
                 #{'api':'stock_company','fields':'reg_capital,province'},

                 {'api':'income', 'fields':'end_date,revenue,n_income,oper_cost,operate_profit'},
                 {'api':'cashflow', 'fields':'end_date,c_pay_acq_const_fiolta,n_cashflow_act'},
                 {'api':'balancesheet', 'fields':'end_date,total_assets,total_liab,total_hldr_eqy_inc_min_int,accounts_receiv,'
                                                 'money_cap,st_borr,non_cur_liab_due_1y,lt_borr,bond_payable'},
                 {'api':'stk_holdernumber', 'fields':'end_date,holder_num'},
                 {'api':'fina_indicator', 'fields':'end_date,ebitda'}
                 ]
    )
    k = test.fetch()
    upsert_to_mysql(engine, table_name='test', df_uncleaned=k,
                    primary_key=['ts_code','trade_date','data_name'],
                    create_sql_command='USE DEFAULT narrow_data')

engine.dispose()