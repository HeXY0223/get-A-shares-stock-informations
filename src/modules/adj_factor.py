import tushare as ts
import pandas as pd
from dotenv import load_dotenv
import os
from utils.utils import *
from tqdm import tqdm
if __name__ == '__main__':
    load_dotenv()
    api_key = os.environ.get("API_KEY")
    ts.set_token(api_key)
    pro = ts.pro_api()
    engine = easyConnect()
    table_name = 'stock_daily_electronic_information'
    ts_codes = pd.read_sql('select ts_code from {table_name}'.format(table_name=
                                                           'stock_daily_electronic_information'), engine)
    ts_codes = ts_codes['ts_code'].unique()
    sql_query = f"""
        SELECT 
            ts_code, 
            MIN(trade_date) AS oldest_date, 
            MAX(trade_date) AS latest_date 
        FROM {table_name} 
        GROUP BY ts_code
    """
    stock_dates_df = pd.read_sql(sql_query, engine)
    print(stock_dates_df['oldest_date'])
    for i in tqdm(stock_dates_df.index):
        #print(stock_dates_df['oldest_date'][i])
        adjfactor_data = pro.adj_factor(ts_code=stock_dates_df['ts_code'][i],
                                        start_date=stock_dates_df['oldest_date'][i].strftime('%Y%m%d'),
                                        end_date=stock_dates_df['latest_date'][i].strftime('%Y%m%d'))
        #print(adjfactor_data)
        upsert_to_mysql(engine=engine, table_name=table_name, df_uncleaned=adjfactor_data)
