import pymysql.err
import tushare as ts
import os
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from utils.utils import *
from tqdm import tqdm
import time

def get_index_daily(ts_code:str="", start_date:str='20200806', end_date:str='20250806', strategy:str='conservative', echo=False):
    load_dotenv()
    api_key = os.environ.get("API_KEY")
    ts.set_token(api_key)
    pro = ts.pro_api()
    try:
        index_data = pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if index_data.empty:
            if echo: print(f"无法访问代码为{ts_code}的股票，跳过。")
            return pd.DataFrame()
        return index_data
    except Exception as e:
        if echo:
            print(f"get_index_daily出现错误:{e}")
        return pd.DataFrame()


def upsert_index_daily(engine, ts_codes:list=[],
                         start_date:str='20200806', end_date:str='20250806',
                         strategy:str='conservative', table_name='index_daily',
                         create_sql_command:str='USE DEFAULT index_daily', echo:bool=False):
    # 如果数据太多，分配处理，每 step次访问一组，免得出问题
    step = 5
    for i in tqdm(range(0,len(ts_codes),step), desc="fetch & saving data"):
        df_uncleaned_list = []
        for each_index in ts_codes[i:i + step]:
            df_uncleaned_list.append(get_index_daily(each_index, start_date, end_date, strategy, echo))
        df_uncleaned = pd.concat(df_uncleaned_list, ignore_index=True)
        primary_key = ['ts_code', 'trade_date']  # 根据表结构，主键是 ts_code和 trade_date
        upsert_to_mysql(engine, table_name, df_uncleaned, primary_key, create_sql_command, echo)
        time.sleep(3)


if __name__ == '__main__':
    load_dotenv()
    password = os.environ.get("PASSWORD")
    database = os.environ.get("DATABASE")
    db_conn_str = \
        r'mysql+pymysql://root:{password}@{host}:{port}/{database}'.format(
            password=password,
            host='localhost',
            port=3306,
            database=database
        )
    engine = sqlalchemy.create_engine(db_conn_str)
    test_ts_codes = ['000300.SH','000905.SH']
    upsert_index_daily(engine=engine, ts_codes=test_ts_codes, start_date='20200806', end_date='20250806',
                       table_name='test_index_daily_1')