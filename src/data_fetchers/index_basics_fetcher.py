import pymysql.err
import tushare as ts
import os
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv

from utils.utils import *


def get_index_basics(ts_code:str=""):
    required_fields_basic = ['ts_code', 'name', 'market', 'publisher', 'category', 'base_date', 'base_point', 'exp_date', 'desc']
    load_dotenv()
    api_key = os.environ.get("API_KEY")
    ts.set_token(api_key)
    pro = ts.pro_api()
    stock_data = pro.index_basic(ts_code=ts_code, fields=",".join(required_fields_basic))
    stock_data.rename(columns={'desc':'description'}, inplace=True)
    return stock_data

def upsert_index_basics(engine, ts_codes:list=[], table_name='index_basic_info',
                        create_sql_command='USE DEFAULT index_basic_info', echo=False):
    for each_code in ts_codes:
        df_uncleaned = get_index_basics(ts_code=each_code)
        primary_key = ['ts_code']
        upsert_to_mysql(engine, table_name, df_uncleaned, primary_key, create_sql_command=create_sql_command, echo=echo)



if __name__ == '__main__':
    ts_codes = ['000300.SH','000905.SH']
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
    upsert_index_basics(engine=engine, ts_codes=ts_codes)