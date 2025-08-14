import pymysql.err
import tushare as ts
import os
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from utils.utils import *



def get_stock_basics():
    """
    获取股票基本信息数据

    功能：
    - 从Tushare Pro API获取股票（上市、退市和暂停上市）的基本信息和公司主营业务信息
    - 返回包含所有股票信息的 DataFrame

    返回：
    - pd.DataFrame: 包含股票基本信息和主营业务信息的合并数据
    """
    # 要获取的基本信息字段列表
    required_fields_basic = ['ts_code', 'symbol', 'name', 'area', 'industry', 'market', 'list_status', 'list_date',
                              'delist_date']
    # 要获取的公司信息字段列表
    required_fields_info = ['ts_code', 'main_business']
    load_dotenv()
    api_key = os.environ.get("API_KEY")
    ts.set_token(api_key)
    pro = ts.pro_api()
    stock_basics = pd.concat([
        pro.stock_basic(fields=','.join(required_fields_basic), list_status='L'),  # 上市股票
        pro.stock_basic(fields=','.join(required_fields_basic), list_status='D'),  # 退市股票
        pro.stock_basic(fields=','.join(required_fields_basic), list_status='P')  # 暂停上市股票
    ], axis=0)  # 按行方向合并
    # 获取公司主营业务信息
    stock_company_info = pro.stock_company(fields=','.join(required_fields_info))
    # 将股票基本信息与公司信息按股票代码(ts_code)左连接合并
    stock_data = pd.merge(
        stock_basics,  # 左表：股票基本信息
        stock_company_info,  # 右表：公司主营业务信息
        how='left',  # 使用左连接方式
        on='ts_code'  # 连接键：股票代码
    )
    return stock_data

def upsert_stock_basics(engine, table_name='stock_basic_info',
                        create_sql_command='USE DEFAULT stock_basic_info'):
    """
    将 get_stock_basics 中获取到的 Pandas DataFrame的数据插入或更新到 MySQL 表中 (兼容 SQLAlchemy 2.x)。

    此版本增加了对 np.nan 值的处理，将其自动转换成 None，以防止 'nan can not be used with MySQL' 错误。

    :param engine: sqlalchemy.engine.Engine - SQLAlchemy的数据库连接引擎。
    :param table_name: str - 目标数据库表的名称。 默认值为 stock_basic_info.

    可以在 tests/get_stock_basics_test.ipynb 中查看详细运行情况。
    """
    df_uncleaned = get_stock_basics()
    primary_key = ['ts_code']  # 根据表结构，主键是 ts_code
    upsert_to_mysql(engine, table_name, df_uncleaned, primary_key, create_sql_command)

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
    upsert_stock_basics(engine, table_name='test_stock_basic_info', create_sql_command='USE DEFAULT stock_basic_info')


