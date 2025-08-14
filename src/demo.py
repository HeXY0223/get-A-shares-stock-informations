# src/demo.py
import warnings
# 忽略所有 FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

from data_fetchers.stock_daily_fetcher import upsert_daily_markets
from utils.utils import *
from data_fetchers.index_basics_fetcher import upsert_index_basics
from data_fetchers.index_daily_fetcher import upsert_index_daily

if __name__ == '__main__':
    engine = easyConnect()
    # 下载申万行业分类（如果没有）
    #get_sw_category(engine=engine)
    # 下载股票基础信息（如果没有）
    #upsert_stock_basics(engine=engine)
    # 电子信息类股票日线 特别慢！！！！！
    # 如果想用就解开下面那两行注释
    query = 'select ts_code from sw_category where l1_name="电子" or l1_name="计算机" or l1_name="通信"'
    df = pd.read_sql(query, engine)
    ts_codes = df['ts_code'].tolist()
    logger.info(f"电子信息类:{ts_codes[:5]}等股票")
    upsert_daily_markets(engine=engine, ts_codes=ts_codes, table_name='stock_daily_electronic_information',
                         create_sql_command='USE DEFAULT stock_daily',
                         strategy='aggressive')
    # upsert_daily_markets(engine=engine, ts_codes=['000050.SZ','000062.SZ'], table_name='test_electronic',
    #                      create_sql_command='USE DEFAULT stock_daily')
    # 下载那两个指数的信息和日线
    upsert_index_basics(engine=engine, ts_codes=['000300.SH', '000905.SH'])
    upsert_index_daily(engine=engine,ts_codes=['000300.SH','000905.SH'],start_date='20200808',end_date='20250808',
                       table_name='index_daily_famous')
