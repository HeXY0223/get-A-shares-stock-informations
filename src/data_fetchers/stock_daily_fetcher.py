import pymysql.err
import tushare as ts
import os
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from utils.utils import *
from tqdm import tqdm
import time

def get_stock_daily(ts_code:list=[], start_date:str='20200806', end_date:str='20250806', strategy:str='conservative', echo=False):
    load_dotenv()
    api_key = os.environ.get("API_KEY")
    ts.set_token(api_key)
    pro = ts.pro_api()

    merged_list = []
    for each_code in ts_code:
        nan_count = 0
        merged = pd.DataFrame()
        cannot_access = False
        while nan_count < 3:
            cq_data = ts.pro_bar(ts_code=each_code, start_date=start_date, end_date=end_date, adj=None)  # 不复权
            qfq_data = ts.pro_bar(ts_code=each_code, start_date=start_date, end_date=end_date, adj='qfq')  # 前复权
            hfq_data = ts.pro_bar(ts_code=each_code, start_date=start_date, end_date=end_date, adj='hfq')  # 后复权
            adjfactor_data = pro.adj_factor(ts_code=each_code, start_date=start_date, end_date=end_date)
            # 对于无法访问的股票（如IPO终止、退市过早等等），直接抛弃掉
            if cq_data.empty or qfq_data.empty or hfq_data.empty or adjfactor_data.empty:
                # 先看看这股票叫啥
                name = pro.index_member_all(ts_code=each_code)['name'][0]
                if echo: print(f"无法访问到{name}(股票代码:{each_code})。这可能是因为该股票IPO终止，或者退市过早。")
                cannot_access = True
                break
            # 重命名除权&复权字段
            cq_data = cq_data.rename(columns={'vol':'volume'})
            qfq_data = qfq_data.rename(columns={'open': 'open_qfq', 'high': 'high_qfq', 'low': 'low_qfq', 'close': 'close_qfq', 'pre_close': 'pre_close_qfq', 'change': 'price_change_qfq'})
            hfq_data = hfq_data.rename(columns={'open': 'open_hfq', 'high': 'high_hfq', 'low': 'low_hfq', 'close': 'close_hfq', 'pre_close': 'pre_close_hfq', 'change': 'price_change_hfq'})

            merged = pd.merge(cq_data[['ts_code','trade_date','open','high','low','close','volume','amount']], qfq_data[['ts_code', 'trade_date', 'open_qfq', 'high_qfq', 'low_qfq', 'close_qfq', 'pre_close_qfq', 'price_change_qfq']], on=['ts_code', 'trade_date'], how='left')
            merged = pd.merge(merged, hfq_data[['ts_code', 'trade_date', 'open_hfq', 'high_hfq', 'low_hfq', 'close_hfq', 'pre_close_hfq', 'price_change_hfq']], on=['ts_code', 'trade_date'], how='left')
            merged = pd.merge(merged, adjfactor_data, on=['ts_code', 'trade_date'], how='left')
            if merged.isnull().any().any() and merged.isnull().sum().sum() != 4:  # 如果有空值 不等于4的原因是：如果刚上市的股票会在上市第一天缺少 pre_close和 price_change的前复权和后复权数据，一共四个。
                nan_count += 1
                merged = pd.DataFrame()  # 清空merged
            else:
                break
        if cannot_access:
            continue
        if nan_count == 3:
            print(f"已经尝试访问股票代码为{each_code}的股票3遍，仍有空值。")
            if strategy == 'conservative':
                print("采取保守策略(strategy=conservative)，数据不入库。")
                continue
            elif strategy == 'aggressive':
                print("采取激进策略(strategy=aggressive)，数据入库。")
            else:
                raise ValueError(f"无效的strategy参数: {strategy}。")
        merged_list.append(merged)
    if merged_list:
        final_df = pd.concat(merged_list, ignore_index=True)
        return final_df
    else:
        return pd.DataFrame()



def upsert_daily_markets(engine, ts_codes:list=[],
                         start_date:str='20200806', end_date:str='20250806',
                         strategy:str='conservative', table_name='stock_daily',
                         create_sql_command:str='USE DEFAULT stock_daily', echo:bool=False):
    # 如果数据太多，分配处理，每 step次访问一组，免得出问题
    step = 5
    for i in tqdm(range(0,len(ts_codes),step), desc="fetch & saving data"):
        df_uncleaned = get_stock_daily(ts_codes[i:i + step], start_date, end_date, strategy, echo)
        primary_key = ['ts_code', 'trade_date']  # 根据表结构，主键是 ts_code和 trade_date
        upsert_to_mysql(engine, table_name, df_uncleaned, primary_key, create_sql_command, echo)
        time.sleep(1)

if __name__ == '__main__':
    engine = easyConnect()
    upsert_daily_markets(engine=engine,ts_codes=['000050.SZ'], start_date='20200806', end_date='20250806',
                         table_name='test_stock_daily_1')