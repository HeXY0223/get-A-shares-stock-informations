import warnings
# 忽略所有 FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from utils.utils import *  # 假设包含 easyConnect 和 upsert_to_mysql
import tushare as ts
from dotenv import load_dotenv
from modules.get_stock_daily import upsert_daily_markets, get_stock_daily  # 假设此函数用于增量更新
from datetime import datetime, timedelta
import os
from tqdm import tqdm

def update_stock_daily(engine, table_name: str, echo=False):
    """
    更新股票日线数据，包括增量更新和因复权因子变化而进行的全量修正。
    """
    load_dotenv()
    api_key = os.getenv("API_KEY")
    if not api_key:
        raise ValueError("无法获取 API_KEY，请检查 .env 文件。")
    pro = ts.pro_api(api_key)

    # 从数据库一次性获取所有股票代码及其最早和最晚的交易日期
    sql_query = f"""
        SELECT 
            ts_code, 
            MIN(trade_date) AS oldest_date, 
            MAX(trade_date) AS latest_date 
        FROM {table_name} 
        GROUP BY ts_code
    """
    stock_dates_df = pd.read_sql(sql_query, engine)
    if stock_dates_df.empty:
        print(f"数据表 {table_name} 为空，无法继续更新。")
        return

    # 【修正 #1】: 明确将 trade_date 字符串转换为 datetime 对象，以便后续比较
    # Tushare 返回的日期格式为 'YYYYMMDD'
    stock_dates_df['oldest_date'] = pd.to_datetime(stock_dates_df['oldest_date'], format='%Y%m%d')
    stock_dates_df['latest_date'] = pd.to_datetime(stock_dates_df['latest_date'], format='%Y%m%d')

    all_ts_codes = stock_dates_df['ts_code'].unique()
    print(f"数据库中共有 {len(all_ts_codes)} 只股票。")

    # --- 第一部分：增量更新日线数据 ---

    # 获取今天的日期（作为字符串和日期对象，方便使用）
    date_today_str = datetime.now().strftime('%Y%m%d')
    date_today_obj = datetime.now().date()  # 用于日期比较

    # 【修正 #2】: cutoff_date 保持为 date 对象，而不是转换为字符串
    cutoff_date = (datetime.now() - timedelta(days=90)).date()

    # 【修正 #3】: 现在是 date 对象之间的比较，不会再有 TypeError
    # latest_date 列已经是 datetime 类型，.dt.date 可以提取其日期部分
    active_stocks_df = stock_dates_df[stock_dates_df['latest_date'].dt.date >= cutoff_date].copy()
    available_ts_codes = active_stocks_df['ts_code'].tolist()

    print(f"识别出 {len(available_ts_codes)} 只活跃股票进行增量更新。")

    # 对每只活跃股票，从其数据库中的最新日期+1天开始获取数据
    print("开始进行增量数据更新...")
    for index, row in tqdm(active_stocks_df.iterrows()):
        ts_code = row['ts_code']
        # start_date 应为数据库中最新日期的后一天
        latest_date_in_db = row['latest_date'].date()
        start_date_obj = latest_date_in_db + timedelta(days=1)

        # 只有当开始日期不晚于今天时，才需要获取数据
        if start_date_obj <= date_today_obj:
            start_date_str = start_date_obj.strftime('%Y%m%d')
            # upsert_daily_markets(engine=engine, ts_codes=[ts_code], start_date=start_date_str, end_date=date_today_str,
            #                      table_name=table_name, echo=echo)
            df_uncleaned = get_stock_daily(ts_code=[ts_code],
                                           start_date=start_date_str, end_date=date_today_str,
                                           echo=echo)
            primary_key = ['ts_code', 'trade_date']
            upsert_to_mysql(engine=engine, table_name=table_name, df_uncleaned=df_uncleaned,
                            primary_key=primary_key, echo=echo)

    print("增量数据更新完成。")

    # --- 第二部分：检查并修正因复权因子变化导致的数据不一致 ---

    print("开始检查复权因子变化...")
    # 将股票日期信息转为字典，方便快速查找
    # 存储字符串格式的日期，因为后续API调用和SQL查询需要字符串
    stock_dates_dict = stock_dates_df.set_index('ts_code').copy()
    stock_dates_dict['oldest_date'] = stock_dates_dict['oldest_date'].dt.strftime('%Y%m%d')
    stock_dates_dict['latest_date'] = stock_dates_dict['latest_date'].dt.strftime('%Y%m%d')
    stock_dates_dict = stock_dates_dict.to_dict('index')

    # 从数据库获取活跃股票在最新交易日的复权因子
    chunk_size = 100
    db_adj_factors = []
    available_ts_codes_chunks = [available_ts_codes[i:i + chunk_size] for i in
                                 range(0, len(available_ts_codes), chunk_size)]

    for chunk in available_ts_codes_chunks:
        # 这里使用的 latest_date 是字符串格式
        latest_dates_info = [f"('{code}', '{stock_dates_dict[code]['latest_date']}')" for code in chunk]
        sql_adj_factor_query = f"""
            SELECT ts_code, adj_factor 
            FROM {table_name} 
            WHERE (ts_code, trade_date) IN ({','.join(latest_dates_info)})
        """
        db_adj_factors.append(pd.read_sql(sql_adj_factor_query, engine))

    if not db_adj_factors:
        print("未能从数据库获取复权因子信息，跳过检查。")
        qfq_changed_ts_codes = []
    else:
        db_adj_factor_df = pd.concat(db_adj_factors, ignore_index=True).set_index('ts_code')

        # 从 Tushare 获取最新的复权因子
        ts_adj_factors = []
        for chunk in available_ts_codes_chunks:
            # daily_basic 接口需要用逗号分隔的字符串
            ts_adj_factors.append(pro.daily_basic(ts_code=','.join(chunk), fields='ts_code,trade_date,adj_factor'))

        ts_adj_factor_df = pd.concat(ts_adj_factors, ignore_index=True).drop_duplicates('ts_code').set_index('ts_code')

        # 合并数据库和Tushare的复权因子，进行比较
        comparison_df = db_adj_factor_df.join(ts_adj_factor_df, lsuffix='_db', rsuffix='_ts')
        comparison_df.dropna(inplace=True)  # 确保两边都有数据才比较

        # 如果数据库中latest那一天的复权因子 和 今天的复权因子不同则：
        # 注意：Tushare的复权因子是浮点数，直接比较可能因精度问题出错，最好使用 np.isclose
        import numpy as np
        qfq_changed_ts_codes = comparison_df[
            ~np.isclose(comparison_df['adj_factor_db'], comparison_df['adj_factor_ts'])
        ].index.tolist()

    if not qfq_changed_ts_codes:
        print("没有检测到复权因子发生变化的股票。")
    else:
        print(f"检测到 {len(qfq_changed_ts_codes)} 只股票的复权因子发生变化，将刷新其全部历史数据。")
        print(qfq_changed_ts_codes)

        curr_qfq_data_list = []
        for ts_code in qfq_changed_ts_codes:
            oldest_date_str = stock_dates_dict[ts_code]['oldest_date']
            # 使用 pro.pro_bar 获取复权数据
            curr_qfq_each = ts.pro_bar(ts_code=ts_code, start_date=oldest_date_str, end_date=date_today_str, adj='qfq',
                                        fields="ts_code,trade_date,open,high,low,close,change,pre_close")
            if curr_qfq_each is not None and not curr_qfq_each.empty:
                curr_qfq_data_list.append(curr_qfq_each)

        if curr_qfq_data_list:
            print("正在合并并更新全历史前复权数据...")
            curr_qfq_data = pd.concat(curr_qfq_data_list, ignore_index=True)
            curr_qfq_data.rename(
                columns={'open': 'open_qfq', 'high': 'high_qfq', 'low': 'low_qfq', 'close': 'close_qfq',
                         'pre_close': 'pre_close_qfq', 'change': 'price_change_qfq'}, inplace=True)

            upsert_to_mysql(engine=engine, table_name=table_name, df_uncleaned=curr_qfq_data,
                            primary_key=['ts_code', 'trade_date'], echo=echo)
            print("全历史前复权数据更新完成。")

    print("所有更新任务执行完毕。")


if __name__ == '__main__':
    try:
        db_engine = easyConnect()
        update_stock_daily(engine=db_engine, table_name="stock_daily_electronic_information")
    except Exception as e:
        print(f"程序执行出错: {e}")

