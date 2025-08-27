import warnings
# 忽略所有 FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)
from utils.utils import *  # 假设包含 easyConnect 和 upsert_to_mysql
import tushare as ts
from dotenv import load_dotenv
from data_fetchers.stock_daily_fetcher import upsert_daily_markets  # 假设此函数用于增量更新
from datetime import datetime, timedelta
import os
import numpy as np
from utils.logger_config import app_logger as logger
from loguru import logger

@logger.catch()
def update_stock_daily(engine, table_name: str):
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
        logger.warning(f"数据表 {table_name} 为空，无法继续更新。")
        return

    stock_dates_df['oldest_date'] = pd.to_datetime(stock_dates_df['oldest_date'], format='%Y%m%d')
    stock_dates_df['latest_date'] = pd.to_datetime(stock_dates_df['latest_date'], format='%Y%m%d')

    all_ts_codes = stock_dates_df['ts_code'].unique()
    logger.info(f"数据库中共有 {len(all_ts_codes)} 只股票。")

    # --- 第一部分：增量更新日线数据 ---
    date_today_str = datetime.now().strftime('%Y%m%d')
    date_today_obj = datetime.now().date()

    cutoff_date = (datetime.now() - timedelta(days=90)).date()

    active_stocks_df = stock_dates_df[stock_dates_df['latest_date'].dt.date >= cutoff_date].copy()
    available_ts_codes = active_stocks_df['ts_code'].tolist()

    logger.info(f"识别出 {len(available_ts_codes)} 只活跃股票进行增量更新。")

    logger.info("开始进行增量数据更新...")
    grouped = active_stocks_df.groupby('latest_date')
    for latest_date, group in grouped:
        start_date_obj = latest_date.date() + timedelta(days=1)

        if start_date_obj <= date_today_obj:
            start_date_str = start_date_obj.strftime('%Y%m%d')
            ts_codes_to_update = group['ts_code'].tolist()

            logger.info(f"批量更新 {len(ts_codes_to_update)} 只股票，日期范围: {start_date_str} -> {date_today_str}")
            upsert_daily_markets(engine=engine, ts_codes=ts_codes_to_update, start_date=start_date_str,
                                 end_date=date_today_str,
                                 table_name=table_name)

    logger.info("增量数据更新完成。")

    # --- 第二部分：检查并修正因复权因子变化导致的数据不一致 ---

    logger.info("开始检查复权因子变化...")
    stock_dates_dict = {row['ts_code']: {'oldest_date': row['oldest_date'].strftime('%Y%m%d'),
                                         'latest_date': row['latest_date'].strftime('%Y%m%d')} for _, row in
                        active_stocks_df.iterrows()}

    chunk_size = 100
    db_adj_factors_list = []
    available_ts_codes_chunks = [available_ts_codes[i:i + chunk_size] for i in
                                 range(0, len(available_ts_codes), chunk_size)]

    for chunk in available_ts_codes_chunks:
        latest_dates_info = [f"('{code}', '{stock_dates_dict[code]['latest_date']}')" for code in chunk if
                             code in stock_dates_dict]
        if not latest_dates_info: continue
        sql_adj_factor_query = f"""
            SELECT ts_code, trade_date, adj_factor 
            FROM {table_name} 
            WHERE (ts_code, trade_date) IN ({','.join(latest_dates_info)})
        """
        db_adj_factors_list.append(pd.read_sql(sql_adj_factor_query, engine))

    if not db_adj_factors_list:
        logger.warning("未能从数据库获取任何复权因子信息，跳过检查。")
        qfq_changed_ts_codes = []
    else:
        db_adj_factor_df = pd.concat(db_adj_factors_list, ignore_index=True)
        if 'adj_factor' in db_adj_factor_df.columns:
            db_adj_factor_df.rename(columns={'adj_factor': 'adj_factor_db'}, inplace=True)
            db_adj_factor_df.set_index('ts_code', inplace=True)
        else:
            logger.warning("警告: 从数据库返回的数据中未找到 'adj_factor' 列。")
            db_adj_factor_df = pd.DataFrame()

        # ######################################################################## #
        # #【代码修改区域开始】: 使用新的 adj_factor 接口替换旧的 daily_basic 接口 #
        # ######################################################################## #

        # 核心逻辑：比较我们数据库中存储的旧复权因子，和Tushare现在为同一天提供的复权因子是否一致
        logger.info("正在从 Tushare 获取最新的历史复权因子进行比对...")

        # 1. 创建一个从 assets latest_date -> [ts_code_list] 的映射，方便按天查询
        date_to_codes_map = active_stocks_df.groupby(active_stocks_df['latest_date'].dt.strftime('%Y%m%d'))[
            'ts_code'].apply(list).to_dict()

        # 2. 循环每一天，获取当天的所有股票的复权因子，然后过滤出我们需要的
        ts_adj_factors_list = []
        for trade_date, codes_for_date in date_to_codes_map.items():
            try:
                # 使用新的 pro.adj_factor 接口
                daily_factors_df = pro.adj_factor(trade_date=trade_date, fields='ts_code,adj_factor')
                if daily_factors_df is not None and not daily_factors_df.empty:
                    # 过滤出我们关心的股票
                    filtered_df = daily_factors_df[daily_factors_df['ts_code'].isin(codes_for_date)]
                    ts_adj_factors_list.append(filtered_df)
            except Exception as e:
                logger.error(f"获取 {trade_date} 的复权因子时出错: {e}")

        # 3. 合并所有从Tushare查询到的结果
        if ts_adj_factors_list:
            ts_adj_factor_df = pd.concat(ts_adj_factors_list, ignore_index=True)
            if 'adj_factor' in ts_adj_factor_df.columns:
                ts_adj_factor_df.rename(columns={'adj_factor': 'adj_factor_ts'}, inplace=True)
                ts_adj_factor_df.set_index('ts_code', inplace=True)
            else:
                logger.warning("警告: 从Tushare API(adj_factor)返回的数据中未找到 'adj_factor' 列。")
                ts_adj_factor_df = pd.DataFrame()
        else:
            logger.warning("未能从Tushare获取任何用于比对的复权因子。")
            ts_adj_factor_df = pd.DataFrame()

        # ######################################################################## #
        # #【代码修改区域结束】                                                   #
        # ######################################################################## #

        if not db_adj_factor_df.empty and not ts_adj_factor_df.empty:
            comparison_df = db_adj_factor_df.join(ts_adj_factor_df, how='inner')

            # 确保用于比较的列存在
            if 'adj_factor_db' in comparison_df.columns and 'adj_factor_ts' in comparison_df.columns:
                comparison_df.dropna(subset=['adj_factor_db', 'adj_factor_ts'], inplace=True)
                qfq_changed_ts_codes = comparison_df[
                    ~np.isclose(comparison_df['adj_factor_db'], comparison_df['adj_factor_ts'])
                ].index.tolist()
            else:
                qfq_changed_ts_codes = []
        else:
            qfq_changed_ts_codes = []

    if not qfq_changed_ts_codes:
        logger.info("没有检测到复权因子发生变化的股票。")
    else:
        logger.info(f"检测到 {len(qfq_changed_ts_codes)} 只股票的复权因子发生变化，将刷新其全部历史数据。")
        logger.debug(qfq_changed_ts_codes)

        curr_qfq_data_list = []
        for ts_code in qfq_changed_ts_codes:
            oldest_date_str = stock_dates_dict[ts_code]['oldest_date']
            curr_qfq_each = pro.pro_bar(ts_code=ts_code, start_date=oldest_date_str, end_date=date_today_str, adj='qfq',
                                        fields="ts_code,trade_date,open,high,low,close,change,pre_close")
            if curr_qfq_each is not None and not curr_qfq_each.empty:
                curr_qfq_data_list.append(curr_qfq_each)

        if curr_qfq_data_list:
            logger.info("正在合并并更新全历史前复权数据...")
            curr_qfq_data = pd.concat(curr_qfq_data_list, ignore_index=True)
            curr_qfq_data.rename(
                columns={'open': 'open_qfq', 'high': 'high_qfq', 'low': 'low_qfq', 'close': 'close_qfq',
                         'pre_close': 'pre_close_qfq', 'change': 'price_change_qfq'}, inplace=True)

            upsert_to_mysql(engine=engine, table_name=table_name, df_uncleaned=curr_qfq_data,
                            primary_key=['ts_code', 'trade_date'])
            logger.info("全历史前复权数据更新完成。")

    logger.info("所有更新任务执行完毕。")


if __name__ == '__main__':
    try:
        db_engine = easyConnect()
        update_stock_daily(engine=db_engine, table_name="stock_daily")
    except Exception as e:
        logger.error(f"程序执行出错: {e}")
