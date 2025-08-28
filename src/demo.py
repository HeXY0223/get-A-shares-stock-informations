# src/demo.py
import warnings
# 忽略所有 FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

from data_fetchers.stock_daily_fetcher import upsert_daily_markets
from utils.utils import *
from data_fetchers.index_basics_fetcher import upsert_index_basics
from data_fetchers.index_daily_fetcher import upsert_index_daily
from data_fetchers.sw_category_fetcher import get_sw_category
from data_fetchers.stock_basics_fetcher import upsert_stock_basics
from factor_lab import factor_prepocess, factor_IC_analyze
from pipelines.factor_update import run_factor_update
from tqdm import tqdm
from data_fetchers.base import FetcherBase
@logger.catch()
def main():
    end_date = pd.Timestamp.today()
    start_date = end_date - pd.DateOffset(months=6)
    end_str = end_date.strftime('%Y%m%d')
    start_str = start_date.strftime('%Y%m%d')
    engine = easyConnect()
    # 下载申万行业分类（如果没有）
    logger.info("正在进行：下载申万行业分类（如果没有）")
    get_sw_category(engine=engine)
    # 下载股票基础信息（如果没有）
    logger.info("正在进行：下载股票基础信息（如果没有）")
    upsert_stock_basics(engine=engine)
    # 筛选出当前上市的股票
    logger.info("正在进行：筛选出当前上市的股票")
    basic_info_query = "select ts_code from stock_basic_info where list_status='L'"
    ts_codes = pd.read_sql(basic_info_query, con=engine)['ts_code'].tolist()
    # 下载这些股票从半年前到现在的数据
    logger.info("正在进行：下载当前上市的股票从半年前到现在的数据（预计：3小时）")
    # upsert_daily_markets(engine=engine, ts_codes=ts_codes,
    #                      start_date=start_str, end_date=end_str,
    #                      table_name='stock_daily')
    basic_info_query = "select ts_code from stock_basic_info where list_status='L'"
    engine = easyConnect()
    ts_codes = pd.read_sql(basic_info_query, con=engine)['ts_code'].tolist()
    length = len(ts_codes)
    for i in tqdm(range(length)):
        test = FetcherBase(
            start_date='20250101',
            end_date='20250801',
            ts_codes=[ts_codes[i]],
            queries=[{'api': 'daily_basic', 'fields': 'turnover_rate,turnover_rate_f,total_mv,pe_ttm,pb,ps_ttm,dv_ttm'},
                     {'api': 'margin_detail', 'fields': 'rzye'},
                     {'api': 'pro_bar', 'fields': 'open,high,low,close,vol,amount,pct_chg', 'adj': 'qfq'},

                     # {'api':'stock_basic','fields':'name,symbol'},
                     # {'api':'stock_company','fields':'reg_capital,province'},

                     {'api': 'income', 'fields': 'end_date,revenue,n_income,oper_cost,operate_profit'},
                     {'api': 'cashflow', 'fields': 'end_date,c_pay_acq_const_fiolta,n_cashflow_act'},
                     {'api': 'balancesheet',
                      'fields': 'end_date,total_assets,total_liab,total_hldr_eqy_inc_min_int,accounts_receiv,'
                                'money_cap,st_borr,non_cur_liab_due_1y,lt_borr,bond_payable'},
                     {'api': 'stk_holdernumber', 'fields': 'end_date,holder_num'},
                     {'api': 'fina_indicator', 'fields': 'end_date,ebitda'}
                     ]
        )
        k = test.fetch()
        upsert_to_mysql(engine, table_name='test', df_uncleaned=k,
                        primary_key=['ts_code', 'trade_date', 'data_name'],
                        create_sql_command='USE DEFAULT narrow_data')

    # 获取因子数据
    logger.info("正在进行：获取因子数据")
    run_factor_update(ts_codes=ts_codes, start_date=start_str, end_date=end_str, table_name='factor_raw')
    
    # 预处理
    logger.info("正在进行：因子预处理")
    factor_processor = factor_prepocess.FactorPreProcessor(
        start_date=start_str,
        end_date=end_str,
        table_raw='factor_raw',
        table_processed='factor_processed',
        stock_daily='temp_total_mv',
        create_sql_processed='USE DEFAULT factor_panel_data_without_foreign_key'
    )
    factor_processor.process()
    # IC值计算
    logger.info("正在进行：IC值计算")
    IC_analyzer = factor_IC_analyze.ICAnalyzer(
        factor_table='factor_processed',
        market_table='stock_daily'
    )
    ret = IC_analyzer.run_analysis()
    logger.info("\n--- 因子分析结果汇总 ---")
    logger.info(ret)
    ret.to_sql('factor_IC', con=engine, if_exists='append', index=False)

if __name__ == '__main__':
    main()