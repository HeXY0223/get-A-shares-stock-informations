from utils.utils import *
import tushare as ts
from dotenv import load_dotenv
from data_fetchers.stock_daily_fetcher import upsert_daily_markets
def update_stock_daily(engine, table_name:str, echo=False):
    load_dotenv()
    api_key = os.getenv("API_KEY")
    pro = ts.pro_api(api_key)
    ts_codes = pd.read_sql('select ts_code from {}'.format(table_name), engine)
    ts_codes = ts_codes['ts_code'].unique()
    print(ts_codes)
    print(ts_codes.shape)

    # 获取今天的日期 格式：date_today = yyyymmdd
    # 表里面有一联合主键：(ts_code, trade_date)，需要访问该ts_code对应最晚的trade_date
    # latest = 最晚的trade_date oldest = 最早的trade_date
    # 有一些股票已经停牌了 大多数股票仍然在运行 如何判断最晚的trade_date呢?
    # 我们找到众数多、离当前交易日近的那天作为latest 并且把latest之前3个月都不更新数据的股票视为已经停牌的股票
    # 除去已经停牌的股票之后 剩下的就是 available_ts_codes
    available_ts_codes = []
    upsert_daily_markets(engine=engine,ts_codes=available_ts_codes, start_date=latest, end_date=date_today,
                         table_name=table_name, echo=echo)

    # 如果有分股分红导致的前复权数据变化，则更改之
    curr_qfq_data_list = []
    qfq_changed_ts_codes = []
    for ts_code in available_ts_codes:
         if condition: # 如果数据库中latest那一天的复权因子 和 今天的复权因子不同则：
             qfq_changed_ts_codes.append(ts_code)
    for ts_code in qfq_changed_ts_codes:
        curr_qfq_each = ts.pro_api(ts_code=ts_code, start_date=oldest, end_date=date_today, adj='qfq',
                                   fields="ts_code, trade_date, open, high, low, close, change, pre_close")
        curr_qfq_data_list.append(curr_qfq_each)
    if curr_qfq_data_list:
        curr_qfq_data = pd.concat(curr_qfq_data_list, ignore_index=True)
        curr_qfq_data.rename(
            columns={'open': 'open_qfq', 'high': 'high_qfq', 'low': 'low_qfq', 'close': 'close_qfq',
                     'pre_close': 'pre_close_qfq', 'change': 'price_change_qfq'},inplace=True)
        upsert_to_mysql(engine=engine, table_name=table_name, df_uncleaned=curr_qfq_data,
                        primary_key=['ts_code','trade_date'], echo=echo)

if __name__ == '__main__':
    update_stock_daily(easyConnect(), table_name="stock_daily_electronic_information")