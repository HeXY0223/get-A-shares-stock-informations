import tushare as ts
from factor_lab.base import FactorBase
from factor_lab.value import EVEBITDA
import pandas as pd
from utils.utils import easyConnect
basic_info_query = "select ts_code from stock_basic_info where list_status='L'"
ts_codes = pd.read_sql(basic_info_query, con=easyConnect())['ts_code'].tolist()
ts_codes = ts_codes[:30]
test = EVEBITDA(
    ts_codes=ts_codes,
    start_date='20250201',
    end_date='20250801'
)
test.save_to_db(table_name='test', create_sql='USE DEFAULT factor_panel_data_without_foreign_key')