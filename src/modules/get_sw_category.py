import tushare as ts
import os
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy

from utils.utils import easyConnect, output_controller


# 注：申万行业分类一共包含了 5733只股票，但是有些股票不止分在了一个行业，所以实际上一共有 5815行。
#    这也导致了它无法与 stock_basic_info合并。

@output_controller
def get_sw_category(engine, src:str='SW2021', echo=False):
    load_dotenv()
    api_key = os.getenv('API_KEY')
    ts.set_token(api_key)
    pro = ts.pro_api(api_key)
    try:
        inspector = sqlalchemy.inspect(engine)
        if not inspector.has_table('sw_category'):
            if echo: print(f"表格 'sw_category' 不存在，正在自动创建...")
            l1 = pro.index_classify(level='L1', src='SW2021')
            all_of_stocks = []
            for item in l1['index_code']:
                all_of_stocks.append(pro.index_member_all(l1_code=item))
            df_all = pd.concat(all_of_stocks, ignore_index=True)
            df_all.to_sql('sw_category', con=engine, if_exists='replace', index=False)
            if echo: print("success")
        else:
            if echo: print("表格 'sw_category' 已存在，无需创建")
    except Exception as e:
        if echo: print(f"get_sw_category出现错误：{e}")

if __name__ == '__main__':
    get_sw_category(engine=easyConnect(), echo=True)