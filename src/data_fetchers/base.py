import tushare as ts
import pandas as pd
from utils.utils import easyConnect,easyPro
from utils.logger_config import app_logger as logger
from loguru import logger
import time

class FetcherBase():
    def __init__(self, start_date, end_date, ts_codes:list, queries:list):
        self.start_date = pd.to_datetime(start_date).strftime("%Y%m%d")
        self.end_date = pd.to_datetime(end_date).strftime("%Y%m%d")
        self.ts_codes = ts_codes
        self.queries = queries
        self.pro = easyPro()

    def combine(self, dataframes:list, on:list):
        pass

    def detach_api_params(self, query:dict):
        return query['api'], {key: value for key, value in query.items() if key != 'api'}

    def probar_fetch(self, ts_code, query:dict):
        api, params = self.detach_api_params(query)
        if 'fields' in params.keys():
            params['fields'] += ',ts_code,trade_date'
        logger.trace(params)
        params['start_date'] = self.start_date
        params['end_date'] = self.end_date
        res = ts.pro_bar(ts_code=ts_code, **params)
        required_fields = query['fields'].split(',')
        required_fields.extend(['ts_code', 'trade_date'])
        fields = [col for col in list(set(required_fields)) if col in res.columns]
        return res[fields]

    def extra_info_fetch(self, ts_codes, query:dict):
        api, params = self.detach_api_params(query)
        if 'fields' in params.keys():
            params['fields'] += ',ts_code,trade_date'
        if api == 'stock_basic':
            df = self.pro.stock_basic(**params)
            return df[df['ts_code'].isin(self.ts_codes)]
        elif api == 'stock_company':
            res = []
            for ts_code in self.ts_codes:
                res.append(self.pro.stock_company(ts_code=ts_code, **params))
            return pd.concat(res, ignore_index=True)

    def normal_fetch(self, ts_code, query:dict):
        api, params = self.detach_api_params(query)
        if 'fields' in params.keys():
            params['fields'] += ',ts_code,trade_date'
        logger.trace(params)
        params['start_date'] = self.start_date
        params['end_date'] = self.end_date
        time.sleep(0.1)
        return self.pro.query(api,ts_code=ts_code, **params)

    def ann_fetch(self, ts_codes, query:dict):
        """
        获取季度报告等 并非每天都有的财报数据
        """
        api, params = self.detach_api_params(query)
        if 'fields' in params.keys():
            params['fields'] += ',ts_code'
        logger.trace(params)
        params['start_date'] = self.start_date
        params['end_date'] = self.end_date
        res = []
        for ts_code in self.ts_codes:
            res.append(self.pro.query(api,ts_code=ts_code, **params))
        return pd.concat(res, ignore_index=True)


    @logger.catch
    def fetch(self):

        extra_list = []
        ann_list = []
        remaining_queries = []

        for query in self.queries:
            if query['api'] in ['stock_basic', 'stock_company']: # 这种没有日期的，用特殊获取函数
                df = self.extra_info_fetch(self.ts_codes, query)
                extra_list.append(df)
                #self.queries.remove(query)
            elif 'fields' in query.keys() and 'end_date' in query['fields']:
                df = self.ann_fetch(self.ts_codes, query)
                ann_list.append(df)
                #self.queries.remove(query)
            else:
                remaining_queries.append(query)
        self.queries = remaining_queries

        if len(extra_list) > 1: # 拼接extra_list
            for each in extra_list:
                each.set_index('ts_code', inplace=True)
            extra = pd.concat(extra_list, ignore_index=False, axis=1)
        elif len(extra_list) == 1:
            extra = extra_list[0]
        else:
            extra = None
        if extra is not None:
            extra.reset_index(inplace=True, drop=False)
            extra_long = pd.melt(extra, id_vars='ts_code', var_name='data_name', value_name='data_value')
            extra_long['trade_date'] = '1970-01-01'
        else:
            extra_long = None


        if len(ann_list) > 1:  # 拼接ann_list
            for each in ann_list:
                if each.shape[0] == 0:
                    logger.warning(f"在获取以下几个数据时失败，返回值为空。这可能是因为起始日期与结束日期相差不大。\n{each.columns.tolist()}")
                    continue
                each.set_index(['ts_code', 'end_date'], inplace=True)
            ann = pd.concat(ann_list, ignore_index=False, axis=1)
        elif len(ann_list) == 1:
            ann = ann_list[0]
        else:
            ann = None
        if ann is not None:
            ann.reset_index(inplace=True, drop=False)
            if ann.shape[0] > 0:
                ann_long = pd.melt(ann, id_vars=['ts_code','end_date'], var_name='data_name', value_name='data_value')
                ann_long.rename(columns={'end_date':'trade_date'}, inplace=True)
            else:
                ann_long = None
        else:
            ann_long = None

        if self.queries:
            daily_list = []
            for ts_code in self.ts_codes:
                daily_list_each = []
                for query in self.queries:
                    api = query['api']
                    if api in ['pro_bar','ts.pro_bar']: # pro_bar用特殊获取函数
                        df = self.probar_fetch(ts_code, query)
                    else:
                        df = self.normal_fetch(ts_code, query)
                    df = df.loc[:, ~df.columns.duplicated()]
                    #logger.info(df)
                    df.set_index(['ts_code', 'trade_date'], inplace=True)
                    if 'adj' in query.keys() and query['adj'] in ['qfq','hfq']: # 返回加了复权后缀的列名
                        target_cols = {"open", "high", "low", "close", "change", "pct_chg", "pre_close"}
                        rename_map = {col: f"{col}_{query['adj']}" for col in df.columns if col in target_cols}
                        df = df.rename(columns=rename_map)
                    daily_list_each.append(df)
                daily_list.append(pd.concat(daily_list_each, ignore_index=False, axis=1))
            daily = pd.concat(daily_list, ignore_index=False, axis=0)
            daily.reset_index(inplace=True, drop=False)
            daily_long = pd.melt(
                daily,
                id_vars=['ts_code', 'trade_date'],
                var_name='data_name',
                value_name='data_value'
            )
            logger.trace("daily_long预览：{daily_long[:5]}")
        else:
            daily_long = None

        ret = pd.concat([ann_long, daily_long, extra_long], ignore_index=True)
        return ret



if __name__ == '__main__':
    test = FetcherBase(
        start_date='20250201',
        end_date='20250801',
        ts_codes=['000001.SZ','600109.SH'],
        queries=[{'api':'daily_basic', 'fields':'close, pe, pb, ps, pb_ttm'}, # pb_ttm doesn't exist
                 {'api':'stock_basic','fields':'name,symbol'},
                 {'api':'stock_company','fields':'reg_capital,province'},
                 {'api':'pro_bar','fields':'open,high,low,vol','adj':'qfq'},
                 {'api':'income', 'fields':'end_date,revenue,n_income,oper_cost,operate_profit'},
                 {'api':'balancesheet', 'fields':'end_date,total_assets,total_liab,total_hldr_eqy_inc_min_int,accounts_receiv'},
                 {'api':'stk_holdernumber', 'fields':'end_date,holder_num'}
                 ]
    )
    k = test.fetch()
    logger.info(k)
    logger.info(k['data_name'].unique())
    logger.success("success")