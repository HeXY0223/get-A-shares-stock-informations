# src/factor_lab/sentiment.py

import pandas as pd
import numpy as np
from dotenv import load_dotenv

from .base import FactorBase
from utils.utils import *
import tushare as ts
from utils.logger_config import app_logger as logger
from loguru import logger

def get_growth_data(ts_codes: list[str], start_date: str, end_date: str, report_type: str) -> pd.DataFrame:
    logger.trace(f"正在获取 {len(ts_codes)} 支股票从 {start_date} 到 {end_date} 的数据...")
    start_date = start_date.replace('-', '')
    end_date = end_date.replace('-', '')
    all_data = []
    pro = easyPro()
    for code in ts_codes:
        if report_type == 'income':
            df = pro.income(ts_code=code, start_date=start_date, end_date=end_date,
                            fields="ts_code,end_date,revenue,n_income,oper_cost")
            # 只添加非空的DataFrame 避免FutureWarning
            if not df.empty:
                all_data.append(df)
        elif report_type == 'cashflow':
            df = pro.cashflow(ts_code=code, start_date=start_date, end_date=end_date,
                              fields="ts_code,end_date,c_pay_acq_const_fiolta")
            if not df.empty:
                all_data.append(df)
    concater = pd.concat(all_data, ignore_index=True)
    concater.rename(columns={'end_date':'trade_date'}, inplace=True)
    return concater

class Revenue(FactorBase):
    @property
    def factor_name(self) -> str:
        return "主营业收入"

    def calculate(self) -> pd.DataFrame:
        real_start_date = (pd.to_datetime(self.start_date) - pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        real_end_date = (pd.to_datetime(self.end_date) + pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        rev = get_growth_data(self.ts_codes, real_start_date, real_end_date, report_type='income')
        cp = rev.copy()
        rev.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last', inplace=True)
        rev_wide = rev.pivot(index='trade_date', columns='ts_code', values='revenue')
        #不排序了直接allin
        start_dt_str = self.start_date.replace("-", "")
        end_dt_str = self.end_date.replace("-", "")
        return rev_wide.loc[start_dt_str:end_dt_str]

class NetProfitGR(FactorBase):
    @property
    def factor_name(self) -> str:
        return "净利润增长率"
    def calculate(self) -> pd.DataFrame:
        real_start_date = (pd.to_datetime(self.start_date) - pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        real_end_date = (pd.to_datetime(self.end_date) + pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        net_profit = get_growth_data(self.ts_codes, real_start_date, real_end_date, report_type='income')
        net_profit.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last', inplace=True)

        net_profitCR = self.calculate_period_change_rate_from_long_data(data=net_profit,
                                                                        value_col='n_income',
                                                                        use_abs_denominator=True)
        npCR_wide = net_profitCR.pivot(index='trade_date', columns='ts_code', values='change_rate')
        start_dt_str = self.start_date.replace("-", "")
        end_dt_str = self.end_date.replace("-", "")
        return npCR_wide.loc[start_dt_str:end_dt_str]

class GrossProfitGR(FactorBase):
    @property
    def factor_name(self) -> str:
        return "毛利润增长率"
    def calculate(self) -> pd.DataFrame:
        real_start_date = (pd.to_datetime(self.start_date) - pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        real_end_date = (pd.to_datetime(self.end_date) + pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        gross_profit = get_growth_data(self.ts_codes, real_start_date, real_end_date, report_type='income')
        gross_profit.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last', inplace=True)
        gross_profit['gross_profit'] = gross_profit['revenue'] - gross_profit['oper_cost']
        gross_profitCR = self.calculate_period_change_rate_from_long_data(data=gross_profit,
                                                                          value_col='gross_profit',
                                                                          use_abs_denominator=True)
        gpCR_wide = gross_profitCR.pivot(index='trade_date', columns='ts_code', values='change_rate')
        start_dt_str = self.start_date.replace("-", "")
        end_dt_str = self.end_date.replace("-", "")
        return gpCR_wide.loc[start_dt_str:end_dt_str]

class RevenueGR(FactorBase):
    @property
    def factor_name(self) -> str:
        return "营业收入增长率"
    def calculate(self) -> pd.DataFrame:
        real_start_date = (pd.to_datetime(self.start_date) - pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        real_end_date = (pd.to_datetime(self.end_date) + pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        revenue = get_growth_data(self.ts_codes, real_start_date, real_end_date, report_type='income')
        revenue.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last', inplace=True)

        revenueCR = self.calculate_period_change_rate_from_long_data(data=revenue,
                                                                     value_col='revenue',
                                                                     use_abs_denominator=True)
        rCR_wide = revenueCR.pivot(index='trade_date', columns='ts_code', values='change_rate')
        start_dt_str = self.start_date.replace("-", "")
        end_dt_str = self.end_date.replace("-", "")
        return rCR_wide.loc[start_dt_str:end_dt_str]

class CapExGR(FactorBase):
    @property
    def factor_name(self) -> str:
        return "资本支出增长率"
    def calculate(self) -> pd.DataFrame:
        real_start_date = (pd.to_datetime(self.start_date) - pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        real_end_date = (pd.to_datetime(self.end_date) + pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        capex = get_growth_data(self.ts_codes, real_start_date, real_end_date, report_type='cashflow')
        capex.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last', inplace=True)

        capexCR = self.calculate_period_change_rate_from_long_data(data=capex,
                                                                   value_col='c_pay_acq_const_fiolta',
                                                                   use_abs_denominator=True)
        capexCR_wide = capexCR.pivot(index='trade_date', columns='ts_code', values='change_rate')
        start_dt_str = self.start_date.replace("-", "")
        end_dt_str = self.end_date.replace("-", "")
        return capexCR_wide.loc[start_dt_str:end_dt_str]