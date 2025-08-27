# src/factor_lab/momentum.py

import pandas as pd
import numpy as np
from dotenv import load_dotenv

from .base import FactorBase
from utils.utils import *
import tushare as ts
from utils.logger_config import app_logger as logger
from loguru import logger

# 获取指定股票列表在某时间段内的日线行情数据
def get_momentum_data(ts_codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取日线行情数据的示例函数。

    返回的 DataFrame 包含:
    - ts_code: 股票代码
    - trade_date: 交易日期
    - close: 复权收盘价
    """
    logger.trace(f"正在获取 {len(ts_codes)} 支股票从 {start_date} 到 {end_date} 的数据...")
    start_date = start_date.replace('-', '')
    end_date = end_date.replace('-', '')
    all_data = []
    pro = easyPro()
    for code in ts_codes:
        if code.endswith('.SI'):
            df = pro.sw_daily(ts_code=code, start_date=start_date, end_date=end_date)
        else:
            df = ts.pro_bar(ts_code=code, start_date=start_date, end_date=end_date, adj='qfq')
        all_data.append(df)

    return pd.concat(all_data).set_index('trade_date')


# --- 动量因子实现 ---

class Return12M(FactorBase):
    """
    12个月收益率
    计算公式：过去250个交易日累计收益率（复权）
    """

    @property
    def factor_name(self) -> str:
        return "12个月收益率"

    def calculate(self) -> pd.DataFrame:
        # 需要往前多取250个交易日的数据用于计算
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(months=14)).strftime('%Y-%m-%d')

        # 获取复权收盘价
        daily_data = self.fetch_data([{
            'api':'pro_bar','adj':'qfq','fields':'close'
        }], start_date=start_dt_extended)
        daily_data.rename(columns={'close_qfq':'close'}, inplace=True)
        close_prices = daily_data.pivot(columns='ts_code', values='close')
        # 计算250日收益率: (P_t / P_{t-250}) - 1
        factor_values = (close_prices / close_prices.shift(250)) - 1

        # locer = factor_values.loc[self.start_date:self.end_date]
        # 截取最终需要的日期范围
        return factor_values.loc[self.start_date.replace("-",""):self.end_date.replace("-","")]


class Alpha6M(FactorBase):
    """
    6个月超额收益（Alpha）
    计算公式：个股收益率 - 申万电子指数收益率（120日）
    """

    def __init__(self, ts_codes: list[str], start_date: str, end_date: str, index_code: str = '801080.SI'):
        super().__init__(ts_codes, start_date, end_date)
        self.index_code = index_code  # 申万电子指数代码

    @property
    def factor_name(self) -> str:
        return "6个月超额收益（Alpha）"

    def calculate(self) -> pd.DataFrame:
        # 需要往前多取120个交易日的数据
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(months=7)).strftime('%Y-%m-%d')

        # 获取个股和指数的复权收盘价
        all_codes = self.ts_codes + [self.index_code]
        daily_data = self.fetch_data([{
            'api':'pro_bar','adj':'qfq','fields':'close'
        }], start_date=start_dt_extended)
        close_prices = daily_data.pivot(columns='ts_code', values='close')

        # 计算120日收益率
        returns_120d = (close_prices / close_prices.shift(120)) - 1

        # 分离个股和指数
        stock_returns = returns_120d[self.ts_codes]
        index_returns = returns_120d[self.index_code]

        # 计算超额收益 (Alpha)
        # 使用 .subtract() 并设置 axis=0 可以实现列对Series的广播减法
        factor_values = stock_returns.subtract(index_returns, axis=0)
        # locer = factor_values.loc[self.start_date.replace("-",""):self.end_date.replace("-","")]
        return factor_values.loc[self.start_date.replace("-",""):self.end_date.replace("-","")]


class RSI14(FactorBase):
    """
    相对强弱指标（RSI_14）
    计算公式：14日平均涨幅 / (14日平均涨幅 + 14日平均跌幅) × 100
    """

    @property
    def factor_name(self) -> str:
        return "相对强弱指标（RSI_14）"

    def calculate(self) -> pd.DataFrame:
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(days=30)).strftime('%Y-%m-%d')

        daily_data = self.fetch_data([{
            'api': 'pro_bar', 'adj': 'qfq', 'fields': 'close'
        }], start_date=start_dt_extended)
        daily_data.rename(columns={'close_qfq': 'close'}, inplace=True)
        close_prices = daily_data.pivot(columns='ts_code', values='close')

        # 计算价格变化
        delta = close_prices.diff()

        # 分离上涨和下跌
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)

        # 使用EMA（指数移动平均）计算平均涨跌幅，这是RSI的标准算法
        avg_gain = gain.ewm(span=14, adjust=False).mean()
        avg_loss = loss.ewm(span=14, adjust=False).mean()

        # 计算 RS (Relative Strength)
        rs = avg_gain / avg_loss

        # 计算 RSI
        # 公式: RSI = 100 - (100 / (1 + RS))
        rsi = 100 - (100 / (1 + rs))
        # locer = rsi.loc[self.start_date.replace("-",""):self.end_date.replace("-","")]
        return rsi.loc[self.start_date.replace("-",""):self.end_date.replace("-","")]

