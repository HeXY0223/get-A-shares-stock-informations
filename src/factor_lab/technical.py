# src/factor_lab/technical.py

import pandas as pd
import numpy as np
from .base import FactorBase
from utils.utils import easyPro
import tushare as ts
from utils.logger_config import app_logger as logger
from loguru import logger

def get_technical_data(ts_codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取日线行情数据的示例函数 (占位符)。

    返回的 DataFrame 应包含:
    - ts_code: 股票代码
    - trade_date: 交易日期
    - close: 复权收盘价
    - volume: 成交量 (用于某些技术指标)
    """

    logger.trace(f"正在获取 {len(ts_codes)} 支股票从 {start_date} 到 {end_date} 的数据... (此为模拟数据)")
    start_date = start_date.replace('-', '')
    end_date = end_date.replace('-', '')
    all_data = []
    pro = easyPro()
    for code in ts_codes:
        df = ts.pro_bar(ts_code=code, start_date=start_date, end_date=end_date,
                        fields='ts_code,trade_date,close,vol', adj='qfq')
        all_data.append(df)
    concater = pd.concat(all_data).set_index('trade_date')
    concater.rename(columns={'vol': 'volume'}, inplace=True)
    return concater


# --- 技术类因子实现 ---

class MACD(FactorBase):
    """
    MACD指标
    计算公式：12日EMA - 26日EMA（需计算快慢线差值）

    MACD (Moving Average Convergence Divergence) 是一个趋势跟踪动量指标，
    通过计算两条不同速度的指数移动平均线的差值来反映价格趋势的变化。
    """

    @property
    def factor_name(self) -> str:
        return "MACD指标"

    def calculate(self) -> pd.DataFrame:
        # 需要往前多取数据以确保EMA计算的稳定性，通常取3-4倍的最长周期
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(days=120)).strftime('%Y-%m-%d')

        # 获取复权收盘价数据
        daily_data = get_technical_data(self.ts_codes, start_dt_extended, self.end_date)
        close_prices = daily_data.pivot(columns='ts_code', values='close')

        # 计算12日EMA和26日EMA
        # EMA = 指数移动平均，对近期数据给予更高权重
        ema_12 = close_prices.ewm(span=12, adjust=False).mean()
        ema_26 = close_prices.ewm(span=26, adjust=False).mean()

        # 计算MACD线 = 12日EMA - 26日EMA
        macd_line = ema_12 - ema_26

        # 截取最终需要的日期范围
        return macd_line.loc[self.start_date.replace("-", ""):self.end_date.replace("-", "")]


class BollingerBandWidth(FactorBase):
    """
    布林带宽度
    计算公式：(上轨 - 下轨) / 中轨（20日移动平均）

    布林带宽度用来衡量价格波动的相对大小，宽度越大表明波动性越高，
    宽度越小表明波动性越低。通常用于识别市场的挤压和扩张状态。
    """

    def __init__(self, ts_codes: list[str], start_date: str, end_date: str, period: int = 20,
                 std_multiplier: float = 2.0):
        """
        初始化布林带计算参数

        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            period: 移动平均周期，默认20日
            std_multiplier: 标准差倍数，默认2倍
        """
        super().__init__(ts_codes, start_date, end_date)
        self.period = period
        self.std_multiplier = std_multiplier

    @property
    def factor_name(self) -> str:
        return "布林带宽度"

    def calculate(self) -> pd.DataFrame:
        # 需要往前多取数据以确保移动平均计算的稳定性
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(days=60)).strftime('%Y-%m-%d')

        # 获取复权收盘价数据
        daily_data = get_technical_data(self.ts_codes, start_dt_extended, self.end_date)
        close_prices = daily_data.pivot(columns='ts_code', values='close')

        # 计算20日移动平均线（中轨）
        middle_band = close_prices.rolling(window=self.period, min_periods=self.period).mean()

        # 计算20日移动标准差
        rolling_std = close_prices.rolling(window=self.period, min_periods=self.period).std()

        # 计算上轨和下轨
        # 上轨 = 中轨 + (标准差倍数 × 移动标准差)
        # 下轨 = 中轨 - (标准差倍数 × 移动标准差)
        upper_band = middle_band + (self.std_multiplier * rolling_std)
        lower_band = middle_band - (self.std_multiplier * rolling_std)

        # 计算布林带宽度 = (上轨 - 下轨) / 中轨
        # 这个指标标准化了带宽，使得不同价格水平的股票可以进行比较
        band_width = (upper_band - lower_band) / middle_band

        # 截取最终需要的日期范围
        return band_width.loc[self.start_date.replace("-", ""):self.end_date.replace("-", "")]


# --- 可选的扩展技术指标 ---
# 如果你需要更多技术指标，可以参考以下示例：

class MACD_Signal(FactorBase):
    """
    MACD信号线
    计算公式：MACD线的9日EMA

    这是MACD指标的扩展，信号线用于产生买卖信号。
    当MACD线上穿信号线时产生买入信号，下穿时产生卖出信号。
    """

    @property
    def factor_name(self) -> str:
        return "MACD信号线"

    def calculate(self) -> pd.DataFrame:
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(days=120)).strftime('%Y-%m-%d')

        daily_data = get_technical_data(self.ts_codes, start_dt_extended, self.end_date)
        close_prices = daily_data.pivot(columns='ts_code', values='close')

        # 先计算MACD线
        ema_12 = close_prices.ewm(span=12, adjust=False).mean()
        ema_26 = close_prices.ewm(span=26, adjust=False).mean()
        macd_line = ema_12 - ema_26

        # 计算MACD信号线 = MACD线的9日EMA
        signal_line = macd_line.ewm(span=9, adjust=False).mean()

        return signal_line.loc[self.start_date.replace("-", ""):self.end_date.replace("-", "")]


class MACD_Histogram(FactorBase):
    """
    MACD柱状图
    计算公式：MACD线 - MACD信号线

    MACD柱状图反映了MACD线和信号线之间的差距，
    可以更早地识别趋势的变化。
    """

    @property
    def factor_name(self) -> str:
        return "MACD柱状图"

    def calculate(self) -> pd.DataFrame:
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(days=120)).strftime('%Y-%m-%d')

        daily_data = get_technical_data(self.ts_codes, start_dt_extended, self.end_date)
        close_prices = daily_data.pivot(columns='ts_code', values='close')

        # 计算MACD线
        ema_12 = close_prices.ewm(span=12, adjust=False).mean()
        ema_26 = close_prices.ewm(span=26, adjust=False).mean()
        macd_line = ema_12 - ema_26

        # 计算信号线
        signal_line = macd_line.ewm(span=9, adjust=False).mean()

        # 计算MACD柱状图 = MACD线 - 信号线
        macd_histogram = macd_line - signal_line

        return macd_histogram.loc[self.start_date.replace("-", ""):self.end_date.replace("-", "")]
