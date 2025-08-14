# src/factor_lab/liquidity.py

import pandas as pd
import numpy as np
from .base import FactorBase
from utils.utils import easyPro, easyConnect
import tushare as ts
from utils.logger_config import app_logger as logger
from loguru import logger

def get_daily_trading_data(ts_codes: list[str], start_date: str, end_date: str, turnover = 'turnover_rate') -> pd.DataFrame:
    """
    turnover这个参数可以为turnover_rate，此时返回普通的换手率；
    也可以为turnover_rate_f，此时返回自由流通股的换手率。
    返回的 DataFrame 应包含:
    - ts_code: 股票代码
    - trade_date: 交易日期
    - turnover_rate: 换手率（%）
    - vol: 成交量（手）
    - amount: 成交额（千元）
    - pct_chg: 涨跌幅（%）
    """
    logger.trace(f"正在获取 {len(ts_codes)} 支股票从 {start_date} 到 {end_date} 的交易数据...")
    start_date = start_date.replace('-', '')
    end_date = end_date.replace('-', '')
    pro = easyPro()
    all_data = []
    if turnover not in ['turnover_rate', 'turnover_rate_f']:
        logger.info("换手率传入参数应为turnover_rate或turnover_rate_f,后者为换手率（自由流通股）。已自动更改为turnover_rate")
        turnover = 'turnover_rate'
    for code in ts_codes:
        # 模拟交易数据

        df_1 = pro.daily_basic(ts_code=code, start_date=start_date, end_date=end_date,
                            fields=f"ts_code,trade_date,{turnover}") # requires 2 apis
        df_2 = ts.pro_bar(ts_code=code, start_date=start_date, end_date=end_date,
                          fields="ts_code,trade_date,vol,amount,pct_chg")
        df = pd.merge(left=df_1, right=df_2, on=["ts_code",'trade_date'], how="left")
        all_data.append(df)
    concater = pd.concat(all_data).set_index('trade_date')
    if turnover == 'turnover_rate_f':
        concater.rename(columns={'turnover_rate_f': 'turnover_rate'}, inplace=True)
    return concater


def get_institutional_holdings(ts_codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取机构持股数据的示例函数 (占位符)。
    SORRY我不会写

    返回的 DataFrame 应包含:
    - ts_code: 股票代码
    - end_date: 报告期结束日期
    - inst_hold_ratio: 机构持股比例（%）
    """
    return None


# --- 流动性因子实现 ---

class TurnoverRate20D(FactorBase):
    """
    换手率（20日平均）
    计算公式：日均换手率的20日移动平均
    """

    @property
    def factor_name(self) -> str:
        return "换手率（20日平均）"

    def calculate(self) -> pd.DataFrame:
        # 需要往前多取20个交易日的数据用于计算移动平均
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(days=40)).strftime('%Y-%m-%d')

        # 获取交易数据
        trading_data = get_daily_trading_data(self.ts_codes, start_dt_extended, self.end_date)

        # 透视表获取换手率数据
        turnover_data = trading_data.pivot(columns='ts_code', values='turnover_rate')

        # 计算20日移动平均
        factor_values = turnover_data.rolling(window=20, min_periods=10).mean()

        # 截取最终需要的日期范围
        return factor_values.loc[self.start_date.replace("-", ""):self.end_date.replace("-", "")]


class AmihudIlliquidity(FactorBase):
    """
    Amihud非流动性指标
    计算公式：日均（|收益率| / 成交额）
    """

    @property
    def factor_name(self) -> str:
        return "Amihud非流动性指标"

    def calculate(self) -> pd.DataFrame:
        # 获取交易数据
        trading_data = get_daily_trading_data(self.ts_codes, self.start_date, self.end_date)

        # 计算每日的Amihud指标
        trading_data['amihud_daily'] = trading_data['pct_chg'].abs() / (trading_data['amount'] * 1000)  # 成交额转换为元

        # 透视表
        amihud_daily = trading_data.pivot(columns='ts_code', values='amihud_daily')

        # Amihud指标通常取对数以减少极值影响，并乘以10^6便于观察
        factor_values = np.log(amihud_daily * 1000000 + 1)  # 加1避免取对数时为负无穷

        return factor_values.loc[self.start_date.replace("-", ""):self.end_date.replace("-", "")]


class InstitutionalHoldingChange(FactorBase):
    """
    机构持股比例变化
    计算公式：机构持股比例（季度变化）
    """

    @property
    def factor_name(self) -> str:
        return "机构持股比例变化"

    def calculate(self) -> pd.DataFrame:
        # 需要往前多取一个季度的数据用于计算变化率
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(months=6)).strftime('%Y-%m-%d')

        # 获取机构持股数据
        inst_data = get_institutional_holdings(self.ts_codes, start_dt_extended, self.end_date)

        if inst_data.empty:
            return pd.DataFrame()

        # 确保日期格式正确
        inst_data['end_date'] = pd.to_datetime(inst_data['end_date'])

        # 使用基类提供的方法计算期间变化率
        change_rate_df = self.calculate_period_change_rate_from_long_data(
            data=inst_data,
            value_col='inst_hold_ratio',
            date_col='end_date',
            ts_code_col='ts_code',
            periods=1,
            use_abs_denominator=False  # 机构持股比例变化不需要取绝对值
        )

        if change_rate_df.empty:
            return pd.DataFrame()

        # 透视表转换为宽格式
        factor_values = change_rate_df.pivot(index='end_date', columns='ts_code', values='change_rate')

        # 对于季度数据，需要将其扩展到每个交易日
        # 使用前向填充的方式
        start_date = pd.to_datetime(self.start_date.replace("-", ""))
        end_date = pd.to_datetime(self.end_date.replace("-", ""))
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')

        # 重新索引并前向填充
        factor_values = factor_values.reindex(date_range, method='ffill')

        # 只保留交易日（这里简化处理，实际应该根据交易日历）
        # 过滤掉周末
        factor_values = factor_values[factor_values.index.dayofweek < 5]

        return factor_values.loc[self.start_date.replace("-", ""):self.end_date.replace("-", "")]

