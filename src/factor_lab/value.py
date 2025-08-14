# src/factor_lab/value.py

import pandas as pd
import numpy as np
from .base import FactorBase
from utils.utils import easyPro
from utils.logger_config import app_logger as logger
from loguru import logger


def get_daily_basic_data(ts_codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取日线基本指标。这是计算价值类因子的主要数据源。

    Tushare API: pro.daily_basic()

    需要字段:
    - ts_code: 股票代码
    - trade_date: 交易日期
    - total_mv: 当日总市值（万元）
    - pe_ttm: 市盈率 (TTM)
    - pb: 市净率
    - ps_ttm: 市销率 (TTM)
    - dv_ttm: 股息率 (TTM, %)
    """

    # df = pro.daily_basic(ts_code=','.join(ts_codes), start_date=start_date.replace('-',''), end_date=end_date.replace('-',''),
    #                      fields='ts_code,trade_date,total_mv,pe_ttm,pb,ps_ttm,dv_ttm')
    # return df
    logger.trace(f"正在获取 {len(ts_codes)} 支股票从 {start_date} 到 {end_date} 的日线基本指标...")
    start_date = start_date.replace('-', '')
    end_date = end_date.replace('-', '')
    pro = easyPro()
    all_data = []
    for code in ts_codes:
        df = pro.daily_basic(ts_code=code, start_date=start_date, end_date=end_date,
                             fields=['ts_code', 'trade_date', 'total_mv', 'pe_ttm', 'pb', 'ps_ttm', 'dv_ttm'])
        all_data.append(df)
    return pd.concat(all_data, ignore_index=True)


def get_financial_data(ts_codes: list[str], start_date: str, report_type: str) -> pd.DataFrame:
    """
    获取季度的财务报表数据，用于计算 EV/EBITDA。

    Tushare API:
    - `fina_indicator`: 获取财务指标数据 (EBITDA)
    - `balancesheet`: 获取资产负债表数据 (有息负债、现金)

    fina_indicator需要获取：
    'end_date'
    'ts_code'
    'ebitda' # 息税折旧摊销前利润
    balancesheet需要获取：
    'end_date'
    'ts_code'
    'monetary_cap' # 现金
    'st_borr' # 短期借款
    'non_cur_liab_due_1y' # 一年内到期的非流动负债
    'lt_borr' # 长期借款
    'bonds_payable' # 应付债券

    Args:
        ts_codes (list[str]): 股票代码列表。
        start_date (str): 为了能覆盖计算周期初期的财报，通常需要从计算起始日往前推至少1年。
        report_type (str): 'fina_indicator' 或 'balancesheet'
    """


    logger.trace(f"正在获取 {len(ts_codes)} 支股票自 {start_date} 以来的 {report_type} 数据...")
    start_date = start_date.replace('-', '')
    pro = easyPro()
    all_data = []
    if report_type == 'fina_indicator':
        for code in ts_codes:
            df = pro.fina_indicator(ts_code=code, start_date=start_date,
                                    fields='ts_code,end_date,ebitda')
            all_data.append(df)
    elif report_type == 'balancesheet':
        for code in ts_codes:
            df = pro.balancesheet(ts_code=code, start_date=start_date,
                                 fields='ts_code,end_date,money_cap,st_borr,non_cur_liab_due_1y,lt_borr,bond_payable')
            df.rename(columns={'money_cap':'monetary_cap', 'bond_payable':'bonds_payable'}, inplace=True)
            all_data.append(df)
    concater = pd.concat(all_data)
    return pd.concat(all_data)


# =============================================================================
# 价值类因子 (Value Factors)
#
# 说明:
# 对于PE, PB, PS, DY，Tushare的 `daily_basic` 接口已经提供了计算好的TTM值，
# 直接使用这些值是最高效、最准确的做法。
# 对于 EV/EBITDA，由于没有现成数据，我们需要手动合成。
# =============================================================================

class PE(FactorBase):
    """因子1：市盈率（PE）"""

    @property
    def factor_name(self) -> str:
        return "市盈率（PE）"

    def calculate(self) -> pd.DataFrame:
        # 直接从 Tushare 的日线基本指标中获取 pe_ttm
        daily_basic = get_daily_basic_data(self.ts_codes, self.start_date, self.end_date)

        # 将数据从窄表转换为宽表
        pe_wide = daily_basic.pivot(index='trade_date', columns='ts_code', values='pe_ttm')

        # 统一日期格式并排序
        pe_wide.index = pd.to_datetime(pe_wide.index)
        pe_wide = pe_wide.sort_index()

        # 进行切片
        start_dt_str = self.start_date.replace("-", "")
        end_dt_str = self.end_date.replace("-", "")
        return pe_wide.loc[start_dt_str:end_dt_str]


class PB(FactorBase):
    """因子2：市净率（PB）"""

    @property
    def factor_name(self) -> str:
        return "市净率（PB）"

    def calculate(self) -> pd.DataFrame:
        # 直接获取 pb
        daily_basic = get_daily_basic_data(self.ts_codes, self.start_date, self.end_date)
        pb_wide = daily_basic.pivot(index='trade_date', columns='ts_code', values='pb')
        pb_wide.index = pd.to_datetime(pb_wide.index)
        pb_wide = pb_wide.sort_index()

        start_dt_str = self.start_date.replace("-", "")
        end_dt_str = self.end_date.replace("-", "")
        return pb_wide.loc[start_dt_str:end_dt_str]


class PS(FactorBase):
    """因子4：市销率（PS）"""

    @property
    def factor_name(self) -> str:
        return "市销率（PS）"

    def calculate(self) -> pd.DataFrame:
        # 直接获取 ps_ttm
        daily_basic = get_daily_basic_data(self.ts_codes, self.start_date, self.end_date)
        ps_wide = daily_basic.pivot(index='trade_date', columns='ts_code', values='ps_ttm')
        ps_wide.index = pd.to_datetime(ps_wide.index)
        ps_wide = ps_wide.sort_index()

        start_dt_str = self.start_date.replace("-", "")
        end_dt_str = self.end_date.replace("-", "")
        return ps_wide.loc[start_dt_str:end_dt_str]


class DY(FactorBase):
    """因子5：股息率（DY）"""

    @property
    def factor_name(self) -> str:
        return "股息率（DY）"

    def calculate(self) -> pd.DataFrame:
        # 直接获取 dv_ttm (TTM股息率)
        daily_basic = get_daily_basic_data(self.ts_codes, self.start_date, self.end_date)
        dy_wide = daily_basic.pivot(index='trade_date', columns='ts_code', values='dv_ttm')
        dy_wide.index = pd.to_datetime(dy_wide.index)
        dy_wide = dy_wide.sort_index()

        start_dt_str = self.start_date.replace("-", "")
        end_dt_str = self.end_date.replace("-", "")
        return dy_wide.loc[start_dt_str:end_dt_str]


class EVEBITDA(FactorBase):
    """因子3：企业价值倍数（EV/EBITDA）"""

    @property
    def factor_name(self) -> str:
        return "企业价值倍数（EV/EBITDA）"

    def calculate(self) -> pd.DataFrame:
        # 1. 获取日度总市值数据
        daily_basic = get_daily_basic_data(self.ts_codes, self.start_date, self.end_date)
        market_cap = daily_basic.pivot(index='trade_date', columns='ts_code', values='total_mv')
        market_cap.index = pd.to_datetime(market_cap.index)
        market_cap = market_cap.sort_index()

        # 2. 获取季度财务数据
        fin_start_date = (pd.to_datetime(self.start_date) - pd.DateOffset(months=6)).strftime('%Y-%m-%d')

        # 获取EBITDA
        fina_indicator = get_financial_data(self.ts_codes, fin_start_date, 'fina_indicator')
        fina_indicator['end_date'] = pd.to_datetime(fina_indicator['end_date'].astype(str))  # 转换为datetime对象

        # 在 pivot 之前，对关键列去重，每个报告期只保留最后一条记录
        fina_indicator.drop_duplicates(subset=['ts_code', 'end_date'], keep='last', inplace=True)


        ebitda = fina_indicator.pivot(index='end_date', columns='ts_code', values='ebitda')

        # 获取并计算有息负债和现金
        balancesheet = get_financial_data(self.ts_codes, fin_start_date, 'balancesheet')
        balancesheet['end_date'] = pd.to_datetime(balancesheet['end_date'].astype(str))  # 转换为datetime对象

        # 同样对资产负债表数据进行去重处理
        balancesheet.drop_duplicates(subset=['ts_code', 'end_date'], keep='last', inplace=True)


        # 计算有息负债 (Interest Bearing Debt)
        debt_cols = ['st_borr', 'non_cur_liab_due_1y', 'lt_borr', 'bonds_payable']
        # 先填充NaN为0，避免求和时产生NaN
        balancesheet[debt_cols] = balancesheet[debt_cols].fillna(0)
        balancesheet['interest_debt'] = balancesheet[debt_cols].sum(axis=1)

        interest_debt = balancesheet.pivot(index='end_date', columns='ts_code', values='interest_debt')
        cash = balancesheet.pivot(index='end_date', columns='ts_code', values='monetary_cap')

        # 3. 合并日度数据和季度数据
        aligned_df = pd.DataFrame(index=ebitda.index)

        aligned_ebitda = pd.merge(aligned_df, ebitda, left_index=True, right_index=True, how='left').ffill()
        aligned_interest_debt = pd.merge(aligned_df, interest_debt, left_index=True, right_index=True,
                                             how='left').ffill()
        aligned_cash = pd.merge(aligned_df, cash, left_index=True, right_index=True, how='left').ffill()

        # 为了安全，重新对齐一下列，防止股票顺序不一致
        aligned_ebitda = aligned_ebitda.reindex(columns=market_cap.columns)
        aligned_interest_debt = aligned_interest_debt.reindex(columns=market_cap.columns)
        aligned_cash = aligned_cash.reindex(columns=market_cap.columns)

        # 4. 计算 EV 和 EV/EBITDA
        # 由于market_cap没有节假日，两个aligned覆盖面有很少（只有季度末有数据），所以先把market_cap填满
        # 可恶！！！  在这卡了好久
        all_dates = pd.date_range(start=pd.to_datetime(self.start_date), end=pd.to_datetime(self.end_date), freq='D')
        market_cap = market_cap.reindex(index=all_dates)
        market_cap = market_cap.ffill()
        ev = market_cap * 10000 + aligned_interest_debt - aligned_cash
        # 在计算除法前，将所有不符合条件的 EBITDA (小于等于0) 的值设为 NaN
        # 这使得 EV/EBITDA 对于亏损或EBITDA为0的公司自然地成为 NaN
        aligned_ebitda[aligned_ebitda <= 0] = np.nan

        ev_ebitda = ev / aligned_ebitda
        ev_ebitda.replace([np.inf, -np.inf], np.nan, inplace=True)

        # 5. 截取最终需要的日期范围
        start_dt_str = self.start_date.replace("-", "")
        end_dt_str = self.end_date.replace("-", "")
        return ev_ebitda.loc[start_dt_str:end_dt_str]
