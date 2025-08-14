# src/factor_lab/quality.py

import pandas as pd
import numpy as np
from .base import FactorBase
from utils.utils import easyPro


def get_financial_data(ts_codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取财务数据的示例函数。

    返回的 DataFrame 应包含:
    - ts_code: 股票代码
    - end_date: 报告期
    - total_assets: 总资产
    - total_liab: 总负债
    - total_equity: 股东权益合计
    - n_income: 净利润
    - revenue: 营业收入
    - operate_profit: 营业利润
    - accounts_receiv: 应收账款
    - n_cashflow_act: 经营活动现金流量净额
    """
    print(f"正在获取 {len(ts_codes)} 支股票从 {start_date} 到 {end_date} 的财务数据...")
    start_date = start_date.replace('-', '')
    end_date = end_date.replace('-', '')
    pro = easyPro()
    all_data = []

    for code in ts_codes:
        try:
            # 1. 获取利润表数据
            income_df = pro.income(
                ts_code=code,
                start_date=start_date,
                end_date=end_date,
                fields='ts_code,end_date,revenue,operate_profit,n_income'
            )
            # 2. 获取资产负债表数据
            balance_df = pro.balancesheet(
                ts_code=code,
                start_date=start_date,
                end_date=end_date,
                fields='ts_code,end_date,total_assets,total_liab,total_hldr_eqy_inc_min_int,accounts_receiv'
            )
            # 3. 获取现金流量表数据
            cashflow_df = pro.cashflow(
                ts_code=code,
                start_date=start_date,
                end_date=end_date,
                fields='ts_code,end_date,n_cashflow_act'
            )

            # 合并三张表的数据
            merged_df = pd.merge(income_df, balance_df, on=['ts_code', 'end_date'])
            merged_df = pd.merge(merged_df, cashflow_df, on=['ts_code', 'end_date'])
            # 如果合并后为空，则跳过
            if merged_df.empty:
                continue

            # 重命名列以匹配目标字段
            merged_df.rename(columns={
                'total_hldr_eqy_inc_min_int': 'total_equity'
            }, inplace=True)
            # 强制转换数值列类型，确保concat时数据类型一致
            numeric_cols = [
                'total_assets', 'total_liab', 'total_equity', 'n_income', 'revenue',
                'operate_profit', 'accounts_receiv', 'n_cashflow_act'
            ]
            for col in numeric_cols:
                if col in merged_df.columns:
                    merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')
            # 添加到结果集
            all_data.append(merged_df)

        except Exception as e:
            print(f"获取股票 {code} 数据失败: {str(e)}")
            continue

    # 合并所有股票数据
    if all_data:

        result_df = pd.concat(all_data, ignore_index=True)
        # 数据去重 balancesheet 那个接口获取到的数据有一些重复列
        result_df.drop_duplicates(subset=['ts_code', 'end_date'], keep='last', inplace=True)
        return result_df[['ts_code', 'end_date', 'total_assets', 'total_liab',
                          'total_equity', 'n_income', 'revenue', 'operate_profit',
                          'accounts_receiv', 'n_cashflow_act']]
    else:
        return pd.DataFrame()


def get_daily_basic(ts_codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取每日基本面数据的示例函数。

    返回的 DataFrame 应包含:
    - ts_code: 股票代码
    - trade_date: 交易日期
    - pe: 市盈率
    - pb: 市净率
    - total_mv: 总市值
    """
    print(f"正在获取 {len(ts_codes)} 支股票从 {start_date} 到 {end_date} 的财务数据...")
    start_date = start_date.replace('-', '')
    end_date = end_date.replace('-', '')
    pro = easyPro()
    all_data = []

    for code in ts_codes:
        df = pro.daily_basic(ts_code=code, start_date=start_date, end_date=end_date,
                             fields="ts_code,trade_date,pe,pb,total_mv")
        all_data.append(df)
    all_data = [df for df in all_data if not df.empty]
    if not all_data:
        return pd.DataFrame()
    return pd.concat(all_data, ignore_index=True)


# --- 质量类因子实现 ---

class ROE(FactorBase):
    """
    ROE（净资产收益率）
    计算公式：净利润 / 平均股东权益
    """

    @property
    def factor_name(self) -> str:
        return "ROE"

    def calculate(self) -> pd.DataFrame:
        # 获取财务数据，需要多获取一个季度用于计算平均值
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(months=6)).strftime('%Y-%m-%d')
        end_dt_extended = (pd.to_datetime(self.end_date) + pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        financial_data = get_financial_data(self.ts_codes, start_dt_extended, end_dt_extended)

        if financial_data.empty:
            return pd.DataFrame()

        # 按股票代码分组计算
        result_list = []

        for ts_code, group in financial_data.groupby('ts_code'):
            group_sorted = group.sort_values('end_date').reset_index(drop=True)

            # 计算平均股东权益（当期与上期的平均值）
            group_sorted['prev_equity'] = group_sorted['total_equity'].shift(1)
            group_sorted['avg_equity'] = (group_sorted['total_equity'] + group_sorted['prev_equity']) / 2

            # 当没有上期数据时，使用当期数据
            group_sorted['avg_equity'] = group_sorted['avg_equity'].fillna(group_sorted['total_equity'])

            # 计算ROE = 净利润 / 平均股东权益
            group_sorted['roe'] = group_sorted['n_income'] / group_sorted['avg_equity']

            # 处理异常值
            group_sorted['roe'] = group_sorted['roe'].replace([np.inf, -np.inf], np.nan)

            result_df = group_sorted[['ts_code', 'end_date', 'roe']].copy()
            result_df.columns = ['ts_code', 'trade_date', 'factor_value']
            result_list.append(result_df)

        if not result_list:
            return pd.DataFrame()

        # 合并所有结果
        combined_result = pd.concat(result_list, ignore_index=True)
        combined_result.drop_duplicates(inplace=True)
        # 转换为宽表格式
        wide_result = combined_result.pivot(index='trade_date', columns='ts_code', values='factor_value')

        # 筛选日期范围
        wide_result.index = pd.to_datetime(wide_result.index)
        start_date_dt = pd.to_datetime(self.start_date)
        end_date_dt = pd.to_datetime(self.end_date)
        locer = wide_result.loc[start_date_dt:end_date_dt]
        return locer


class DebtToAssetRatio(FactorBase):
    """
    资产负债率
    计算公式：总负债 / 总资产
    """

    @property
    def factor_name(self) -> str:
        return "资产负债率"

    def calculate(self) -> pd.DataFrame:
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(months=6)).strftime('%Y-%m-%d')
        end_dt_extended = (pd.to_datetime(self.end_date) + pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        financial_data = get_financial_data(self.ts_codes, start_dt_extended, end_dt_extended)

        if financial_data.empty:
            return pd.DataFrame()

        # 计算资产负债率
        financial_data['debt_ratio'] = financial_data['total_liab'] / financial_data['total_assets']
        financial_data['debt_ratio'] = financial_data['debt_ratio'].replace([np.inf, -np.inf], np.nan)

        # 转换为宽表格式
        result_df = financial_data[['ts_code', 'end_date', 'debt_ratio']].copy()
        result_df.columns = ['ts_code', 'trade_date', 'factor_value']

        wide_result = result_df.pivot(index='trade_date', columns='ts_code', values='factor_value')
        wide_result.index = pd.to_datetime(wide_result.index)

        # 筛选日期范围
        start_date_dt = pd.to_datetime(self.start_date)
        end_date_dt = pd.to_datetime(self.end_date)

        return wide_result.loc[start_date_dt:end_date_dt]


class CashFlowToNetIncome(FactorBase):
    """
    经营活动现金流/净利润
    计算公式：经营性现金流净额 / 净利润
    """

    @property
    def factor_name(self) -> str:
        return "经营活动现金流/净利润"

    def calculate(self) -> pd.DataFrame:
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(months=6)).strftime('%Y-%m-%d')
        end_dt_extended = (pd.to_datetime(self.end_date) + pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        financial_data = get_financial_data(self.ts_codes, start_dt_extended, end_dt_extended)

        if financial_data.empty:
            return pd.DataFrame()

        # 计算经营活动现金流/净利润
        financial_data['cf_to_ni'] = financial_data['n_cashflow_act'] / financial_data['n_income']
        financial_data['cf_to_ni'] = financial_data['cf_to_ni'].replace([np.inf, -np.inf], np.nan)

        # 转换为宽表格式
        result_df = financial_data[['ts_code', 'end_date', 'cf_to_ni']].copy()
        result_df.columns = ['ts_code', 'trade_date', 'factor_value']

        wide_result = result_df.pivot(index='trade_date', columns='ts_code', values='factor_value')
        wide_result.index = pd.to_datetime(wide_result.index)

        # 筛选日期范围
        start_date_dt = pd.to_datetime(self.start_date)
        end_date_dt = pd.to_datetime(self.end_date)

        return wide_result.loc[start_date_dt:end_date_dt]


class AccountsReceivableTurnover(FactorBase):
    """
    应收账款周转率
    计算公式：营业收入 / 平均应收账款余额
    """

    @property
    def factor_name(self) -> str:
        return "应收账款周转率"

    def calculate(self) -> pd.DataFrame:
        # 获取财务数据，需要多获取一个季度用于计算平均值
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(months=6)).strftime('%Y-%m-%d')
        end_dt_extended = (pd.to_datetime(self.end_date) + pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        financial_data = get_financial_data(self.ts_codes, start_dt_extended, end_dt_extended)

        if financial_data.empty:
            return pd.DataFrame()

        # 按股票代码分组计算
        result_list = []

        for ts_code, group in financial_data.groupby('ts_code'):
            group_sorted = group.sort_values('end_date').reset_index(drop=True)

            # 计算平均应收账款余额（当期与上期的平均值）
            group_sorted['prev_accounts_receiv'] = group_sorted['accounts_receiv'].shift(1)
            group_sorted['avg_accounts_receiv'] = (group_sorted['accounts_receiv'] + group_sorted[
                'prev_accounts_receiv']) / 2

            # 当没有上期数据时，使用当期数据
            group_sorted['avg_accounts_receiv'] = group_sorted['avg_accounts_receiv'].fillna(group_sorted['accounts_receiv'])

            # 计算应收账款周转率 = 营业收入 / 平均应收账款余额
            group_sorted['ar_turnover'] = group_sorted['revenue'] / group_sorted['avg_accounts_receiv']

            # 处理异常值
            group_sorted['ar_turnover'] = group_sorted['ar_turnover'].replace([np.inf, -np.inf], np.nan)

            result_df = group_sorted[['ts_code', 'end_date', 'ar_turnover']].copy()
            result_df.columns = ['ts_code', 'trade_date', 'factor_value']
            result_list.append(result_df)

        if not result_list:
            return pd.DataFrame()

        # 合并所有结果
        combined_result = pd.concat(result_list, ignore_index=True)

        # 转换为宽表格式
        wide_result = combined_result.pivot(index='trade_date', columns='ts_code', values='factor_value')
        wide_result.index = pd.to_datetime(wide_result.index)

        # 筛选日期范围
        start_date_dt = pd.to_datetime(self.start_date)
        end_date_dt = pd.to_datetime(self.end_date)

        return wide_result.loc[start_date_dt:end_date_dt]


class OperatingProfitMargin(FactorBase):
    """
    营业利润率
    计算公式：营业利润 / 营业收入
    """

    @property
    def factor_name(self) -> str:
        return "营业利润率"

    def calculate(self) -> pd.DataFrame:
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(months=6)).strftime('%Y-%m-%d')
        end_dt_extended = (pd.to_datetime(self.end_date) + pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        financial_data = get_financial_data(self.ts_codes, start_dt_extended, end_dt_extended)

        if financial_data.empty:
            return pd.DataFrame()

        # 计算营业利润率
        financial_data['op_margin'] = financial_data['operate_profit'] / financial_data['revenue']
        financial_data['op_margin'] = financial_data['op_margin'].replace([np.inf, -np.inf], np.nan)

        # 转换为宽表格式
        result_df = financial_data[['ts_code', 'end_date', 'op_margin']].copy()
        result_df.columns = ['ts_code', 'trade_date', 'factor_value']

        wide_result = result_df.pivot(index='trade_date', columns='ts_code', values='factor_value')
        wide_result.index = pd.to_datetime(wide_result.index)

        # 筛选日期范围
        start_date_dt = pd.to_datetime(self.start_date)
        end_date_dt = pd.to_datetime(self.end_date)

        return wide_result.loc[start_date_dt:end_date_dt]
