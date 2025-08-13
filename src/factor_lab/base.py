# src/factor_lab/base.py

import abc
import pandas as pd
from sqlalchemy.engine import Engine
from utils.utils import easyConnect, upsert_to_mysql
import numpy as np


class FactorBase(abc.ABC):
    """
    因子计算抽象基类

    所有具体的因子类都应继承自该类，并实现指定的抽象方法。
    """

    def __init__(self, ts_codes: list[str], start_date: str, end_date: str):
        """
        初始化因子计算所需的基本参数。

        Args:
            ts_codes (list[str]): 需要计算因子的股票代码列表。
            start_date (str): 计算区间的开始日期 (e.g., '2023-01-01')。
            end_date (str): 计算区间的结束日期 (e.g., '2023-12-31')。
        """
        self.ts_codes = ts_codes
        self.start_date = start_date
        self.end_date = end_date
        self.engine = easyConnect()  # 初始化数据库连接

    @property
    @abc.abstractmethod
    def factor_name(self) -> str:
        """
        抽象属性：返回因子的中文名称，必须与数据库中的 factor_name 一致。
        """
        raise NotImplementedError("每个因子类必须定义 factor_name 属性。")

    @abc.abstractmethod
    def calculate(self) -> pd.DataFrame:
        """
        抽象方法：核心计算逻辑。

        该方法需要被每个具体的因子类重写。
        它应当获取所需数据，执行计算，并返回一个 "宽表" 格式的 DataFrame。

        Returns:
            pd.DataFrame:
                - index 为交易日期 (pd.DatetimeIndex)
                - columns 为股票代码 (ts_code)
                - values 为对应日期的因子值
        """
        raise NotImplementedError("每个因子类必须实现 calculate() 方法。")

    def to_narrow_format(self, wide_df: pd.DataFrame) -> pd.DataFrame:
        """
        将计算结果从宽表格式转换为窄表格式，以方便入库。

        Args:
            wide_df (pd.DataFrame): calculate() 方法返回的宽表 DataFrame。

        Returns:
            pd.DataFrame: 包含 ['ts_code', 'trade_date', 'factor_name', 'factor_value'] 列的窄表。
        """
        if wide_df.empty:
            return pd.DataFrame(columns=['ts_code', 'trade_date', 'factor_name', 'factor_value'])

        narrow_df = wide_df.stack().reset_index()
        narrow_df.columns = ['trade_date', 'ts_code', 'factor_value']
        narrow_df['factor_name'] = self.factor_name

        # 调整列序以匹配数据库表结构
        return narrow_df[['ts_code', 'trade_date', 'factor_name', 'factor_value']]

    def save_to_db(self, table_name:str="", create_sql:str="", echo=False):
        """
        执行因子计算并将结果保存到数据库。

        这是一个完整的“计算到存储”的流程。
        """
        print(f"开始计算因子: {self.factor_name}...")

        # 1. 执行计算
        wide_factor_values = self.calculate()

        if wide_factor_values.empty:
            print(f"因子 {self.factor_name} 计算结果为空，跳过入库。")
            return

        # 2. 转换为窄表
        narrow_factor_values = self.to_narrow_format(wide_factor_values)

        # 3. 数据清洗，去除无效值
        narrow_factor_values.dropna(subset=['factor_value'], inplace=True)

        if narrow_factor_values.empty:
            print(f"因子 {self.factor_name} 清洗后结果为空，跳过入库。")
            return

        print(f"计算完成，准备将 {len(narrow_factor_values)} 条数据写入数据库...")

        # 4. 调用 upsert 函数写入数据库
        # 注意：对于因子值表，主键应当是 ts_code, trade_date 和 factor_name 的组合，
        # 这样才能唯一确定一条记录。
        upsert_to_mysql(
            engine=self.engine,
            table_name=table_name,
            df_uncleaned=narrow_factor_values,
            primary_key=['ts_code', 'trade_date', 'factor_name'],
            create_sql_command=create_sql,
            echo=echo
        )
        print(f"因子 {self.factor_name} 数据同步完成。")

    def calculate_period_change_rate(self, data: pd.DataFrame, periods: int = 1,
                                     use_abs_denominator: bool = False) -> pd.DataFrame:
        """
        计算期间变化率：(本期值 - 上期值) / 上期值 或 (本期值 - 上期值) / |上期值|

        适用于计算各种财务指标的同比/环比变化率，如：
        - 股东户数变化率
        - 融资余额变化率
        - 净利润变化率
        - 毛利润变化率
        - 营业收入变化率
        - 购建固定资产现金支出变化率等

        Args:
            data (pd.DataFrame): 原始数据，index为日期，columns为股票代码
            periods (int): 期间间隔，默认为1（上一期）
            use_abs_denominator (bool): 是否对分母取绝对值，默认False
                                      - False: (本期 - 上期) / 上期
                                      - True:  (本期 - 上期) / |上期|

        Returns:
            pd.DataFrame: 计算后的变化率数据，格式与输入数据相同

        Examples:
            # 股东户数变化率（不取绝对值）
            shareholder_change = self.calculate_period_change_rate(shareholder_data, periods=1, use_abs_denominator=False)

            # 净利润变化率（分母取绝对值）
            profit_change = self.calculate_period_change_rate(profit_data, periods=1, use_abs_denominator=True)
        """
        if data.empty:
            return pd.DataFrame()

        # 确保数据按日期排序
        data_sorted = data.sort_index()

        # 计算上期值
        previous_data = data_sorted.shift(periods)

        # 计算分子：本期 - 上期
        numerator = data_sorted - previous_data

        # 计算分母：上期值或|上期值|
        denominator = previous_data.abs() if use_abs_denominator else previous_data

        # 计算变化率，避免除零
        change_rate = numerator / denominator

        # 处理无穷大和NaN值
        change_rate = change_rate.replace([np.inf, -np.inf], np.nan)

        return change_rate


    def calculate_period_change_rate_from_long_data(self, data: pd.DataFrame,
                                                    value_col: str,
                                                    date_col: str = 'trade_date',
                                                    ts_code_col: str = 'ts_code',
                                                    periods: int = 1,
                                                    use_abs_denominator: bool = False) -> pd.DataFrame:
        """
        从长格式（窄表）数据计算期间变化率：(本期值 - 上期值) / 上期值 或 (本期值 - 上期值) / |上期值|

        适用于处理包含ts_code, end_date, value列的长格式数据，如：
        - 股东户数变化率
        - 融资余额变化率
        - 净利润变化率等

        Args:
            data (pd.DataFrame): 长格式原始数据，包含ts_code, date, value列
            value_col (str): 数值列名
            date_col (str): 日期列名，默认'trade_date'
            ts_code_col (str): 股票代码列名，默认'ts_code'
            periods (int): 期间间隔，默认为1（上一期）
            use_abs_denominator (bool): 是否对分母取绝对值，默认False

        Returns:
            pd.DataFrame: 包含ts_code, end_date, change_rate列的计算结果

        Examples:
            # 股东户数变化率
            result = self.calculate_period_change_rate_from_long_data(
                data=shareholder_df,
                date_col='end_date',
                value_col='holder_num',
                use_abs_denominator=False
            )
        """
        if data.empty:
            return pd.DataFrame(columns=[ts_code_col, date_col, 'change_rate'])

        # 确保日期列为datetime类型
        data = data.copy()
        data[date_col] = pd.to_datetime(data[date_col])

        # 按股票代码和日期排序
        data_sorted = data.sort_values([ts_code_col, date_col])

        # 按股票代码分组计算变化率
        result_list = []

        for ts_code, group in data_sorted.groupby(ts_code_col):
            # 对每个股票的数据按日期排序
            group_sorted = group.sort_values(date_col).reset_index(drop=True)

            # 计算上期值
            group_sorted['previous_value'] = group_sorted[value_col].shift(periods)

            # 计算分子：本期 - 上期
            numerator = group_sorted[value_col] - group_sorted['previous_value']

            # 计算分母：上期值或|上期值|
            denominator = group_sorted['previous_value'].abs() if use_abs_denominator else group_sorted['previous_value']

            # 计算变化率，避免除零
            change_rate = numerator / denominator

            # 处理无穷大和NaN值
            change_rate = change_rate.replace([np.inf, -np.inf], np.nan)

            # 构建结果
            result_df = pd.DataFrame({
                ts_code_col: ts_code,
                date_col: group_sorted[date_col],
                'change_rate': change_rate
            })

            result_list.append(result_df)

        # 合并所有结果
        if result_list:
            final_result = pd.concat(result_list, ignore_index=True)
            # 删除NaN行（通常是每个股票的第一行，因为没有上期数据）
            final_result = final_result.dropna(subset=['change_rate'])
            return final_result
        else:
            return pd.DataFrame(columns=[ts_code_col, date_col, 'change_rate'])
