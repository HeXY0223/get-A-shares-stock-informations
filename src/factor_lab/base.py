# src/factor_lab/base.py

import abc
import pandas as pd
from sqlalchemy.engine import Engine

# 假设你的数据库工具函数位于 src/utils/utils.py
from utils.utils import easyConnect, upsert_to_mysql


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

    def save_to_db(self, table_name:str="", echo=False):
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
            echo=echo
        )
        print(f"因子 {self.factor_name} 数据同步完成。")

