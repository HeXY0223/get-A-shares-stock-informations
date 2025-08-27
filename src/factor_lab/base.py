# src/factor_lab/base.py

import abc
import pandas as pd
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from utils.utils import easyConnect, upsert_to_mysql
import numpy as np
from utils.logger_config import app_logger as logger
from loguru import logger
from data_fetchers.base import FetcherBase


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

    def save_to_db(self, table_name:str="", create_sql:str=""):
        """
        执行因子计算并将结果保存到数据库。

        这是一个完整的“计算到存储”的流程。
        """
        logger.debug(f"开始计算因子: {self.factor_name}...")

        # 1. 执行计算
        wide_factor_values = self.calculate()

        if wide_factor_values.empty:
            logger.warning(f"因子 {self.factor_name} 计算结果为空，跳过入库。")
            return

        # 2. 转换为窄表
        narrow_factor_values = self.to_narrow_format(wide_factor_values)

        # 3. 数据清洗，去除无效值
        narrow_factor_values.dropna(subset=['factor_value'], inplace=True)

        if narrow_factor_values.empty:
            logger.warning(f"因子 {self.factor_name} 清洗后结果为空，跳过入库。")
            return

        logger.debug(f"计算完成，准备将 {len(narrow_factor_values)} 条数据写入数据库...")

        # 4. 调用 upsert 函数写入数据库
        # 注意：对于因子值表，主键应当是 ts_code, trade_date 和 factor_name 的组合，
        # 这样才能唯一确定一条记录。
        upsert_to_mysql(
            engine=self.engine,
            table_name=table_name,
            df_uncleaned=narrow_factor_values,
            primary_key=['ts_code', 'trade_date', 'factor_name'],
            create_sql_command=create_sql,
        )
        logger.info(f"因子 {self.factor_name} 数据同步完成。")

    def add(self, df, add1: str, add2: str, result: str = ""):
        if result:
            df[result] = df[add1] + df[add2]
        else:
            print(f'{add1}_add_{add2}')
            df[f'{add1}_add_{add2}'] = df[add1] + df[add2]

    def sub(self, df, sub1: str, sub2: str, result: str = ""):
        if result:
            df[result] = df[sub1] - df[sub2]
        else:
            print(f'{sub1}_sub_{sub2}')
            df[f'{sub1}_sub_{sub2}'] = df[sub1] - df[sub2]

    def mul(self, df, mul1: str, mul2: str, result: str = ""):
        if result:
            df[result] = df[mul1] * df[mul2]
        else:
            print(f'{mul1}_mul_{mul2}')
            df[f'{mul1}_mul_{mul2}'] = df[mul1] * df[mul2]

    def div(self, df, div1: str, div2: str, result: str = ""):
        if result:
            df[result] = df[div1] / df[div2]
        else:
            print(f'{div1}_div_{div2}')
            df[f'{div1}_div_{div2}'] = df[div1] / df[div2]

    def changedate(self, origin_date, days: int):
        return (pd.to_datetime(origin_date) + pd.DateOffset(days=days)).strftime('%Y-%m-%d')

    def calculate_period_change_rate(self, data: pd.DataFrame,
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

    def fetch_data(self, queries: list, start_date=None, end_date=None):
        """
        从统一的窄表 'extra_data' 中获取数据，并能自动查漏补缺。
        params:
        queries: FetcherBase中所要求的query，举例如下
        queries=[{'api':'daily_basic', 'fields':'close, pe, pb, ps, pb_ttm'}, # pb_ttm doesn't exist
         {'api':'stock_basic','fields':'name,symbol'},
         {'api':'stock_company','fields':'reg_capital,province'},
         {'api':'pro_bar','fields':'open,high,low,vol','adj':'qfq'}]

        工作流程:
        1. 根据 ts_codes 和日期范围，从数据库 'extra_data' 表中查询所有需要的字段。
        2. 构建一个“完整”的 (trade_date, ts_code) 索引网格。
        3. 将从数据库查到的数据与完整网格对比，找出缺失的 (trade_date, ts_code) 组合。
        4. 如果存在缺失，则只为缺失的股票代码启动一次在线获取。
        5. 将在线获取的新数据存入数据库，并与本地数据合并。
        6. 返回一张完整、干净的宽表数据。

        注意：
        1. start_date, end_date不传参的时候默认是self.start_date 和 self.end_date
        2. 当需要访问返回end_date的报表类数据的时候，记得在query里面加上end_date
        3. 如果请求了复权行情，有复权的值会加上_qfq或者_hfq的后缀
        """
        start_date = self.start_date if not start_date else pd.to_datetime(start_date).strftime('%Y%m%d')
        end_date = self.end_date if not end_date else pd.to_datetime(end_date).strftime('%Y%m%d')
        logger.info(f"开始为因子 {self.__class__.__name__} 获取数据，将自动查漏补缺。")
        all_fields_needed = set()
        for query in queries:
            fields_list = [f.strip() for f in query['fields'].split(',')]
            adj_suffix = f"_{query.get('adj', '')}" if query.get('adj') in ['qfq', 'hfq'] else ""
            if adj_suffix:
                targets = ['open', 'high', 'low', 'close', 'change', 'pre_close']
                all_fields_needed.update([f + adj_suffix if f in targets else f for f in fields_list])
            else:
                targets = ['change', 'pre_close']
                all_fields_needed.update([f"{f}_qfq" if f in targets else f for f in fields_list])
            if 'change_qfq' in all_fields_needed:
                all_fields_needed.remove('change_qfq')
                all_fields_needed.add('price_change_qfq')
            if 'change_hfq' in all_fields_needed:
                all_fields_needed.remove('change_hfq')
                all_fields_needed.add('price_change_hfq')

        # --- 步骤 1: 尽力从数据库获取所有数据 ---
        df_from_db = pd.DataFrame()
        try:
            field_placeholders = ', '.join([f"'{f}'" for f in all_fields_needed])
            ts_code_placeholders = ', '.join([f"'{c}'" for c in self.ts_codes])

            sql_fetch = (f"SELECT ts_code, trade_date, data_name, data_value FROM extra_data "
                         f"WHERE data_name IN ({field_placeholders}) "
                         f"AND trade_date BETWEEN '{start_date}' AND '{end_date}' "
                         f"AND ts_code IN ({ts_code_placeholders})")

            df_narrow = pd.read_sql(sql_fetch, self.engine)

            if not df_narrow.empty:
                df_from_db = df_narrow.pivot_table(
                    index=['trade_date', 'ts_code'], columns='data_name', values='data_value', aggfunc='first'
                )
                df_from_db.index = df_from_db.index.set_levels(pd.to_datetime(df_from_db.index.levels[0]),
                                                               level='trade_date')
                logger.debug(
                    f"从数据库成功加载 {len(df_from_db)} 条记录，涉及 {df_from_db.index.get_level_values('ts_code').nunique()} 只股票。")
        except Exception as e:
            logger.warning(f"从数据库 'extra_data' 查询数据时出错: {e}。将尝试完全在线获取。")

        # --- 步骤 2 & 3: 构建完整网格，识别缺失的股票 ---
        trade_dates = pd.date_range(start=start_date, end=end_date, freq='D')

        # 识别哪些股票的数据完全没有
        # 即使某股票只有部分日期的数据，这里也会认为它是“已存在”的
        found_ts_codes = set()
        if not df_from_db.empty:
            found_ts_codes = set(df_from_db.index.get_level_values('ts_code').unique())

        missing_ts_codes = list(set(self.ts_codes) - found_ts_codes)

        # --- 步骤 4: 如果有缺失，则进行针对性的在线获取 ---
        if missing_ts_codes:
            logger.info(f"发现 {len(missing_ts_codes)} 只股票的数据在数据库中完全缺失，准备在线获取: {missing_ts_codes}")

            online_fetcher = FetcherBase(start_date=start_date, end_date=end_date,
                                         ts_codes=missing_ts_codes, queries=queries)
            df_online_narrow = online_fetcher.fetch()

            if not df_online_narrow.empty:
                logger.debug(f"在线获取了 {len(df_online_narrow)} 条新数据，准备存入数据库并合并...")
                # 步骤 5: 存入数据库以备后用
                upsert_to_mysql(engine=self.engine, table_name='extra_data', df_uncleaned=df_online_narrow,
                                primary_key=['ts_code', 'trade_date', 'data_name'], create_sql_command='auto')

                # 将新数据转换为宽表
                df_online_wide = df_online_narrow.pivot_table(
                    index=['trade_date', 'ts_code'], columns='data_name', values='data_value', aggfunc='first'
                )
                df_online_wide.index = df_online_wide.index.set_levels(pd.to_datetime(df_online_wide.index.levels[0]),
                                                                       level='trade_date')

                # 合并到主DataFrame中
                df_merged = pd.concat([df_from_db, df_online_wide])
            else:
                logger.warning(f"尝试为缺失股票在线获取数据，但未返回任何内容。")
                df_merged = df_from_db
        else:
            logger.info("数据库数据完整，无需在线获取。")
            df_merged = df_from_db

        # --- 步骤 6: 返回最终结果 ---
        if df_merged.empty:
            logger.warning(f"最终未能获取任何数据，返回空 DataFrame。")
            return pd.DataFrame()

        # 重新索引以确保所有股票和日期都在 DataFrame 中，缺失值用 NaN 表示
        # 这对于需要对齐操作的因子计算至关重要
        df_merged = df_merged.copy()

        df_final = df_merged.reset_index(drop=False)
        #complete_index = pd.MultiIndex.from_product(
        #    [pd.date_range(start=start_date, end=end_date), self.ts_codes],
        #    names=['trade_date', 'ts_code']
        #)
        #df_final = df_merged.reindex(complete_index)
#
        #df_final.sort_index(inplace=True)
        logger.success(f"数据获取完成，返回一个包含 {len(df_final)} 行、{len(df_final.columns)} 个字段的完整宽表。")

        return df_final


