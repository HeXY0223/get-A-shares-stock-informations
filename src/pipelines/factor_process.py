import pandas as pd
import numpy as np
from sqlalchemy.engine import Engine
from utils.utils import easyConnect, upsert_to_mysql  # 假设您的utils里有这些函数
from utils.factor_processing import winsorize, standardize, neutralize
from loguru import logger


class FactorProcessor:
    def __init__(self, start_date: str, end_date: str, table_raw: str, table_processed: str, create_sql_processed: str):
        """
        初始化因子处理流水线。

        Args:
            start_date (str): 处理的开始日期。
            end_date (str): 处理的结束日期。
            table_raw (str): 原始因子数据表名。
            table_processed (str): 处理后因子数据要存入的表名。
            create_sql_processed (str): 创建处理后表的SQL语句
        """
        self.start_date = start_date
        self.end_date = end_date
        self.engine = easyConnect()
        self.table_raw = table_raw
        self.table_processed = table_processed
        self.create_sql_processed = create_sql_processed

    def _fetch_daily_data(self, trade_date: str) -> pd.DataFrame:
        """为单个交易日获取所有需要的数据，并整合成一个宽表。"""
        logger.debug(f"正在为 {trade_date} 获取数据...")

        # 1. 获取当天的所有原始因子值
        sql_factors = f"SELECT ts_code, factor_name, factor_value FROM {self.table_raw} WHERE trade_date = '{trade_date}'"
        raw_factors_df = pd.read_sql(sql_factors, self.engine)
        if raw_factors_df.empty:
            return pd.DataFrame()
        # 将窄表转换为宽表
        factor_wide_df = raw_factors_df.pivot(index='ts_code', columns='factor_name', values='factor_value')

        # 2. 获取当天的中性化所需风险因子（市值、行业等）
        # 假设您有 'daily_basics' 表存市值，'stock_basics' 表存行业
        trade_date_str = trade_date.replace('-', '')
        daily_basics = 'temp_data'
        sql_risk = f"""
            SELECT db.ts_code, db.total_mv, sb.l1_name 
            FROM {daily_basics} db
            LEFT JOIN sw_category sb ON db.ts_code = sb.ts_code
            WHERE db.trade_date = '{trade_date_str}'
        """
        risk_df = pd.read_sql(sql_risk, self.engine).set_index('ts_code')

        # 对市值取对数
        risk_df['log_mv'] = np.log(risk_df['total_mv'])
        # 将行业数据转换为虚拟变量
        industry_dummies = pd.get_dummies(risk_df['l1_name'], prefix='ind', dtype=int)

        risk_exposure_df = pd.concat([risk_df[['log_mv']], industry_dummies], axis=1)

        # 3. 合并所有数据
        daily_cross_section = factor_wide_df.join(risk_exposure_df, how='inner')

        return daily_cross_section

    def process(self):
        """执行整个处理流程。"""
        trade_dates = pd.date_range(start=self.start_date, end=self.end_date, freq='B')  # 获取交易日历

        all_processed_data = []

        for trade_date in trade_dates:
            date_str = trade_date.strftime('%Y-%m-%d')
            logger.info(f"开始处理 {date_str} 的数据...")

            # 1. 获取当天的截面数据
            daily_data = self._fetch_daily_data(date_str)
            if daily_data.empty:
                logger.warning(f"{date_str} 无数据，跳过。")
                continue

            # 2. 准备风险敞口矩阵和因子列名
            factor_columns = [col for col in daily_data.columns if not col.startswith('ind_') and col != 'log_mv']
            risk_columns = [col for col in daily_data.columns if col.startswith('ind_') or col == 'log_mv']
            risk_exposures = daily_data[risk_columns]

            processed_factors_today = pd.DataFrame(index=daily_data.index)

            # 3. 逐个因子进行处理
            for factor in factor_columns:
                # a. 去极值
                factor_winsorized = winsorize(daily_data[factor])

                # b. 标准化
                factor_standardized = standardize(factor_winsorized)

                # c. 中性化
                factor_neutralized = neutralize(factor_standardized, risk_exposures)

                # d. 再次标准化（中性化后的残差不保证标准差为1）
                final_factor = standardize(factor_neutralized)

                processed_factors_today[factor] = final_factor

            # 4. 将处理完的宽表转换为窄表，并添加入总列表
            narrow_df = processed_factors_today.stack().reset_index()
            narrow_df.columns = ['ts_code', 'factor_name', 'factor_value']
            narrow_df['trade_date'] = trade_date
            all_processed_data.append(narrow_df)

        # 5. 合并所有日期的处理结果并存入数据库
        if all_processed_data:
            final_df = pd.concat(all_processed_data, ignore_index=True)
            final_df = final_df[['ts_code', 'trade_date', 'factor_name', 'factor_value']].dropna()

            logger.info(f"所有日期处理完毕，准备写入 {len(final_df)} 条数据至 '{self.table_processed}' 表...")

            upsert_to_mysql(
                engine=self.engine,
                table_name=self.table_processed,
                df_uncleaned=final_df,
                primary_key=['ts_code', 'trade_date', 'factor_name'],
                create_sql_command=self.create_sql_processed
            )
            logger.info("数据写入完成！")
        else:
            logger.warning("没有处理任何数据。")


if __name__ == '__main__':
    factor_processor = FactorProcessor(
        start_date='2025-08-18',
        end_date='2025-08-19',
        table_raw='temp_factor',
        table_processed='factor_processed',
        create_sql_processed='USE DEFAULT factor_panel_data_without_foreign_key'
    )
    factor_processor.process()