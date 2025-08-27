import pandas as pd
from typing import Optional
from sqlalchemy.engine import Engine
# 1. 导入您项目中的模块
from utils.logger_config import app_logger as logger
from utils.utils import easyPro, easyConnect


class ICAnalyzer:
    """
    因子分析器，用于从数据库加载数据并计算因子的IC、Rank IC和IC-IR。
    """

    def __init__(self, factor_table: str, market_table: str):
        """
        初始化因子分析器。

        Args:
            db_conn (Engine): SQLAlchemy 的数据库连接引擎。
            factor_table (str): 存储因子数据的数据库表名。
            market_table (str): 存储日线行情数据的数据库表名。
            pro_api: Tushare Pro 的 API 实例 (可选)。
        """
        self.db_conn = easyConnect()
        self.factor_table = factor_table
        self.market_table = market_table
        self.pro_api = easyPro()

        if not self.db_conn:
            raise ValueError("数据库连接 (db_conn) 不能为空。")

        logger.info("因子分析器初始化完成。")
        logger.info(f"因子表: {self.factor_table}")
        logger.info(f"行情表: {self.market_table}")

    def _load_and_prepare_factors(self) -> Optional[pd.DataFrame]:
        """
        从数据库加载并处理因子数据，将其从长格式转换为宽格式。
        """
        logger.info(f"开始从数据库表 '{self.factor_table}' 加载因子数据...")
        try:
            # 使用 pd.read_sql 直接从数据库读取数据
            sql_query = f"SELECT ts_code, trade_date, factor_name, factor_value FROM {self.factor_table}"
            factor_df = pd.read_sql(sql_query, self.db_conn, parse_dates=['trade_date'])

            if factor_df.empty:
                logger.warning(f"从表 '{self.factor_table}' 中未读取到任何数据。")
                return None

            # 将长格式的因子数据转换为宽格式
            factor_wide_df = factor_df.pivot_table(
                index=['trade_date', 'ts_code'],
                columns='factor_name',
                values='factor_value'
            ).reset_index()

            logger.success("因子数据从数据库加载和转换成功。")
            return factor_wide_df
        except Exception as e:
            logger.error(f"从数据库表 '{self.factor_table}' 读取因子数据时出错: {e}")
            return None
    @logger.catch()
    def _load_and_prepare_market_data(self) -> Optional[pd.DataFrame]:
        """
        从数据库加载行情数据并计算下一交易日的收益率。
        """
        logger.info(f"开始从数据库表 '{self.market_table}' 加载行情数据...")
        try:
            # 选择需要的列，这里我们用前复权收盘价 'close_qfq'
            sql_query = f"SELECT ts_code, trade_date, close_qfq FROM {self.market_table}"
            #sql_query = f"SELECT ts_code, trade_date, close FROM {self.market_table}"
            market_df = pd.read_sql(sql_query, self.db_conn, parse_dates=['trade_date'])

            if market_df.empty:
                logger.warning(f"从表 '{self.market_table}' 中未读取到任何数据。")
                return None

            market_df = market_df.sort_values(by=['ts_code', 'trade_date'])

            # 计算 next_return
            market_df['next_return'] = market_df.groupby('ts_code')['close_qfq'].pct_change().shift(-1)
            #market_df['next_return'] = market_df.groupby('ts_code')['close'].pct_change().shift(-1)
            logger.success("行情数据加载和收益率计算成功。")
            return market_df
        except Exception as e:
            logger.error(f"从数据库表 '{self.market_table}' 读取行情数据时出错: {e}")
            return None

    def _calculate_ic_stats(self, merged_df: pd.DataFrame, factor_columns: list) -> pd.DataFrame:
        """
        计算IC、Rank IC和IC-IR。
        """
        logger.info("开始计算每日 IC 和 Rank IC...")

        ic_series = merged_df.groupby('trade_date').apply(
            lambda x: x[factor_columns].corrwith(x['next_return'], method='pearson')
        )

        rank_ic_series = merged_df.groupby('trade_date').apply(
            lambda x: x[factor_columns].corrwith(x['next_return'], method='spearman')
        )
        logger.success("每日 IC 和 Rank IC 计算完成。")

        logger.info("开始计算 IC-IR 等最终统计指标...")
        ic_mean = ic_series.mean()
        ic_std = ic_series.std()
        ic_ir = ic_mean.divide(ic_std).fillna(0)
        rank_ic_mean = rank_ic_series.mean()

        results_df = pd.DataFrame({
            'IC均值': ic_mean,
            'IC标准差': ic_std,
            'IC-IR': ic_ir,
            'Rank IC均值': rank_ic_mean
        }).reset_index().rename(columns={'factor_name': '因子名称'})

        logger.success("所有统计指标计算完成。")
        return results_df

    def run_analysis(self) -> Optional[pd.DataFrame]:
        """
        执行完整的因子分析流程。
        """
        factor_wide_df = self._load_and_prepare_factors()
        if factor_wide_df is None:
            return None

        market_df = self._load_and_prepare_market_data()
        if market_df is None:
            return None

        logger.info("开始合并因子数据和收益率数据...")
        # 注意：合并的键现在是 'ts_code' 和 'trade_date'
        merged_df = pd.merge(
            factor_wide_df,
            market_df[['trade_date', 'ts_code', 'next_return']],
            on=['trade_date', 'ts_code'],
            how='inner'
        )
        merged_df.dropna(inplace=True)
        logger.success("数据合并完成。")

        factor_columns = [col for col in factor_wide_df.columns if col not in ['trade_date', 'ts_code']]
        if not factor_columns:
            logger.error("未能从因子表中识别出任何因子列。")
            return None

        final_results = self._calculate_ic_stats(merged_df, factor_columns)

        return final_results


# --- 主程序入口 ---
if __name__ == '__main__':

    # 定义数据库表名
    # !! 如果您的表名不同，请在这里修改 !!
    FACTOR_DATA_TABLE = 'factor_processed'
    MARKET_DATA_TABLE = 'temp_data'

    # 实例化分析器
    analyzer = ICAnalyzer(
        factor_table=FACTOR_DATA_TABLE,
        market_table=MARKET_DATA_TABLE,
    )

    # 执行分析
    results = analyzer.run_analysis()

    # 打印最终结果
    if results is not None:
        logger.info("\n--- 因子分析结果汇总 ---")
        print(results.to_string())

