import pandas as pd
import numpy as np
from utils.utils import easyConnect, upsert_to_mysql  # 假设您的utils里有这些函数
from factor_lab import factor_prepocess, factor_IC_analyze
from loguru import logger



if __name__ == '__main__':
    factor_processor = factor_prepocess.FactorPreProcessor(
        start_date='2025-08-18',
        end_date='2025-08-19',
        table_raw='factor_raw',
        table_processed='factor_processed',
        create_sql_processed='USE DEFAULT factor_panel_data_without_foreign_key'
    )
    factor_processor.process()

    IC_analyzer = factor_IC_analyze.ICAnalyzer(
        factor_table='factor_processed',
        market_table='stock_daily'
    )