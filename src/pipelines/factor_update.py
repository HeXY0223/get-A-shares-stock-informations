# src/pipelines/factor_update.py

import sys
from pathlib import Path
import pandas as pd
from utils.logger_config import app_logger as logger
from loguru import logger

from utils.utils import easyConnect

# 将 src 目录添加到系统路径，以便导入自定义模块

sys.path.append(str(Path(__file__).resolve().parent.parent))

# 从 factor_lab 中导入所有需要计算的因子类
#from factor_lab.momentum import Return12M, Alpha6M, RSI14

from factor_lab.value import PE, PB, PS, DY, EVEBITDA

#from factor_lab.sentiment import ShareHolderNumCR, FinancingBalanceCR, AnalystRating, LonghuNetInflow
#
#from factor_lab.growth import Revenue, RevenueGR, CapExGR, GrossProfitGR, NetProfitGR
#
#from factor_lab.technical import MACD, BollingerBandWidth
#
#from factor_lab.volatility import AnnualizedVolatility, MaxDrawdown, BetaValue
#
#from factor_lab.liquidity import TurnoverRate20D, AmihudIlliquidity # InstitutionalHoldingChange还未实现
#
#from factor_lab.quality import ROE, DebtToAssetRatio, CashFlowToNetIncome, AccountsReceivableTurnover, OperatingProfitMargin
# 将来可以添加更多

@logger.catch()
def run_factor_update(table_name:str="factor_panel_data", ts_codes: list=[]):
    """
    执行所有因子计算和数据同步的主函数。
    """
    # --- 配置区 ---
    # 定义要计算的股票池和时间范围
    TS_CODES = ts_codes # ['000001.SZ', '600519.SH', '300750.SZ']  # 示例股票池
    START_DATE = '2025-08-18'
    END_DATE = '2025-08-19'

    # TS_CODES = ['000592.SZ'] #龙虎榜测试专用
    # START_DATE = '2021-05-15'
    # END_DATE = '2021-06-15'

    # 将所有要运行的因子类放入一个列表中
    factor_classes_to_run = [
        # Return12M,
        # Alpha6M,
        # RSI14,
        PE,
        PB,
        PS,
        # DY,
        # EVEBITDA,
        # ShareHolderNumCR,
        # FinancingBalanceCR,
        # AnalystRating # 积分不够 高频访问不了 lol
        # LonghuNetInflow # 贼慢！
        # Revenue,
        # RevenueGR,
        # CapExGR,
        # GrossProfitGR,
        # NetProfitGR,
        # MACD,
        # BollingerBandWidth,
        # AnnualizedVolatility,
        # MaxDrawdown,
        # BetaValue,
        # TurnoverRate20D,
        # AmihudIlliquidity,
        # ROE,
        # DebtToAssetRatio,
        # CashFlowToNetIncome,
        # AccountsReceivableTurnover,
        # OperatingProfitMargin
    ]

    # --- 执行区 ---
    for factor_cls in factor_classes_to_run:
        try:
            # 1. 实例化因子对象
            # Alpha6M 有额外参数，需要特殊处理，接下来版本进行修改
            factor_instance = factor_cls(
                ts_codes=TS_CODES,
                start_date=START_DATE,
                end_date=END_DATE
            )

            # 2. 调用统一的保存方法
            factor_instance.save_to_db(table_name=table_name, create_sql='USE DEFAULT factor_panel_data_without_foreign_key')

        except Exception as e:
            # 增加错误处理，确保一个因子失败不会中断整个流程
            logger.error(f"!!! 计算或存储因子 {factor_cls.__name__} 时发生错误: {e}")
            continue


if __name__ == '__main__':
    logger.info("开始执行因子更新流程...")
    stocks = pd.read_sql("select ts_code from temp_data", easyConnect())['ts_code'].unique().tolist()
    run_factor_update(table_name="temp_factor", ts_codes=stocks)
    logger.info("所有因子更新流程执行完毕。")

