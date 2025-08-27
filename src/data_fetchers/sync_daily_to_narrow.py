import pandas as pd
from tqdm import tqdm
from utils.utils import easyConnect, upsert_to_mysql
from utils.logger_config import app_logger as logger


def sync_daily_to_narrow(start_date: str, end_date: str):
    """
    将 'stock_daily' 宽表中指定日期范围的数据，同步到 'extra_data' 窄表中。

    本函数为幂等操作，利用 upsert_to_mysql 的特性，可重复执行而不会产生重复数据，
    只会更新已有记录。

    工作流程:
    1. 连接数据库。
    2. 分块从 'stock_daily' 读取指定日期范围的数据。
    3. 对每个数据块，使用 pandas.melt 将其从宽表转换为窄表。
    4. 调用您提供的 upsert_to_mysql 函数，将转换后的窄表数据“更新或插入”到 'extra_data' 表中。

    Args:
        start_date (str): 同步的开始日期 (e.g., '2023-01-01')。
        end_date (str): 同步的结束日期 (e.g., '2023-12-31')。
    """
    logger.info(f"开始同步 'stock_daily' 表从 {start_date} 到 {end_date} 的数据至 'extra_data' 表。")

    engine = easyConnect()

    # --- 配置项 ---
    SOURCE_TABLE = 'stock_daily'
    TARGET_TABLE = 'extra_data'
    # 根据您机器的内存进行调整，50000行是一个比较安全的大小
    CHUNK_SIZE = 50000

    # --- 准备工作 ---
    # 动态获取源表的所有列，以确定哪些是需要转换的数值列
    try:
        sample_df = pd.read_sql(f"SELECT * FROM {SOURCE_TABLE} LIMIT 1", engine)
    except Exception as e:
        logger.error(f"无法读取源表 '{SOURCE_TABLE}'！请检查表是否存在或数据库连接是否正常。错误: {e}")
        return

    id_vars = ['ts_code', 'trade_date']
    value_vars = [col for col in sample_df.columns if col not in id_vars]

    logger.debug(f"将要转换的字段共 {len(value_vars)} 个: {value_vars}")

    # 构建带日期范围的查询语句
    sql_query = (
        f"SELECT * FROM {SOURCE_TABLE} "
        f"WHERE trade_date BETWEEN '{start_date}' AND '{end_date}'"
    )

    # 计算总行数以显示进度条
    try:
        count_query = sql_query.replace("*", "COUNT(*)")
        total_rows = pd.read_sql(count_query, engine).iloc[0, 0]
    except Exception as e:
        logger.error(f"计算行数时出错: {e}")
        return

    if total_rows == 0:
        logger.warning(f"在 {start_date} 到 {end_date} 范围内，'{SOURCE_TABLE}' 中没有找到需要同步的数据。")
        return

    total_chunks = (total_rows // CHUNK_SIZE) + 1
    logger.info(f"总计需要同步 {total_rows} 行数据，分为 {total_chunks} 个批次处理。")

    # --- 执行转换与同步 ---
    chunk_iterator = pd.read_sql(sql_query, engine, chunksize=CHUNK_SIZE)

    for df_chunk_wide in tqdm(chunk_iterator, total=total_chunks, desc="同步进度"):

        # 1. 使用 melt 函数将宽表数据块转换为窄表
        df_chunk_narrow = pd.melt(
            df_chunk_wide,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name='data_name',  # 新的“指标名称”列
            value_name='data_value'  # 新的“指标值”列
        )

        # 2. 清理无用数据（值为 NaN 的行）
        df_chunk_narrow.dropna(subset=['data_value'], inplace=True)

        if df_chunk_narrow.empty:
            continue

        # 3. 调用您自己的 upsert_to_mysql 函数进行数据同步
        #    主键的设置至关重要，确保了操作的幂等性
        upsert_to_mysql(
            engine=engine,
            table_name=TARGET_TABLE,
            df_uncleaned=df_chunk_narrow,
            primary_key=['ts_code', 'trade_date', 'data_name'],
            create_sql_command='auto'  # 利用您已有的自动建表逻辑
        )

    logger.success(f"成功完成 {start_date} 到 {end_date} 的数据同步！")


if __name__ == '__main__':
    # --- 使用示例 ---
    # 假设您刚刚更新了2024年第一季度的日线数据到 stock_daily 表
    # 现在您想把这部分新数据同步到 extra_data 表中

    start_sync_date = '2025-02-25'
    end_sync_date = '2025-08-26'

    sync_daily_to_narrow(start_sync_date, end_sync_date)

    # 如果您之后又更新了 1月1日 的数据，可以再次运行上面的函数，
    # 它会自动更新 extra_data 中对应的记录，而不会插入重复数据。
