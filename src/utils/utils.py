import functools
import inspect

import pymysql
import sqlalchemy
from utils.table_generator import generator
import pandas as pd
from dotenv import load_dotenv
import os
import tushare as ts
from utils.logger_config import app_logger as logger

@logger.catch()
def ensure_table_exists(engine, table_name: str, create_sql: str="", df=pd.DataFrame()):
    """
    检查数据库中是否存在指定的表，如果不存在，则根据提供的SQL语句创建它。

    :param engine: sqlalchemy.engine.Engine - SQLAlchemy的数据库连接引擎。
    :param table_name: str - 需要检查和创建的表的名称。
    :param create_sql: str - 用于创建表的完整 "CREATE TABLE" SQL语句。
    :return: bool - 如果操作成功（表已存在或创建成功），返回 True，否则返回 False。
    """
    inspector = sqlalchemy.inspect(engine)
    if not inspector.has_table(table_name):
        logger.debug(f"表格 '{table_name}' 不存在，正在自动创建...")
        try:
            if create_sql in ["", "auto", "akaza akari"]:
                logger.debug("未输入SQL语句。")
                if table_name in generator.keys():
                    logger.debug("在预设的SQL语句中找到了匹配的创建表格选项。")
                    create_sql = generator[table_name]
                else:
                    if df.empty:
                        logger.error("并未在预设的SQL语句中找到了匹配的创建表格选项，创建表格失败。")
                        return False
                    else:
                        df.to_sql(table_name, engine)
                        logger.debug("使用传入的数据自动新建了表格。")
                        return True
            elif create_sql[:11].lower() == "use default":
                default_sql = create_sql.lower().replace("use default","").replace(" ","")
                if default_sql in generator.keys():
                    logger.debug("在预设的SQL语句中找到了匹配的创建表格选项。")
                    create_sql = generator[default_sql]
                else:
                    logger.error("并未在预设的SQL语句中找到了匹配的创建表格选项，创建表格失败。")
                    return False
            with engine.connect() as connection:
                connection.execute(sqlalchemy.text(create_sql.format(table_name=table_name)))
                # 在SQLAlchemy 2.x中，DDL语句（如CREATE TABLE）通常会自动提交，
                # 但在显式事务中执行可以确保某些数据库引擎下的行为一致性。
                # 此处为简化，不使用显式begin()/commit()，因为connect()的with块已足够。
            logger.success(f"表格 '{table_name}' 创建成功。")
            return True
        except Exception as e:
            logger.error(f"创建表格 '{table_name}' 时发生严重错误: {e}")
            return False
    else:
        logger.trace(f"表格 '{table_name}' 已存在，无需创建。")
        return True

@logger.catch()
def upsert_to_mysql(engine, table_name:str, df_uncleaned:pd.DataFrame, primary_key:list=['ts_code'],
                    create_sql_command: str=""):
    """
    将 Pandas DataFrame 的数据批量“更新或插入”(Upsert)到 MySQL 数据库表中。

    该函数通过构建原生 SQL `INSERT ... ON DUPLICATE KEY UPDATE` 语句，实现高效的数据同步。
    它会自动处理 DataFrame 中的 `np.nan` 和 `pd.NaT`，将其转换成数据库中的 `NULL`。
    整个操作在一个事务中执行，以确保数据写入的原子性。如果目标表不存在，
    可以根据提供的SQL命令自动创建。

    Args:
        engine (sqlalchemy.engine.Engine): SQLAlchemy 的数据库引擎实例。
        table_name (str): 目标数据库表的名称。
        df_uncleaned (pd.DataFrame): 包含待写入数据的 Pandas DataFrame。
        primary_key (list, optional): 用于判断重复记录的主键或唯一键列表。
                                      默认为 ['ts_code']。
        create_sql_command (str, optional): 如果表不存在时，用于创建表的 SQL DDL 语句。
                                             默认为空字符串，即不尝试创建表。
        echo (bool, optional): 是否在控制台打印执行信息。默认为 False。现在此参数以废止，以loguru库中的日志控制取而代之。

    Returns:
        None: 该函数没有返回值。
    """
    if df_uncleaned is None or df_uncleaned.empty:
        logger.warning("传入的DataFrame为空，操作已跳过。")
        return
    # 处理缺失值：将 DataFrame 中所有的 np.nan (以及 pd.NaT) 替换为 None。
    # .astype(object) 确保所有列都能容纳 None，然后 .where 进行替换。
    df = df_uncleaned.astype(object).where(pd.notnull(df_uncleaned), None)
    # 从 DataFrame 获取列名
    cols = [f"`{col}`" for col in df.columns]
    # logger.debug(cols)
    cols_str = ", ".join(cols)
    # 构造 VALUES 部分的具名占位符
    placeholders = ", ".join(f":{col}" for col in df.columns)
    # 构造 ON DUPLICATE KEY UPDATE 部分
    update_cols = []
    for col in df.columns:
        if col not in primary_key:
            update_cols.append(f"`{col}`=VALUES(`{col}`)")
    update_str = ", ".join(update_cols)
    sql_query = f"""
        INSERT INTO `{table_name}` ({cols_str})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_str}
    """
    # 将清洗后的 DataFrame 转换为字典列表
    data_list_of_dicts = df.to_dict(orient='records')

    # 执行SQL
    try:

        if not ensure_table_exists(engine, table_name, create_sql_command, df=df):
            raise pymysql.err.ProgrammingError("并未发现已存在的表格；同时尝试创建表格也失败了。")
        with engine.connect() as conn:
            with conn.begin() as transaction:
                conn.execute(sqlalchemy.text(sql_query), data_list_of_dicts)
                transaction.commit()
            logger.debug(f"成功向表格 '{table_name}' 同步了 {len(df)} 条数据。")
        engine.dispose()
    except Exception as e:
        logger.error(f"写入数据库时发生错误:{e}")

def easyConnect() -> sqlalchemy.engine.base.Engine:
    load_dotenv()
    password = os.environ.get("PASSWORD")
    database = os.environ.get("DATABASE")
    db_conn_str = \
        r'mysql+pymysql://root:{password}@{host}:{port}/{database}'.format(
            password=password,
            host='localhost',
            port=3306,
            database=database
        )
    engine = sqlalchemy.create_engine(db_conn_str)
    return engine


def easyPro() -> ts.pro.client.DataApi:
    load_dotenv()
    api_key = os.environ.get("API_KEY")
    ts.set_token(api_key)
    return ts.pro_api()