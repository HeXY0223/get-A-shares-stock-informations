# src/factor_lab/volatility.py

import pandas as pd
import numpy as np
from .base import FactorBase
from utils.utils import easyPro, easyConnect
import tushare as ts

def get_daily_data(ts_codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    返回的 DataFrame 应包含:
    - ts_code: 股票代码
    - trade_date: 交易日期
    - close: 复权收盘价
    - volume: 成交量
    - amount: 成交额
    """

    print(f"正在获取 {len(ts_codes)} 支股票从 {start_date} 到 {end_date} 的数据...")
    start_date = start_date.replace('-', '')
    end_date = end_date.replace('-', '')
    pro = easyPro()
    all_data = []
    for code in ts_codes:
        df = ts.pro_bar(ts_code=code, start_date=start_date, end_date=end_date,
                        adj='qfq', fields='ts_code,trade_date,close,vol,amount')
        all_data.append(df)
    concater = pd.concat(all_data).set_index('trade_date')
    concater.rename(columns={'vol':'volume'}, inplace=True)
    return concater


def get_index_daily_data(index_codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    返回的 DataFrame 应包含:
    - ts_code: 指数代码
    - trade_date: 交易日期
    - close: 收盘价
    """

    print(f"正在获取 {len(index_codes)} 个指数从 {start_date} 到 {end_date} 的数据...")
    start_date = start_date.replace('-', '')
    end_date = end_date.replace('-', '')
    pro = easyPro()
    all_data = []
    for code in index_codes:
        df = pro.sw_daily(ts_code=code, start_date=start_date, end_date=end_date,
                             fields='ts_code,trade_date,close')
        all_data.append(df)

    return pd.concat(all_data).set_index('trade_date')

def stock2index(stock_codes: list[str], min_category: int=2) -> pd.DataFrame:
    # 股票代码对应到指数代码
    # 需要：数据库中有申万行业分类sw_category
    engine=easyConnect()
    pro = easyPro()
    query = """
    SELECT ts_code, l1_code, l2_code, l3_code
    FROM sw_category
    WHERE ts_code IN ({})
    """.format(','.join(['%s']*len(stock_codes)))

    df = pd.read_sql(query, con=engine, params=tuple(stock_codes))
    s2i_l1 = df.set_index('ts_code')['l1_code'].to_dict()
    s2i_l2 = df.set_index('ts_code')['l2_code'].to_dict()
    s2i_l3 = df.set_index('ts_code')['l3_code'].to_dict()

    # 添加缺失项
    for code in stock_codes:
        if code not in s2i_l1:
            s2i_l1[code] = None
        if code not in s2i_l2:
            s2i_l2[code] = None
        if code not in s2i_l3:
            s2i_l3[code] = None
    s2i = {}
    # 逐项检查指数是否有行情 已知一级行业全有行情
    if_l3 = pro.index_classify(level='l3', src='sw2021', fields='index_code,is_pub')
    if_l2 = pro.index_classify(level='l2', src='sw2021', fields='index_code,is_pub')
    if min_category == 3:
        for stock, index_l3 in s2i_l3.items():
            if index_l3 is not None:
                if if_l3[if_l3['index_code'] == index_l3]['is_pub'].iloc[0] == '1':
                    s2i[stock] = index_l3
                else:
                    index_l2 = s2i_l2[stock]
                    if if_l2[if_l2['index_code'] == index_l2]['is_pub'].iloc[0] == '1':
                        s2i[stock] = index_l2
                    else:
                        s2i[stock] = s2i_l1[stock] # index_l1
        return s2i
    elif min_category == 2:
        for stock, index_l2 in s2i_l2.items():
            if index_l2 is not None:
                if if_l2[if_l2['index_code'] == index_l2]['is_pub'].iloc[0] == '1':
                    s2i[stock] = index_l2
                else:
                    s2i[stock] = s2i_l1[stock]  # index_l1
        return s2i
    elif min_category == 1:
        return s2i_l1



# --- 波动类因子实现 ---

class AnnualizedVolatility(FactorBase):
    """
    年化波动率
    计算公式：日收益率标准差 × √250
    """

    @property
    def factor_name(self) -> str:
        return "年化波动率"

    def calculate(self) -> pd.DataFrame:
        # 需要往前多取一些数据用于计算收益率
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(months=2)).strftime('%Y-%m-%d')

        # 获取复权收盘价
        daily_data = get_daily_data(self.ts_codes, start_dt_extended, self.end_date)
        close_prices = daily_data.pivot(columns='ts_code', values='close')

        # 计算日收益率
        daily_returns = close_prices.pct_change()

        # 计算滚动30日标准差并年化 (√250)
        rolling_std = daily_returns.rolling(window=30, min_periods=20).std()
        annualized_vol = rolling_std * np.sqrt(250)

        # 截取最终需要的日期范围
        return annualized_vol.loc[self.start_date.replace("-", ""):self.end_date.replace("-", "")]


class MaxDrawdown(FactorBase):
    """
    最大回撤
    计算公式：历史最高点至最低点的最大跌幅
    """

    @property
    def factor_name(self) -> str:
        return "最大回撤"

    def calculate(self) -> pd.DataFrame:
        # 需要往前多取数据计算历史最高点
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(months=6)).strftime('%Y-%m-%d')

        # 获取复权收盘价
        daily_data = get_daily_data(self.ts_codes, start_dt_extended, self.end_date)
        close_prices = daily_data.pivot(columns='ts_code', values='close')

        # 计算滚动最大回撤
        def calculate_max_drawdown_series(price_series):
            """计算单只股票的滚动最大回撤"""
            # 计算累计最高价
            cummax = price_series.expanding().max()
            # 计算回撤 = (当前价格 - 历史最高价) / 历史最高价
            drawdown = (price_series - cummax) / cummax
            # 计算过去60日内的最大回撤
            max_dd = drawdown.rolling(window=60, min_periods=30).min()
            return max_dd.abs()  # 转为正数表示回撤幅度

        # 对每只股票计算最大回撤
        max_drawdown = close_prices.apply(calculate_max_drawdown_series)

        # 截取最终需要的日期范围
        return max_drawdown.loc[self.start_date.replace("-", ""):self.end_date.replace("-", "")]


class BetaValue(FactorBase):
    """
    Beta值
    计算公式：个股与行业指数的协方差 / 行业指数方差（250日）
    """

    def __init__(self, ts_codes: list[str], start_date: str, end_date: str, min_category: int = 2):
        super().__init__(ts_codes, start_date, end_date)
        self.min_category = min_category  # 最小行业分类级别

    @property
    def factor_name(self) -> str:
        return "Beta值"

    def calculate(self) -> pd.DataFrame:
        # 需要往前多取250个交易日的数据用于计算
        start_dt_extended = (pd.to_datetime(self.start_date) - pd.DateOffset(months=14)).strftime('%Y-%m-%d')

        # 1. 获取股票到行业指数的映射关系
        stock_to_index = stock2index(self.ts_codes, min_category=self.min_category)

        # 2. 收集所有需要的行业指数代码
        index_codes = list(set(stock_to_index.values()))
        # 过滤掉None值
        index_codes = [code for code in index_codes if code is not None]

        if not index_codes:
            print("警告：没有找到有效的行业指数代码")
            return pd.DataFrame()

        # 3. 获取个股复权收盘价
        stock_data = get_daily_data(self.ts_codes, start_dt_extended, self.end_date)
        stock_prices = stock_data.pivot(columns='ts_code', values='close')

        # 4. 获取所有相关行业指数的收盘价
        index_data = get_index_daily_data(index_codes, start_dt_extended, self.end_date)
        index_prices = index_data.pivot(columns='ts_code', values='close')

        # 5. 计算日收益率
        stock_returns = stock_prices.pct_change()
        index_returns = index_prices.pct_change()

        # 6. 为每只股票计算对应行业指数的Beta值
        beta_results = []

        for stock_code in self.ts_codes:
            # 获取该股票对应的行业指数代码
            corresponding_index = stock_to_index.get(stock_code)

            if corresponding_index is None or corresponding_index not in index_prices.columns:
                print(f"警告：股票 {stock_code} 没有找到对应的行业指数，跳过")
                continue

            if stock_code not in stock_returns.columns:
                print(f"警告：股票 {stock_code} 没有价格数据，跳过")
                continue

            # 获取该股票和对应指数的收益率序列
            stock_return_series = stock_returns[stock_code]
            index_return_series = index_returns[corresponding_index]

            # 计算滚动250日Beta值
            def calculate_beta_for_stock(stock_returns_ser, index_returns_ser):
                """计算单只股票相对于其对应指数的Beta值"""
                # 确保两个序列对齐
                aligned_stock, aligned_index = stock_returns_ser.align(index_returns_ser, join='inner')

                # 计算滚动250日的协方差和方差
                covariance = aligned_stock.rolling(window=250, min_periods=200).cov(aligned_index)
                index_variance = aligned_index.rolling(window=250, min_periods=200).var()

                # Beta = Cov(stock, index) / Var(index)
                beta = covariance / index_variance
                return beta

            # 计算该股票的Beta值
            stock_beta = calculate_beta_for_stock(stock_return_series, index_return_series)
            stock_beta.name = stock_code
            beta_results.append(stock_beta)

        if not beta_results:
            print("警告：没有成功计算任何股票的Beta值")
            return pd.DataFrame()

        # 7. 合并所有股票的Beta值
        beta_values = pd.concat(beta_results, axis=1)

        # 确保列顺序与输入股票代码顺序一致
        available_stocks = [code for code in self.ts_codes if code in beta_values.columns]
        beta_values = beta_values[available_stocks]

        # 8. 截取最终需要的日期范围
        return beta_values.loc[self.start_date.replace("-", ""):self.end_date.replace("-", "")]
