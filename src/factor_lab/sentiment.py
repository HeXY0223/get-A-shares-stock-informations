# src/factor_lab/sentiment.py

import pandas as pd
import numpy as np
from dotenv import load_dotenv

from .base import FactorBase
from utils.utils import *
import tushare as ts


def get_sentiment_data(ts_codes: list[str], start_date: str, end_date: str, report_type: str) -> pd.DataFrame:
    print(f"正在获取 {len(ts_codes)} 支股票从 {start_date} 到 {end_date} 的数据...")
    start_date = start_date.replace('-', '')
    end_date = end_date.replace('-', '')
    all_data = []
    pro = easyPro()
    if report_type == 'stk_holdernumber': # 股东户数 季度数据
        for code in ts_codes:
            df = pro.stk_holdernumber(ts_code=code, start_date=start_date, end_date=end_date,
                                      fields='ts_code,end_date,holder_num')
            all_data.append(df)
    elif report_type == 'financing_balance': # 融资余额 每个交易日都有数据
        for code in ts_codes:
            df = pro.margin_detail(ts_code=code, start_date=start_date, end_date=end_date,
                                   fields="ts_code,trade_date,rzye")
            all_data.append(df)
    elif report_type == 'analyst_rating':
        for code in ts_codes:
            df = pro.report_rc(ts_code=code, start_date=start_date, end_date=end_date,
                               fields='ts_code,report_title,rating')
            all_data.append(df)
    elif report_type == 'top_inst':
        for code in ts_codes:
            for date in pd.date_range(start=start_date, end=end_date):
                df = pro.top_inst(ts_code=code, trade_date=date.strftime("%Y%m%d"), fields='ts_code,trade_date,net_buy,buy,sell')
                all_data.append(df)
    return pd.concat(all_data, ignore_index=True)

def get_quarter_dates(year):
    # 创建四个季度period
    quarters = [pd.Period(f'{year}-Q{q}') for q in range(1, 5)]

    # 获取每个季度的第一天和最后一天
    starts = [q.start_time for q in quarters]
    ends = [q.end_time for q in quarters]

    return starts, ends

class ShareHolderNumCR(FactorBase):
    '''
    CR: Change Rate
    '''
    @property
    def factor_name(self) -> str:
        return "股东户数变化率"

    def calculate(self) -> pd.DataFrame:
        real_start_date = (pd.to_datetime(self.start_date) -pd.DateOffset(months=6)).strftime('%Y-%m-%d')
        holder_num_df = get_sentiment_data(self.ts_codes, self.start_date, self.end_date, report_type='stk_holdernumber')
        holder_num_df.drop_duplicates(subset=['ts_code', 'end_date'], keep='last', inplace=True)
        holder_num_change = self.calculate_period_change_rate_from_long_data(data=holder_num_df,
                                                                             value_col='holder_num',
                                                                             date_col='end_date')
        # 去掉一些change_rate=0的数据，这种数据产生通常是因为数据挨得太近，比如两天。不知道tushare怎么想的
        holder_num_change = holder_num_change[holder_num_change['change_rate'] != 0]
        holder_num_change.rename(columns={'end_date': 'trade_date'}, inplace=True)
        change_wide = holder_num_change.pivot(index='trade_date', columns='ts_code', values='change_rate')
        start_dt_str = self.start_date.replace("-", "")
        end_dt_str = self.end_date.replace("-", "")
        return change_wide.loc[start_dt_str:end_dt_str]


class FinancingBalanceCR(FactorBase):

    @property
    def factor_name(self) -> str:
        return '融资余额增长率'

    def calculate(self) -> pd.DataFrame:
        real_start_date = (pd.to_datetime(self.start_date) - pd.DateOffset(days=8)).strftime('%Y-%m-%d')
        financing_balance = get_sentiment_data(self.ts_codes, self.start_date, self.end_date, report_type='financing_balance')
        financing_balance.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last', inplace=True)

        financing_balanceCR = self.calculate_period_change_rate_from_long_data(data=financing_balance,
                                                                               value_col='rzye')
        fbCR_wide = financing_balanceCR.pivot(index='trade_date', columns='ts_code', values='change_rate')
        start_dt_str = self.start_date.replace("-", "")
        end_dt_str = self.end_date.replace("-", "")
        return fbCR_wide.loc[start_dt_str:end_dt_str]


class AnalystRating(FactorBase):

    @property
    def factor_name(self) -> str:
        return '分析师评级变化'

    def calculate(self) -> pd.DataFrame:
        analyst_rating = get_sentiment_data(self.ts_codes, self.start_date, self.end_date, report_type='analyst_rating')
        # 取每个季度末为本季度的综合评级时间
        # 数据去重
        analyst_rating.drop_duplicates(subset=['report_title'], keep='last', inplace=True)
        analyst_rating.drop(columns=['report_title'], inplace=True)
        analyst_rating.reindex(columns='trade_date')
        ar_list = []
        quarter_starts, quarter_ends = []
        for year in range(self.start_date.year, self.end_date.year + 1):
            tempstart, tempend = get_quarter_dates(year)
            quarter_starts.extend(tempstart)
            quarter_ends.extend(tempend)
        for start, end in zip(quarter_starts, quarter_ends):
            mask = analyst_rating['trade_date'].between(start, end)
            rating = analyst_rating[mask]['rating']
            rating_map={
                'BUY' : 1,
                '买入' : 1,
                '买进': 1,
                '优于大市': 1,
                '买进': 1,
                '区间操作': -1,
                '增持': 1,
                '强推': 2,
                '强烈推荐': 2,
                '推荐': 1,
                '谨慎推荐': -1,
                '跑赢行业' : 1,
                '卖出' :-1
            }
            rating_num = rating.map(rating_map).fillna(0).sum()
            df = pd.DataFrame({'trade_date':[end.strftime("%Y%m%d")], 'rating_num':[rating_num]})
            ar_list.append(df)
        ar_wide = pd.concat(ar_list, ignore_index=True)
        start_dt_str = self.start_date.replace("-", "")
        end_dt_str = self.end_date.replace("-", "")
        return ar_wide.loc[start_dt_str:end_dt_str]


class LonghuNetInflow(FactorBase):
    """
    情绪类因子：龙虎榜净流入金额

    计算逻辑：月内龙虎榜机构席位净买入金额。
    数据频率：月度。我们会将月度数据填充为日度数据。
    """

    @property
    def factor_name(self) -> str:
        return "龙虎榜净流入金额"

    def calculate(self) -> pd.DataFrame:
        """
        计算龙虎榜机构席位月度净买入总额。
        """
        # 1. 获取龙虎榜机构席位数据 (假设的函数)
        # 该函数应返回DataFrame，包含 ['ts_code', 'trade_date', 'net_buy'] (净买入额)
        raw_data = get_sentiment_data(self.ts_codes, self.start_date, self.end_date, report_type="top_inst")
        copies = raw_data.copy()
        raw_data = raw_data[['ts_code','trade_date','net_buy']]
        if raw_data.empty:
            return pd.DataFrame()

        raw_data['trade_date'] = pd.to_datetime(raw_data['trade_date'])

        # 2. 按月对机构净买入额进行求和
        monthly_inflow = raw_data.groupby(['ts_code', pd.Grouper(key='trade_date', freq='M')])['net_buy'].sum()
        monthly_factor = monthly_inflow.reset_index()
        monthly_factor.columns = ['ts_code', 'trade_date', 'factor_value']

        # 3. 转换为日度宽表并填充
        wide_df = monthly_factor.pivot_table(index='trade_date', columns='ts_code', values='factor_value')

        #trade_cal = tushare_api.get_trade_cal(self.start_date, self.end_date)

        #all_stocks_df = wide_df.reindex(index=trade_cal, columns=self.ts_codes)
        # 对于月度累计金额，当月没有交易的股票应为0，而不是沿用上月的值
        #all_stocks_df.ffill(inplace=True).fillna(0, inplace=True)
        locer = wide_df.loc[self.start_date:self.end_date]
        return wide_df.loc[self.start_date:self.end_date]
        #return all_stocks_df.loc[self.start_date:self.end_date]