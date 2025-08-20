import pandas as pd
import numpy as np
import statsmodels.api as sm


def winsorize(series: pd.Series, method='mad', n_dev=5) -> pd.Series:
    """
    对 Series 进行 MAD 法去极值。

    Args:
        series (pd.Series): 输入的因子序列。
        method (str): 目前支持 'mad' (中位数绝对偏差法)。
        n_dev (int): 几倍的标准差（或MAD）之外被视为极值。

    Returns:
        pd.Series: 去极值后的序列。
    """
    if series.dropna().empty:
        return series

    if method == 'mad':
        median = series.median()
        mad = ((series - median).abs()).median()
        # MAD=0 说明所有值都一样，无需处理
        if mad == 0:
            return series

        upper_bound = median + n_dev * mad * 1.4826  # 1.4826是修正系数
        lower_bound = median - n_dev * mad * 1.4826
        return series.clip(lower_bound, upper_bound)
    else:
        raise ValueError("目前只支持 'mad' 方法")


def standardize(series: pd.Series) -> pd.Series:
    """
    对 Series 进行标准化 (z-score)。

    Args:
        series (pd.Series): 输入的因子序列。

    Returns:
        pd.Series: 标准化后的序列 (均值为0，标准差为1)。
    """
    if series.dropna().empty:
        return series

    return (series - series.mean()) / series.std()


def neutralize(factor_series: pd.Series, risk_exposures: pd.DataFrame) -> pd.Series:
    """
    对因子进行风险中性化。

    Args:
        factor_series (pd.Series): 需要被中性化的因子序列 (Y)。
        risk_exposures (pd.DataFrame): 风险敞口矩阵 (X)，如行业虚拟变量、市值因子等。
                                        DataFrame的 index 必须与 factor_series 的 index 一致。

    Returns:
        pd.Series: 中性化后的因子序列 (回归残差)。
    """
    # 确保所有输入数据都是数值类型，无法转换的值会变成NaN
    Y_numeric = pd.to_numeric(factor_series, errors='coerce')
    X_numeric = risk_exposures.apply(pd.to_numeric, errors='coerce')

    # 丢弃所有输入中的NaN，确保回归模型不会出错
    valid_idx = Y_numeric.notna() & X_numeric.notna().all(axis=1)
    if not valid_idx.any():
        return pd.Series(np.nan, index=factor_series.index)

    Y = Y_numeric[valid_idx]
    X = X_numeric[valid_idx]

    # 为回归添加常数项/截距项
    X = sm.add_constant(X)

    # 运行 OLS 回归
    model = sm.OLS(Y, X).fit()

    # 获取残差
    residuals = pd.Series(np.nan, index=factor_series.index)
    residuals[valid_idx] = model.resid

    return residuals

