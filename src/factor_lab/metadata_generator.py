import pandas as pd
from utils.utils import *
df = pd.read_csv("../../assets/factors.csv")
df.drop('Unnamed: 0',axis=1, inplace=True)
engine = easyConnect()
df.drop('序号',axis=1,inplace=True)
df.rename({'类别':'category', '因子名称':'factor_name', '计算公式/定义':'definition'}, axis=1, inplace=True)
# df = df.reset_index(drop=True)
print(df)
upsert_to_mysql(engine, 'factor_metadata', df, primary_key=['factor_name'],echo=True)