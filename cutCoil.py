from ibadatfile import read_ibadat
from typing import List
import pathlib
import pandas as pd
from sqlalchemy import create_engine

# 连接本地数据库
def connectDB():
    conn = create_engine('mysql+pymysql://root:root@localhost:3306/bao_steel?charset=utf8')
    return conn

# 遍历目录下所有.dat文件，

# 将 DataFrame 写入数据库
try:
    connection = connectDB()
    df = read_ibadat(pathlib.Path('data/bao_t000.dat'))
    # # 找到第一行中值为 True 或 False 的列 (删除模拟量)
    columns_to_drop = df.columns[df.iloc[0].isin([True, False])]
    df.drop(columns=columns_to_drop, inplace=True)
    df.to_sql('vibration_pda', con=connection, if_exists='replace', index=False)
    print("DataFrame 写入数据库成功！")
except Exception as e:
    print("发生错误：", e)


def cutCoil(path) -> List[pd.DataFrame]:
    """_summary_

    Args:
        path (_type_): _description_

    Returns:
        List[pd.DataFrame]: _description_
    """
    # Read the ibadat file
    iba = read_ibadat(path)

# def plot_roll_length() -> :