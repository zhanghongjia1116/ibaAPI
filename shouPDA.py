# -*- coding: utf-8 -*-
# @Time    : 2024-04-21 17:00:09
# @Author  : 张洪嘉
# @File    : shouPDA
# @Software: PyCharm
import datetime
import os
from pathlib import Path

import numpy as np
import pandas as pd
import re
from ibadatfile import IbaDatFile


class ShouPDA(IbaDatFile):
    def __init__(self,
                 path: os.PathLike,
                 raw_mode: bool = False,
                 preload: bool = True,
                 name_target: list[str] = None,
                 ):
        super().__init__(path, raw_mode, preload)
        self.name_target = name_target
        self.info = {
            'clk': 'clk',
            'typ': 'typ',
            'version': 'version',
            'starttime': 'starttime',
            'frames': 'frames',
            'starttrigger': 'starttrigger',
            'stoptrigger': 'stoptrigger',
            'Technostring 1.technostring': 'Technostring 1.technostring',
            'Technostring 1.strip number': 'Technostring 1.strip number',
            'Technostring 1.steel grade': 'Technostring 1.steel grade',
            'Technostring 1.entry width': 'Technostring 1.entry width',
            'Technostring 1.exit width': 'Technostring 1.exit width',
            'Technostring 1.entry thickness': 'Technostring 1.entry thickness',
            'Technostring 1.exit thickness': 'Technostring 1.exit thickness',
            'Technostring 1.time': 'Technostring 1.time',
        }
        for i in range(64):
            self.info[f'Module_name_{i}'] = f'Module_name_{i}'

    def coil_id(self):
        """Return the coil id."""
        return re.search(r'H\d+', self.path).group()  # 使用正则表达式提取匹配的部分

    def start_time(self) -> datetime.datetime:
        """Return the recording start time as datetime object."""
        return datetime.datetime.strptime(
            self.reader.QueryInfoByName("starttime"), "%d.%m.%Y %H:%M:%S.%f")

    def query_info(self, name: str) -> str:
        return self.reader.QueryInfoByName(self.info[name])

    def exportFeaturesInfo(self, path='./') -> str:
        """导出当前文件特征信息到csv文件"""
        init_dict = {
            'index': [channel.index() for channel in self],
            'name': [channel.name() for channel in self],
            'unit': [channel.unit() for channel in self],
            'base': [channel.pda_tbase() for channel in self],
            'offset': [channel.xoffset() for channel in self]
        }
        _df = pd.DataFrame(init_dict)
        _df.to_csv(rf'{path}featuresInfo.csv', index=False)
        path = rf'{path}featuresInfo.csv'
        return path

    def data(self, downSample=1):
        # 初始化名称与数据数组
        column_list = []
        data_list = []

        if self.name_target is None:
            self.name_target = self.channel_names()

        for channel in self:
            name = channel.name()
            if name in self.name_target:
                data = channel.data()
                base = channel.pda_tbase()

                if base == '0.008':
                    data = np.array(data, dtype='float32') if channel.is_analog() else np.array(data, dtype='int8')
                else:  # 观察数据采样频率还可能为0.032 0.024等
                    xOffset = float(channel.xoffset())
                    multiple = int(float(base) / 0.008)  # 重采样的倍数

                    if xOffset != 0:  # 如果滞后时间不为0, 例如 [16:0] F5 PCM revolution counter
                        start_pot = int(xOffset / 0.008)
                        offset_init = np.array([None] * start_pot) if channel.is_analog() else np.array([1] * start_pot)
                        tmp = np.repeat(data, multiple, axis=0)[:-start_pot]
                        data = np.concatenate((offset_init, tmp))
                    else:
                        # 如果滞后时间为0 例如[5:11] RCH1: Wedge passline deviation
                        data = np.repeat(data, multiple, axis=0)
                # 对data降采样
                data = data[::downSample]
                column_list.append(name)
                data_list.append(data)
        # 从列表中一次性添加所有列数据
        iba_data = pd.DataFrame(data_list).T
        iba_data.columns = column_list
        # 首列插入Time self.index()
        iba_data.insert(0, 'Time', self.timeIndex()[::downSample])
        # 首列插入文件名
        iba_data.insert(0, 'CoilId', self.coil_id())
        # iba_data = iba_data[::downSample]
        return iba_data


def main1():
    pda_data_path = Path('./data/H124214505100_1.dat')
    # target = pd.read_csv(r'E:\baoSteel\ibaAPI\data\合并文件所需变量.csv').iloc[:, 0].tolist()
    with ShouPDA(pda_data_path, name_target=None) as file:
        df = file.data(downSample=55)
    df.to_csv(r'./data/H124214505100_1_all.csv', index=False)


def main2():
    pda_data_path = Path('./data/H124214505100_1.dat')
    # target = pd.read_csv(r'E:\baoSteel\ibaAPI\data\合并文件所需变量.csv').iloc[:, 0].tolist()
    with ShouPDA(pda_data_path, name_target=None) as file:
        path = file.exportFeaturesInfo()
    df1 = pd.read_csv(path)
    df2 = pd.read_csv(r'./data/合并文件所需变量.csv',
                      usecols=['变量', '中文释义']).rename(columns={'变量': 'name', '中文释义': 'zh_cn_name'})
    df = pd.merge(df1, df2, on='name', how='left')
    # 调整列顺序
    columns = ['index', 'name', 'zh_cn_name', 'unit', 'base', 'offset']
    df = df[columns]
    df.to_csv(r'./featuresInfo.csv', index=False)


if __name__ == '__main__':
    main1()
    # main2()
