# -*- coding: utf-8 -*-
# @Time    : 2024-04-21 17:20:15
# @Author  : 张洪嘉
# @File    : baoPDA
# @Software: PyCharm
import datetime
import os
from pathlib import Path

import pandas as pd

from ibadatfile import IbaDatFile


class BaoPDA(IbaDatFile):
    def __init__(self,
                 path: os.PathLike,
                 raw_mode: bool = False,
                 preload: bool = True,
                 ):
        super().__init__(path, raw_mode, preload)
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
        for i in range(32):
            self.info[f'Module_name_{i}'] = f'Module_name_{i}'

    def start_time(self) -> datetime.datetime:
        """Return the recording start time as datetime object."""
        return datetime.datetime.strptime(
            self.reader.QueryInfoByName("starttime"), "%d.%m.%Y %H:%M:%S")

    def data(self) -> pd.DataFrame:
        """Return data as a dataframe."""
        data = {channel.name(): channel.data() for channel in self}
        return pd.DataFrame.from_dict(data)


if __name__ == '__main__':
    pda_data_path = Path('./data/bao_t000.dat')
    # df1 = read_ibadat(pda_data_path_1)
    with BaoPDA(pda_data_path) as file:
        df = file.data()
        print(df)
