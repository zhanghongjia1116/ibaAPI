from __future__ import annotations

import datetime
import os
import pathlib
import struct
from typing import Generator

import numpy as np
import pandas as pd
import pythoncom
import pywintypes
from win32com import client

A = 0
B = 0
V = client.VARIANT(pythoncom.VT_BYREF | pythoncom.VT_VARIANT, 2)  # noqa: F821

# print("Python version: ", struct.calcsize("P") * 8)  # 查看当前python解释器是32位还是64位

class IbaChannel:
    """
    Class representing a single channel of an iba .dat file
    """

    def __init__(self, channel):
        """Initialize the channel object."""
        self.channel = channel

    def index(self):
        return f'[{str(self.channel.ModuleNumber)}:{str(self.channel.NumberInModule)}]'

    def name(self) -> str:
        """Return the channel name."""
        _name = self.channel.QueryInfoByName("name")
        if _name == '':
            _name = self.index()
            # print(_name)
        elif _name[0] == ' ':  # 去除变量组第一个变量前的空格
            _name = _name[1:]
        return _name

    def minscale(self):
        """Return the channel minscale."""
        return self.channel.QueryInfoByName("minscale")

    def maxscale(self):
        """Return the channel maxscale."""
        return self.channel.QueryInfoByName("maxscale")

    def xoffset(self) -> int:
        """返回通道x偏移（以帧为单位）一般值为0。"""
        return self.channel.QueryInfoByName("xoffset")

    def unit(self) -> str:
        """Return unit of the channel data."""
        return self.channel.QueryInfoByName("unit")

    def digchannel(self):
        """Return digchannel info."""
        return self.channel.QueryInfoByName("digchannel")

    def pda_type(self) -> str:
        """Return the data type of the channel. Only used for ShouGang data."""
        return self.channel.QueryInfoByName("$PDA_Typ")

    def pda_tbase(self) -> str:
        """获取当前频道的采样率，返回字符串. Only used for ShouGang data."""
        return self.channel.QueryInfoByName("$PDA_Tbase")

    def is_time_based(self) -> bool:
        """返回bool序列是否基于时间。"""
        return self.channel.IsDefaultTimebased()

    def is_bool(self) -> bool:
        """如果序列包含布尔值，则返回1。"""
        return self.channel.IsDigital()

    def is_analog(self) -> bool:
        """Return true if series contains analog values."""
        return self.channel.IsAnalog()

    def data(self) -> np.array:
        """Return the channel data."""
        if self.is_time_based():
            data = np.array(self.channel.QueryTimebasedData(A, B, V)[2])
        else:
            data = np.array(self.channel.QueryLengthbasedData(A, B, V)[2])
        # if self.is_bool():
            # return data.astype(bool)
        # elif self.pda_type() == "int16":
        #     return data.astype("int16")
        # else:
        return data

    def series(self) -> pd.Series:
        return pd.Series(self.data(), name=self.name())

    def id(self) -> int:
        """Return the channel id."""
        return self.channel.QueryChannelId()


class IbaDatFile:
    """Class representing an Iba .dat file"""

    def __init__(
            self,
            path: os.PathLike,
            raw_mode: bool = False,
            preload: bool = True,
    ):
        """
        Initialize the dat file object.

        Args:
            path (os.PathLike): dat file path.
            raw_mode (bool, optional): Defaults to False.
            preload (bool, optional): Defaults to True.
        """
        self.path = os.fspath(path)
        try:
            self.reader = client.dynamic.Dispatch("{089CC1F3-E635-490B-86F8-7731A185DFD9}")
        except pywintypes.com_error as e:
            raise IOError("Necessary dlls are not installed.") from e
        self.reader.PreLoad = int(preload)
        self.reader.RawMode = int(raw_mode)

    def __enter__(self):
        """在进入with语句块时，会执行__enter__方法中的操作，打开文件。"""
        self.reader.Open(self.path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        在退出with语句块时，会执行__exit__方法中的操作，关闭文件。
        """
        self.reader.Close()

    def __iter__(self) -> Generator[IbaChannel, None, None]:
        """__iter__ 方法，可以使用 for 循环对对象进行迭代"""
        enumerator = self.reader.EnumChannels()  # 获取组名
        while not enumerator.IsAtEnd():  # 遍历所有频道
            channel = enumerator.Next()  # 枚举频道
            if channel is not None:  # 如果频道不为空
                yield IbaChannel(channel)  # 返回频道对象

    def __getitem__(self, index: str) -> IbaChannel:
        for channel in self:
            if channel.name() == index:
                return channel
        raise IndexError(index)

    def timeIndex(self) -> pd.DatetimeIndex:
        """返回数据每一行的时间索引。"""
        start = self.start_time()
        frames = self.frames()
        clk = self.clk()
        times = [start + datetime.timedelta(seconds=i * clk) for i in range(frames)]
        return pd.DatetimeIndex(times, name="time")

    def channel_names(self) -> list[str]:
        """Return list of channel names."""
        return [channel.name() for channel in self]

    def query_info_by_name(self, name: str) -> str:
        return self.reader.QueryInfoByName(name)

    def frames(self) -> int:
        """返回通道的帧数, 即数据的行数"""
        return int(self.reader.QueryInfoByName("frames"))

    def clk(self) -> float:
        """读取采样频率"""
        return float(self.reader.QueryInfoByName("clk"))

    def recorder_version(self) -> str:
        """Return the software version of the recorder."""
        return self.reader.QueryInfoByName("version")

    def recorder_name(self) -> str:
        """Return the name of the recorder."""
        return self.reader.QueryInfoByName("name")

    def recorder_type(self) -> str:
        """Return the software version of the recorder."""
        return self.reader.QueryInfoByName("type")

    def start_time(self) -> datetime.datetime:
        """Return the recording start time as datetime object."""
        return datetime.datetime.strptime(
            self.reader.QueryInfoByName("starttime"), "%d.%m.%Y %H:%M:%S.%f"
        )

    def start_time_as_str(self) -> str:
        """Return the recording start time as str."""
        return self.reader.QueryInfoByName("starttime")

    def data(self) -> pd.DataFrame:
        """Return data as a dataframe."""
        data = {channel.name(): channel.data() for channel in self}
        return pd.DataFrame.from_dict(data)


def read_ibadat(path: os.PathLike, raw_mode: bool = False, preload: bool = True) -> pd.DataFrame:
    """
    Read the raw iba .dat file and return the raw data as a dataframe.
    """
    with IbaDatFile(path, raw_mode, preload) as file:
        return file.data()


if __name__ == '__main__':
    data_path = pathlib.Path("data/H124214505100_1.dat")
