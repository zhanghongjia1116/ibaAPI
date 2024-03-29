from __future__ import annotations

import os
from typing import Generator
import datetime
import pathlib
from deprecated import deprecated
import struct
import numpy as np
import pandas as pd
import pythoncom
import pywintypes
from win32com import client

A = 0
B = 0
V = client.VARIANT(pythoncom.VT_BYREF | pythoncom.VT_VARIANT, 2)  # noqa: F821


# print(struct.calcsize("P") * 8)   # 查看当前python解释器是32位还是64位

class IbaChannel:
    """
    Class representing a single channel of an iba .dat file
    """

    def __init__(self, channel):
        """Initialize the channel object."""
        self.channel = channel

    def name(self) -> str:
        """Return the channel name."""
        _name = self.channel.QueryInfoByName("name")
        if _name == '':
            _name = f'{str(self.channel.ModuleNumber)}:{str(self.channel.NumberInModule)}'
            print(_name)
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
        """Return the channel x offset (in frames?)."""
        return self.channel.QueryInfoByName("xoffset")

    def unit(self) -> str:
        """Return unit of the channel data."""
        return self.channel.QueryInfoByName("unit")

    def digchannel(self):
        """Return digchannel info."""
        return self.channel.QueryInfoByName("digchannel")

    def pda_type(self) -> str:
        """Return the data type of the channel. Only used for shougang data."""
        return self.channel.QueryInfoByName("$PDA_Typ")

    def pda_tbase(self) -> str:
        """Return the sample rate of the channel. only used for shougang data."""
        return self.channel.QueryInfoByName("$PDA_Tbase")

    def is_time_based(self) -> bool:
        """Return bool whether series is time based."""
        return self.channel.IsDefaultTimebased()

    def is_bool(self) -> bool:
        """Return true if series contains boolean values."""
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
        if self.is_bool():
            return data.astype(bool)
        elif self.pda_type() == "int16":
            return data.astype("int16")
        else:
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
        """Magic method for context manager."""
        self.reader.Open(self.path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Magic method for context manager, close the reader.
        """
        self.reader.Close()

    def __iter__(self) -> Generator[IbaChannel, None, None]:
        """Iterator interface, returns next channel."""
        enumerator = self.reader.EnumChannels()  # 获取组名
        while not enumerator.IsAtEnd():
            channel = enumerator.Next()
            yield IbaChannel(channel)

    def __getitem__(self, index: str) -> IbaChannel:
        for channel in self:
            if channel.name() == index:
                return channel
        raise IndexError(index)

    def open(self, path: os.PathLike):
        """Open .dat file from *path."""
        self.path = os.fspath(path)
        self.reader.Open(self.path)
        return self

    def close(self):
        """Close reader."""
        self.reader.Close()

    def index(self) -> pd.DatetimeIndex:
        """Return the time index for the channels."""
        start = self.starttime()
        frames = self.frames()
        clk = self.clk()
        times = [start + datetime.timedelta(seconds=i * clk) for i in range(frames)]
        return pd.DatetimeIndex(times, name="time")

    def frames(self) -> int:
        """Return amount of frames of channels."""
        return int(self.reader.QueryInfoByName("frames"))

    def signal_count(self) -> int:
        """Return amount of channels."""
        return int(self.reader.QueryInfoByName("totalSignalCount"))

    def clk(self) -> float:
        """读取采样频率"""
        return float(self.reader.QueryInfoByName("clk"))

    def recorder_version(self) -> str:
        """Return the software version of the recorder."""
        return self.reader.QueryInfoByName("version")

    @deprecated(reason="This method is deprecated, the result is null.")
    def recorder_name(self) -> str:
        """Return the name of the recorder."""
        return self.reader.QueryInfoByName("name")

    @deprecated(reason="This method is deprecated, the result is null.")
    def recorder_type(self) -> str:
        """Return the software version of the recorder."""
        return self.reader.QueryInfoByName("type")

    def starttime(self) -> datetime.datetime:
        """Return the recording start time as datetime object."""
        return datetime.datetime.strptime(
            self.reader.QueryInfoByName("starttime"), "%d.%m.%Y %H:%M:%S.%f"
            # self.reader.QueryInfoByName("starttime"), "%d.%m.%Y %H:%M:%S"
        )

    def starttime_as_str(self) -> str:
        """Return the recording start time as str."""
        return self.reader.QueryInfoByName("starttime")

    def return_channel_names(self) -> list[str]:
        """Return list of channel names."""
        return [channel.name() for channel in self]

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
    data_path = pathlib.Path("data/HD12813900200_1.dat")
    # data_path = pathlib.Path("data/bao_t000.dat")

    # # data_path = pathlib.Path("data/J1423B58800200  _0-1000mym-1534mm.dat")
    # df = read_ibadat(data_path)

    # # 找到第一行中值为 True 或 False 的列 (删除模拟量)
    # columns_to_drop = df.columns[df.iloc[0].isin([True, False])]
    # df.drop(columns=columns_to_drop, inplace=True)
    # df.to_csv("data/bao_t020.csv", index=False)

    # 新建测试
    xbase = float(0)  # 获取当前频道的采样率，float
    xOffset = float(0)  # 获取当前频道的滞后时间，float
    data = object  # 获取当前频道的数据，返回touple

    with IbaDatFile(data_path) as file:
        # 所有特征名
        all_features = file.return_channel_names()
        print(len(all_features))

        # start time
        start_time = file.starttime()
        # print(start_time)

        for channel in file:
            if channel is not None and channel.is_analog() == 1:
                # print(channel.name())
                a = channel.name()
                # print(channel.is_analog())
                # if (channel.IsDefaultTimebased() == 1):  # 若是时间基础的数据
                #     xbase, xOffset, data = channel.QueryTimebasedData(xbase, xOffset, data)
                # else:
                #     xbase, xOffset, data = channel.QueryLengthbasedData(xbase, xOffset, data)

        try:
            print(file.recorder_name())
            print(file.recorder_type())


        except Exception as e:
            print(e)
