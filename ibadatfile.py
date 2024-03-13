from __future__ import annotations

import os
from typing import Generator
import datetime
import pathlib

import numpy as np
import pandas as pd
import pythoncom
import pywintypes
from win32com import client

A = 0
B = 0
V = client.VARIANT(pythoncom.VT_BYREF | pythoncom.VT_VARIANT, 2)   # noqa: F821

import struct
print(struct.calcsize("P") * 8)

class IbaChannel:
    """
    Class representing a single channel of an iba .dat file
    """

    def __init__(self, channel):
        """Initialize the channel object."""
        self.channel = channel

    def name(self) -> str:
        """Return the channel name."""
        return self.channel.QueryInfoByName("name")

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
        """Return the channel unit."""
        return self.channel.QueryInfoByName("$PDA_Typ")

    def data(self) -> np.array:
        """Return the channel data."""
        if self.channel.IsDefaultTimebased():
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

    def is_bool(self) -> bool:
        """Return true if series contains boolean values."""
        return self.channel.IsDigital()

    def is_time_based(self) -> bool:
        """Return bool whether series is time based."""
        return self.channel.IsDefaultTimebased()

    def id(self) -> int:
        """Return the channel id."""
        return self.channel.QueryChannelId()


class IbaDatFile:
    """
    Class representing an Iba .dat file
    """

    def __init__(
        self,
        path: os.PathLike,
        raw_mode: bool = False,
        preload: bool = True,
    ):
        """Initialize the dat file object."""
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
        enumerator = self.reader.EnumChannels()
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
        """Return the clock rate."""
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

    def starttime(self) -> datetime.datetime:
        """Return the recording start time as datetime object."""
        return datetime.datetime.strptime(
            # self.reader.QueryInfoByName("starttime"), "%d.%m.%Y %H:%M:%S.%f"
            self.reader.QueryInfoByName("starttime"), "%d.%m.%Y %H:%M:%S"

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
        df = pd.DataFrame.from_dict(data)
        df.index = self.index()
        return df

def read_ibadat(
    path: os.PathLike,
    raw_mode: bool = False,
    preload: bool = True,
) -> pd.DataFrame:
    with IbaDatFile(path, raw_mode, preload) as file:
        # channel = file.return_channel_names()
        # channel_count = len(channel)

        # columns = file.data().columns
        # columns_count = len(columns)

        # diff = list(set(channel) - set(columns))
        # print(channel)
        # print(set(channel))
        # print(diff)

        return file.data()
    

if __name__ == '__main__':
    
    # path = pathlib.Path("../data/")
    # df = read_ibadat(path)
    # print(df)
    # data_dir = pathlib.Path(r"G:\原始数据（201807-201808勿删！）\PDA\201807\180718")
    # print(data_dir)
    # single_dir_data = list(data_dir.glob("*.dat"))
    
    # for i in range(len(single_dir_data)):
    #     print(single_dir_data[i])
    #     df = read_ibadat(single_dir_data[i])
    #     print(df.shape)
    #     if i == 5:
    #         break
    data_path = pathlib.Path("data/J1423B58800200  _0-1000mym-1534mm.dat")
    df = read_ibadat(data_path)
    
    # # 当前路径
    # print(df.columns[:60])
    # print(df.columns.shape)
    # df.to_csv("data/bao_t000.csv", index=False, encoding="utf-8")
