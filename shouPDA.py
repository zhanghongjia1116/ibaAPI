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

from multiprocessing import Pool, Manager


class ShouPDA(IbaDatFile):
    def __init__(self,
                 path: os.PathLike,
                 raw_mode: bool = False,
                 preload: bool = True,
                 name_target: list[str] = None,
                 down_sample: int = 1,
                 ):
        super().__init__(path, raw_mode, preload)
        self.name_target = name_target
        self.analog_data = {}
        self.digital_data = {}
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
        self.down_sample = down_sample
        # self.load_data()

        for i in range(64):
            self.info[f'Module_name_{i}'] = f'Module_name_{i}'

    def coil_id(self):
        """Return the coil id."""
        return re.match(r"([^_]+)", os.path.basename(self.path)).group(1)  # 使用正则表达式提取匹配的部分

    def timeIndex(self) -> pd.DatetimeIndex:
        """返回数据每一行的时间索引。"""
        # try:
        start = self.start_time()
        frames = self.frames()
        clk = self.clk()
        times = [start + datetime.timedelta(seconds=i * clk) for i in range(frames)]
        self.time_aixs = True
        return pd.DatetimeIndex(times, name="time")
        # except Exception as e:
        #     print(e)
        #     self.time_aixs = False
        #     return None
        
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
    
    def load_data(self) -> pd.DataFrame:
        analog_data = {}
        digital_data = {}
        # # 降采样时间序列
        # try:
        time_index = self.timeIndex()[::self.down_sample]
        analog_data['Time'] = time_index
        digital_data['Time'] = time_index
        # except:
        #     print('Time index is not available.')
        length = len(time_index)
                
        for channel in self:
            name = channel.name()
            data = channel.data()
            base = float(channel.pda_tbase())
            x_offset = float(channel.xoffset())
            if channel.is_analog():
                if base == '0.008':
                    data = np.array(data, dtype='float32')
                else:  # 观察数据采样频率还可能为0.032 0.024等
                    multiple = int(float(base) / 0.008)  # 重采样的倍数
                    data = np.array(data, dtype='float32')
                    if x_offset != 0:  # 如果滞后时间不为0
                        # print(channel.name() + 'offset is ' + str(x_offset))
                        start_pot = int(x_offset / 0.008)
                        offset_init = np.full(start_pot, None, dtype=data.dtype)
                        tmp = np.repeat(data, multiple, axis=0)[:-start_pot]
                        data = np.concatenate((offset_init, tmp))
                    else:  # 如果滞后时间为0
                        data = np.repeat(data, multiple, axis=0)
                        
                data = data[::self.down_sample]
                if len(data) != length:
                    print(f'Warning: {name} length is not equal to time index length.')
                    print(data)
                analog_data[name] = data
                del data
                self.analog_data = analog_data
            else:
                if base == '0.008':
                    data = np.array(data, dtype='int8')
                else:
                    data = np.array(data, dtype='int8')
                    multiple = int(float(base) / 0.008)
                    if x_offset != 0:
                        start_pot = int(x_offset / 0.008)
                        offset_init = np.full(start_pot, 1, dtype='int8')
                        tmp = np.repeat(data, multiple, axis=0)[:-start_pot]
                        data = np.concatenate((offset_init, tmp))
                    else:
                        data = np.repeat(data, multiple, axis=0)
                data = data[::self.down_sample]
                digital_data[name] = data
                del data
                self.digital_data = digital_data
            
def get_feature_info():
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
    
def export_single_steel(steel_path, analog_dir, digital_dir):
    pda_data_path = Path(steel_path)
    # target = pd.read_csv(r'E:\baoSteel\ibaAPI\data\合并文件所需变量.csv').iloc[:, 0].tolist()
    with ShouPDA(pda_data_path, name_target=None, down_sample=5) as file:
        steel_id = file.coil_id()
        file_name_without_suffix = os.path.splitext(os.path.basename(steel_path))[0]
        print(f'Processing {file_name_without_suffix}...')
        
        file.load_data()
        df_analog = pd.DataFrame.from_dict(file.analog_data)   # 容易出现内存溢出
        df_analog.to_csv(rf"{analog_dir}/{steel_id}_analog.csv", index=False)  # index=False 可以去掉索引列
        df_digital = pd.DataFrame.from_dict(file.digital_data)  
        df_digital.to_csv(rf'{digital_dir}/{steel_id}_digital.csv', index=False)


def chunk_export_single_steel(steel_path, analog_dir, digital_dir, progress_dict, chunk_size=3000):
    pda_data_path = Path(steel_path)
    
    with ShouPDA(pda_data_path, name_target=None, down_sample=5) as file:
        # steel_id = file.coil_id()
        file_name_without_suffix = os.path.splitext(os.path.basename(steel_path))[0]
        file.load_data()
        
        # 分块处理模拟数据
        analog_data = file.analog_data
        analog_keys = list(analog_data.keys())
        analog_length = len(analog_data[analog_keys[0]])
        
        # 分块处理数字数据
        digital_data = file.digital_data
        digital_keys = list(digital_data.keys())
        digital_length = len(digital_data[digital_keys[0]])
        
        if analog_length != digital_length:
            assert 'Analog data length is not equal to digital data length.'
        
        if analog_length < chunk_size:
            df_analog = pd.DataFrame.from_dict(analog_data)
            df_analog.to_csv(rf"{analog_dir}/{file_name_without_suffix}_analog.csv", index=False)
            df_digital = pd.DataFrame.from_dict(digital_data)
            df_digital.to_csv(rf'{digital_dir}/{file_name_without_suffix}_digital.csv', index=False)
            return
        
        for start in range(0, analog_length, chunk_size):
            chunk_analog = {
                key: analog_data[key][start:start + chunk_size] for key in analog_keys}
            
            chunk_digital = {
                key: digital_data[key][start:start + chunk_size] for key in digital_keys}
            
            df_analog_chunk = pd.DataFrame.from_dict(chunk_analog)
            df_digital_chunk = pd.DataFrame.from_dict(chunk_digital)
            
            df_analog_chunk.to_csv(rf"{analog_dir}/{file_name_without_suffix}_analog.csv", mode='a',
                                   index=False, header=not Path(rf"{analog_dir}/{file_name_without_suffix}_analog.csv").exists())
            
            df_digital_chunk.to_csv(rf'{digital_dir}/{file_name_without_suffix}_digital.csv', mode='a',
                                    index=False, header=not Path(rf'{digital_dir}/{file_name_without_suffix}_digital.csv').exists())

        # 更新进度
        progress_dict[steel_path] = True
        completed = sum(progress_dict.values())
        total = len(progress_dict)
        print(f"Progress: {completed}/{total} ({(completed / total) * 100:.2f}%)")
    

if __name__ == '__main__':
    # pda_data_path = Path(r"E:\baoSteel\ibaAPI\test\dat_dir\H123116105000_1_00.dat")
    # with ShouPDA(pda_data_path, name_target=None) as file:
    #     file.load_data()
    #     # print(file.analog_data)
    #     # print(file.digital_data)
    #     df_analog = pd.DataFrame.from_dict(file.analog_data)
    #     df_digital = pd.DataFrame.from_dict(file.digital_data)
    #     print(df_analog)
    
    analog_dir = r'E:\张洪嘉硕士论文\dataset\1CD61\analog'
    digital_dir = r'E:\张洪嘉硕士论文\dataset\1CD61\digital'
    # chunk_export_single_steel(pda_data_path, analog_dir, digital_dir, chunk_size=3000)

    # 获取文件夹下所有文件路径
    steel_dir = r'E:\张洪嘉硕士论文\dataset\1CD61\rawdata'
    all_steel_path = [os.path.join(steel_dir, steel) for steel in os.listdir(steel_dir)]
    
    # # 使用tqdm遍历
    # for i in trange(len(all_steel_path), desc='Processing{0}'.format(all_steel_path[0])):
    #     chunk_export_single_steel(all_steel_path[i], analog_dir, digital_dir)
    
    # export_single_steel(all_steel_path[0], analog_dir, digital_dir)

    with Manager() as manager:
        progress_dict = manager.dict({steel_path: False for steel_path in all_steel_path})
        with Pool() as pool:
            # 使用 starmap 将参数传递给每个进程
            pool.starmap(chunk_export_single_steel, [(steel_path, analog_dir, digital_dir, progress_dict) for steel_path in all_steel_path])