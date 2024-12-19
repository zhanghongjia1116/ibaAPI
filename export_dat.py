import os
import pandas as pd
from multiprocessing import Pool
from pathlib import Path
from shouPDA import ShouPDA
import shutil

hebing_dir = r'E:\baoSteel\ibaAPI\test\ToCSV FromAny\output'  # PDA文件和板形数据合并后的导出路径

def run(tup):
    """
        合并文件子程序
        tup=[temp_path,old_path],columns为所需变量
    """

    path = tup[0]  # 缓存路径
    if os.path.splitext(path)[-1] != '.dat': return
    old_path = tup[1]  # 源路径，需要获取月份

    # try:
    if 'H' in os.path.basename(path):
        # print(base_file_name(path))
        file_name  = os.path.basename(path).split('_')[0]
        with ShouPDA(path, name_target=None, down_sample=15) as file:
            file.load_data()
            analog_data = pd.DataFrame(file.analog_data)
            digital_data = pd.DataFrame(file.digital_data)
        analog_data.to_csv(hebing_dir + '/analog/' + '%s.csv' % (file_name), index=False)  # 导出合并文件
        del analog_data
        digital_data.to_csv(hebing_dir + '/digital/' + '%s.csv' % (file_name), index=False)  # 导出合并文件
        del digital_data
    # except:
    #     try:
    #         print(os.path.basename(path), 'error!')
    #         # with open(r'temp\errorlog.txt', mode='a') as f:
    #         #     f.write(old_path + ' ' + 'error' + '\n')
    #     except:
    #         print('写入日志失败')


def walkFile(file):
    import os
    result = []
    for root, dirs, files in os.walk(file):
        # 遍历文件
        for f in files:
            result.append(os.path.join(root, f))
    return result


def make_path(old_pathes, new_dir):
    """将原来的路径转换成缓存路径"""
    new_pathes = []
    for path in old_pathes:
        new_pathes.append(os.path.join(new_dir, os.path.basename(path)))
    return new_pathes


def copy(old_pathes):
    """复制文件至缓存文件夹"""
    new_pathes = make_path(old_pathes, r'D:\多线程暂时存放空间')  # 建立缓存文件路径
    # print(new_pathes)
    for i in range(len(old_pathes)):
        shutil.copyfile(old_pathes[i], new_pathes[i])  # 复制文件


def remove(pathes):
    """删除缓存文件"""
    for i in range(len(pathes)):
        os.remove(pathes[i])  # 删除缓存路径


if __name__ == '__main__':

    # col_data = pd.read_excel(r'E:\钢厂数据\合并文件所需变量.xlsx', header=0)  # 合并文件所需的PDA变量名
    # col_data = pd.read_excel(r'C:\Users\717-2\Desktop\合并文件所需变量.xlsx', header=0)  # 合并文件所需的PDA变量名
    # col_data = pd.read_csv(r'E:\baoSteel\ibaAPI\data\合并文件所需变量.csv', header=0,encoding='gbk')  # 合并文件所需的PDA变量名
    # print(columns)
    from multiprocessing import Process, Manager, Pool, Lock
    import functools
    import os

    result = Manager().list()
    pt = functools.partial(run)
    copy_mission = functools.partial(copy)

    # pathes = walkFile(r'F:\首钢酸轧pda数据完整')  # dat文件的文件夹路径
    pathes = walkFile(r'E:\baoSteel\ibaAPI\test')  # dat文件的文件夹路径

    print('总文件数量', len(pathes))

    # temp_path = r'D:\导出数据临时文件夹'  # 固态缓存路径，一般就是py文件的路径
    temp_path = r'D:\多线程暂时存放空间'  # 固态缓存路径，一般就是py文件的路径

    # os.mkdir(temp_path)

    length = len(pathes)
    size = 12
    for i in range(0, len(pathes), size):
        new_pathes = make_path(pathes[i:i + size], temp_path)
        copy(pathes[i:i + size])
        pool = Pool(size)
        resultList = pool.map(pt, [(o, p) for o, p in zip(new_pathes, pathes[i:i + size])], chunksize=None)
        pool.close()
        pool.join()
        try:
            remove(new_pathes)
        except:
            print('删除缓存失败', new_pathes)
        # new_pathes_temp=new_pathes
        pec = (1 - (i + size) / length) * 100
        print('剩余%.2f' % pec + '%')
        print('=' * 20)
        print('已处理文件数量', i + size)
