import os
import shutil
from datetime import datetime
from pathlib import Path

def create_month_folders(base_dir):
    for month in range(1, 13):
        month_folder = os.path.join(base_dir, f"{month}月")
        os.makedirs(month_folder, exist_ok=True)

def get_file_modification_month(file_path):
    mtime = os.path.getmtime(file_path)
    return datetime.fromtimestamp(mtime).month

def move_file_to_month_folder(file_path, month_folders):
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    target_folder = month_folders[get_file_modification_month(file_path) - 1]
    target_path = os.path.join(target_folder, file_name)

    if os.path.exists(target_path):
        existing_size = os.path.getsize(target_path)
        if existing_size == file_size:
            # If file sizes are the same, overwrite the file
            shutil.move(file_path, target_path)
            return
        else:
            # If file sizes are different, add a suffix and move the file
            base, ext = os.path.splitext(file_name)
            counter = 2
            while os.path.exists(target_path):
                target_path = os.path.join(target_folder, f"{base}({counter}){ext}")
                counter += 1

    shutil.move(file_path, target_path)

# 删除目录下所有空文件夹
def remove_empty_folders(path, remove_root=True):
    if not os.path.isdir(path):
        return
    # remove empty subfolders
    files = os.listdir(path)
    if len(files):
        for f in files:
            fullpath = os.path.join(path, f)
            if os.path.isdir(fullpath):
                remove_empty_folders(fullpath)

    # if folder empty, delete it
    files = os.listdir(path)
    if len(files) == 0 and remove_root:
        print("Removing empty folder:", path)
        os.rmdir(path)


def archive_directory(base_dir):
    create_month_folders(base_dir)
    month_folders = [os.path.join(base_dir, f"{month}月") for month in range(1, 13)]

    for root, _, files in os.walk(base_dir):
        for file in files:
            file_path = os.path.join(root, file)
            if os.path.isfile(file_path):
                move_file_to_month_folder(file_path, month_folders)

if __name__ == "__main__":
    base_directory = r"E:\baoSteel\ibaAPI\test\dat_dir"
    archive_directory(base_directory)
    remove_empty_folders(base_directory)