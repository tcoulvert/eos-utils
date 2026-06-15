import os
import time
from pathlib import Path

def get_lockfilepath(tmp_filepath: str):
    return tmp_filepath+'.lock'

def create_lockfile(tmp_filepath: str):
    Path(get_lockfilepath(tmp_filepath)).touch()

def check_lockfile(tmp_filepath: str):
    return os.path.exists(get_lockfilepath(tmp_filepath))

def delete_lockfile(tmp_filepath: str):
    if check_lockfile(tmp_filepath): os.remove(get_lockfilepath(tmp_filepath))

def watch_tmp(tmp_filepath: str, sleeptime: int=1, timeout: float=600):
    start_watch_time = time.perf_counter()
    time_elapsed = lambda : time.perf_counter() - start_watch_time

    while time_elapsed() < timeout:
        time.sleep(sleeptime)
        if not check_lockfile(tmp_filepath): return True
    return False