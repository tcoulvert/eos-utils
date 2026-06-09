import os
import datetime
import concurrent.futures as ft

from eos_utils import copy_eos, watch_tmp

def blocked_remove(tmp_filepath: str, watch_result: ft.Future, ignore_failures: bool=False, **kwargs):
    if watch_result.result(): 
        os.remove(tmp_filepath)
    elif ignore_failures:
        print(f"WARNING: Tmp file deletion for {tmp_filepath} timed out, and \'ignore_failures\' is set to \'True\'. Continuing with other processes...")
    else: raise TimeoutError(f"ERROR: Tmp file deletion for {tmp_filepath} timed out, and \'ignore_failures\' is set to \'False\'.")

def load_file_eos(filepath: str, max_workers: int=5, **kwargs):
    if filepath.startswith('root://'):  # EOS redirector prefix
        _filepath_ = f".tmp_load-{hash(filepath+datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f'))}.{filepath[filepath.rfind('.')+1:]}"
        copy_eos(filepath, _filepath_, **kwargs)
        with ft.ThreadPoolExecutor(max_workers=max_workers) as executor:
            watch_future = executor.submit(watch_tmp, _filepath_, **kwargs)
            executor.submit(blocked_remove, _filepath_, watch_future, **kwargs)
    else:
        _filepath_ = filepath
    return _filepath_