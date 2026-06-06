import datetime
import fcntl
import json
from concurrent.futures as ft

from eos_utils import copy_eos, watch_tmp

def blocked_remove(tmp_filepath: str, watch_result: ft.Future, **kwargs):
    if watch_result.result(): 
        os.remove(tmp_filepath)

def load_file_eos(filepath: str, **kwargs):
    if filepath.startswith('root://'):  # EOS redirector prefix
        _filepath_ = f".tmp_load-{hash(filepath+datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f'))}.{filepath[filepath.rfind('.')+1:]}"
        copy_eos(filepath, _filepath_, *kwargs)
        with ft.ThreadPoolExecutor(max_workers=2) as executor:
            watch_future = executor.submit(watch_tmp, _filepath_, *kwargs)
            executor.submit(blocked_remove, _filepath_, watch_future, *kwargs)
    else:
        _filepath_ = filepath
    return _filepath_