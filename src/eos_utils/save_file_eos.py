import datetime
import concurrent.futures as ft

from eos_utils import copy_eos, watch_tmp

def blocked_save(tmp_filepath: str, eos_filepath: str, watch_result: ft.Future, ignore_failures: bool=False, **kwargs):
    if watch_result.result(): 
        copy_eos(tmp_filepath, eos_filepath, **kwargs); os.remove(tmp_filepath)
    elif ignore_failures:
        print(f"WARNING: File saving for {tmp_filepath} to {eos_filepath} timed out, and \'ignore_failures\' is set to \'True\'. Continuing with other processes...")
    else: raise TimeoutError(f"ERROR: File saving for {tmp_filepath} to {eos_filepath} timed out, and \'ignore_failures\' is set to \'False\'.")


def save_file_eos(filepath: str, max_workers: int=5, **kwargs):
    if filepath.startswith('root://'):  # EOS redirector prefix
        _filepath_ = f".tmp_save-{hash(filepath+datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f'))}.{filepath[filepath.rfind('.')+1:]}"
        with ft.ThreadPoolExecutor(max_workers=max_workers) as executor:
            watch_future = executor.submit(watch_tmp, _filepath_, **kwargs)
            executor.submit(blocked_save, _filepath_, filepath, watch_future, **kwargs)
    else:
        _filepath_ = filepath
    return _filepath_

