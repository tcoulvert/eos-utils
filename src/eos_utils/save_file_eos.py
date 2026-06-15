import datetime
import os
import threading

from eos_utils import copy_eos, create_lockfile, watch_tmp

def blocked_save(tmp_filepath: str, eos_filepath: str, **kwargs):
    copy_eos(tmp_filepath, eos_filepath, **kwargs)
    os.remove(tmp_filepath)

def save_file_eos(filepath: str, max_workers: int=5, **kwargs):
    if filepath.startswith('root://'):  # EOS redirector prefix
        _filepath_ = f".tmp_save-{hash(filepath+datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f'))}.{filepath[filepath.rfind('.')+1:]}"
        create_lockfile(_filepath_)
        
        # Watch for file to be opened+closed and move to EOS
        lambda_save = lambda: blocked_save(_filepath_, filepath, **kwargs); 
        thread = threading.Thread(target=watch_tmp, args=(_filepath_, lambda_save), kwargs=kwargs)
        thread.start()
    else:
        _filepath_ = filepath
    return _filepath_
