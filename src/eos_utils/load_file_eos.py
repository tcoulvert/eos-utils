import datetime
import os
import threading

from eos_utils import copy_eos, create_lockfile, watch_tmp

def blocked_remove(tmp_filepath: str, **kwargs):
    os.remove(tmp_filepath)

def load_file_eos(filepath: str, **kwargs):
    if filepath.startswith('root://'):  # EOS redirector prefix
        _filepath_ = f".tmp_load-{hash(filepath+datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f'))}.{filepath[filepath.rfind('.')+1:]}"
        create_lockfile(_filepath_)
        copy_eos(filepath, _filepath_, **kwargs)

        # Watch for file to be opened+closed and delete tmp
        lambda_remove = lambda: blocked_remove(_filepath_, **kwargs)
        thread = threading.Thread(target=watch_tmp, args=(_filepath_, lambda_remove), kwargs=kwargs)
        thread.start()
    else:
        _filepath_ = filepath
    return _filepath_
