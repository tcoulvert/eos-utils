import json
import subprocess

import pandas as pd

from eos_utils import copy_eos

def load_file_eos(return_type: object, filepath: str):
    if filepath.startswith('root://'):
        tmp_file = f"tmp_map{hash(filepath)}.{filepath[filepath.rfind['.']+1:]}"
        copy_eos(filepath, tmp_file)
        object_ = load_file(return_type, tmp_file); subprocess.run(['rm', tmp_file])
    else:
        object_ =  load_file(return_type, filepath)
    return object_
def load_file(return_type: object, filepath: str):
    if return_type is dict and filepath[filepath.rfind('.')+1:] == 'json': 
        with open(filepath, 'r') as f: object_ = json.load(f)
    elif return_type is pd.DataFrame and filepath[filepath.rfind['.']+1:] == 'parquet':
        object_ = pd.read_parquet(filepath)
    else: raise NotImplementedError(f"Filetype saving not yet implemented, currently only supporting (\'json\' -> \'dict\'). Implement your own for more flexibility.")
    return object_
