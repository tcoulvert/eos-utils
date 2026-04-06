import json
import subprocess

import pandas as pd

from eos_utils import copy_eos

def save_file_eos(object_: object, filepath: str, force: bool=False):
    if filepath.startswith('root://'):
        tmp_file = f"tmp_map{hash(filepath)}.{filepath[filepath.rfind('.')+1:]}"
        print('saving tmp file')
        save_file(object_, tmp_file)
        print('tmp file saved, copying to eos')
        copy_eos(tmp_file, filepath, force=force)
        print('eos file copied')
        subprocess.run(['rm', tmp_file])
    else:
        save_file(object_, filepath)
def save_file(object_: object, filepath: str):
    if type(object_) is dict and filepath[filepath.rfind('.')+1:] == 'json': 
        with open(filepath, 'w') as f: json.dump(object_, f)
    elif type(object_) is pd.DataFrame and filepath[filepath.rfind('.')+1:] == 'parquet': 
        object_.to_parquet(filepath)
    else: raise NotImplementedError(f"Filetype saving not yet implemented, currently only supporting (\'dict\' -> \'json\'), and (\'pd.DataFrame\' -> \'pyarrow.parquet\'). Implement your own for more flexibility.")
