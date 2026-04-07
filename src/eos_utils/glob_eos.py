import glob
import json
import subprocess

import pandas as pd

from eos_utils.get_redirector import get_redirector

def glob_eos(filepath: str, recursive: bool=False, include_hidden: bool=False):
    if filepath.startswith('root://'):
        redirector, filepath = get_redirector(filepath)

        globsplit_filepath = filepath.split('**')

        list_cmd = 'ls' + (' -R' if recursive else '') + (' -a' if include_hidden else '')
        out = subprocess.run(['xrdfs', redirector, 'ls -R' if recursive else 'ls', filepath], capture_output=True, text=True)
        copy_eos(filepath, tmp_file)
        object_ = load_file(return_type, tmp_file); 
    else:
        object_ =  glob.glob(filepath, recursive=recursive, include_hidden=include_hidden)
    return object_
