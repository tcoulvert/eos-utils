import datetime
import glob
import os

from eos_utils.get_redirector import get_redirector
from eos_utils.get_voms import get_voms

def glob_eos(filepath: str, recursive: bool=False, include_hidden: bool=False):
    if filepath.startswith('root://'):
        redirector, filepath = get_redirector(filepath)
        proxy = get_voms()

        if '/**/' in filepath and recursive:
            filepath = filepath.replace('/**/', '/*')
        if '*' in filepath:
            base_filepath, glob_paths = filepath.split('*')[0], filepath.split('*')[1:]
        else:
            base_filepath, glob_paths = filepath, ['']

        list_cmnd = 'ls' + (' -R' if recursive else '') + (' -a' if include_hidden else '')
        tmp_filename = f".tmp_glob-{hash(filepath+datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f'))}.txt"
        os.system(f"xrdfs {redirector} {list_cmnd} {base_filepath} > {tmp_filename}")

        globs = []
        with open(tmp_filename, 'r') as f:
            for line in f:
                stdline = line.strip()
                match_line = stdline[stdline.find(base_filepath)+len(base_filepath):]; save_line = True
                for glob_path in glob_paths:
                    if glob_path in match_line: 
                        match_line = match_line[match_line.find(glob_path)+len(glob_path):]
                    else: save_line = False; break
                if save_line: globs.append(redirector+stdline)
        os.system(f"rm {tmp_filename}")
    else:
        globs = glob.glob(filepath, recursive=recursive, include_hidden=include_hidden)
    return globs
