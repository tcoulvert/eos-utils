import os

from eos_utils import get_redirector

def file_exists_eos(filepath: str, max_workers: int=5, **kwargs):
    if filepath.startswith('root://'):  # EOS redirector prefix
        redirector, filepath = get_redirector(filepath)
        out = os.system(f"xrdfs {redirector} ls {filepath}")
        return out == 0
    else:
        return os.path.exists(filepath)