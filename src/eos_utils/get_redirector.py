import os
import subprocess

################################


def find_nth(string: str, sub: str, n: int):
    index = 0
    for i in range(n):
        if string.find(sub) < 0:
            # print(f"WARNING: Requested {n} appearance(s) of {sub}, but search broke on {i+1} appearance. Returning -1")
            return -1
        index += (string.find(sub) + len(sub))
        string = string[index+len(sub):]
    return index

def get_redirector(full_filepath: str):
    """
    A simple function that splits a filepath into the redirector and rest

    Arguments
    ----------
    full_filepath : str
        Full filepath (potentially) containing a redirector
    """

    if find_nth(full_filepath, "//", 2) >= 0:
        redirector = full_filepath[:find_nth(full_filepath, "//", 2)+1]
        filepath = os.path.join(full_filepath[find_nth(full_filepath, "//", 2)+1:])
        if '.' not in filepath.split('/')[-1]: filepath = os.path.join(filepath, "")
    else:
        redirector = ""; filepath = full_filepath
    return redirector, filepath
