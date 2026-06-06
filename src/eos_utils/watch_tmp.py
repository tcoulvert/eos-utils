import fcntl
import time

def watch_tmp(tmp_filepath: str, n: int=1, sleep_time: float=10):
    times_blocked, blocked = 0, False
    
    while not (times_blocked == n and not blocked):
        try: 
            with open(tmp_filepath, 'r') as f: fcntl.flock(fn.fileno(), fcntl.LOCK_NB)
            blocked = False
        except:
            times_blocked += 1; blocked = True
        time.sleep(sleep_time)
    
    return True
