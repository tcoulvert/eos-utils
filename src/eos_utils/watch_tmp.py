import os
import time

def watch_tmp(tmp_filepath: str, n: int=1, sleep_time: float=10, timeout: float=600):
    times_blocked, blocked = 0, False
    
    start_watch_time = time.perf_counter()
    time_elapsed = lambda : time.perf_counter() - start_watch_time
    while True:
        
        if times_blocked == n and not blocked: return True
        if time_elapsed > timeout: return False

        try: 
            os.rename(tmp_filepath, tmp_filepath); blocked = False
        except:
            times_blocked += 1; blocked = True
        time.sleep(sleep_time)
    