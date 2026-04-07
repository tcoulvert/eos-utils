import subprocess

################################


def get_voms(reqd_hrs: int=5):
    """
    A simple function that checks if the users VOMS proxy is valid for a minimal amount of time

    Optional arguments:
     - reqd_hrs: <int> Number of hours proxy is required to be valid for
    """

    try:
        stat, out = subprocess.getstatusoutput(f"voms-proxy-info -e --valid {reqd_hrs}:00")
    except:
        print(f"ERROR: voms proxy not found or validity less than {reqd_hrs} hours:\n%s", out)
        raise

    try:
        stat, out = subprocess.getstatusoutput("voms-proxy-info -p")
        out = out.strip().split("\n")[-1]
    except:
        print("ERROR: Unable to voms proxy:\n%s", out)
        raise
    
    proxy = out
    return proxy
