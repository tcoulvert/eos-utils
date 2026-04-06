import os
import subprocess

from .get_voms import get_voms

################################


def find_nth(string: str, sub: str, n: int):
    index = 0
    for i in range(n):
        if string.find(sub) < 0:
            print(f"WARNING: Requested {n} appearance(s) of {sub}, but search broke on {i+1} appearance. Returning -1")
            return -1
        index += (string.find(sub) + len(sub))
        string = string[index+len(sub):]
    return index

def copy_eos(
    origin_filepath: str, destination_filepath: str,
    grep_str: str="...", filetype_str: str="...", force: bool=False, 
    condor: bool=False, output_dir: str=os.path.join(os.getcwd(), ".condor_copy_eos", ""), queue: str="longlunch", memory: str="4GB"
):
    """
    A function that facilitates the transferring of (potentially large and many) files from one location to another, with at least one location being part of the CERN EOS filesystem.

    Required arguments:
     - origin_filepath: <str> Full filepath (including redirector if applicable) for location of directory to transfer from.
     - destination_filepath: <str> Full filepath (including redirector if applicable) for location of directory to transfer to.
    Optional arguments:
     - grep_str: <str> String with which to select (using grep) which files to transfer. Only files whose filepath contains this string at the origin EOS space will be transfered.
     - filetype_str: <str> Filetype of files to transfer, format as \".<filetype>\". Only files whose filetype matches this string will be transfered.
     - force: <bool> Enables forcing the xrdcp (overwriting files if already located at the destination).
    Condor arguments:
     - condor: <bool> Enables transfer with condor.
     - output_dir: <str> Directory for condor files to be dumped into.
     - queue: <str> Queue with which to submit the condor job.
     - memory: <str> RAM with which to submit the condor job.
    """

    if find_nth(origin_filepath, "//", 2) >= 0:
        origin_redirector = origin_filepath[:find_nth(origin_filepath, "//", 2)+1]
        origin_filepath = os.path.join(origin_filepath[find_nth(origin_filepath, "//", 2)+1:])
        if '.' not in origin_filepath.split('/')[-1]: origin_filepath = os.path.join(origin_filepath, "")
    else:
        origin_redirector = ""; origin_filepath = origin_filepath
    if find_nth(destination_filepath, "//", 2) >= 0:
        destination_redirector = destination_filepath[:find_nth(destination_filepath, "//", 2)+1]
        destination_filepath = os.path.join(destination_filepath[find_nth(destination_filepath, "//", 2)+1:])
        if '.' not in destination_filepath.split('/')[-1]: destination_filepath = os.path.join(destination_filepath, "")
    else:
        destination_redirector = ""; destination_filepath = destination_filepath

    if origin_redirector == "" and destination_redirector == "":
        print("ERROR: Both the source and target paths don't have redirectors, wither this is entirely a local copy and should use `cp` or `eoscp`, or you forgot to input the reirectors. Exiting now.")
        return 1
    
    jobs_dir = os.path.join(output_dir, subprocess.getoutput("date +%Y%m%d_%H%M%S"), "")
    if not os.path.exists(jobs_dir): os.makedirs(jobs_dir)

    # Making a temporary file containing a list of all the files that need to be transferred from one EOS space to another
    if grep_str != "...":
        if origin_redirector != "":
            os.system(f"xrdfs {origin_redirector} ls -R {origin_filepath} | grep {grep_str} > temp.txt")
        else:
            os.system(f"ls -R {origin_filepath} | grep {grep_str} > temp.txt")
    else:
        if origin_redirector != "":
            os.system(f"xrdfs {origin_redirector} ls -R {origin_filepath} > temp.txt")
        else:
            os.system(f"ls -R {origin_filepath} > temp.txt")

    # Skimming output and keeping only real files (that have the right filetype, if given)
    files_to_copy = []
    with open("temp.txt", "r") as f:
        for line in f:
            formatted_line = line.rstrip()
            end_of_filepath = formatted_line.split("/")[-1]
            if (
                filetype_str != "..." 
                and end_of_filepath.endswith(filetype_str)
            ) or (
                filetype_str == "..." 
                and end_of_filepath.find(".") != -1
            ): 
                files_to_copy.append(formatted_line[formatted_line.find(origin_filepath)+len(origin_filepath):])

    # Remove already transferred files if not forcing
    if not force:
        skimmed_files_to_copy = []
        for file_to_copy in files_to_copy:
            if destination_redirector != "":
                stat, out = subprocess.getstatusoutput(f"xrdfs {destination_redirector} ls {destination_filepath}{file_to_copy}")
            else:
                stat, out = subprocess.getstatusoutput(f"ls {destination_filepath}{file_to_copy}")
            if stat != 0: skimmed_files_to_copy.append(file_to_copy)
        files_to_copy = skimmed_files_to_copy
    if len(files_to_copy) < 1:
        return 1

    # Deleting the temp file
    os.system(f"rm temp.txt")

    # Get proxy information (required in executable script for this method of running)
    proxy = get_voms()

    if not condor:
        for file_to_copy in files_to_copy:
            cmd = ["xrdcp", "-f", origin_redirector+origin_filepath+file_to_copy, destination_redirector+destination_filepath+file_to_copy]
            if not force:
                cmd.remove("-f")
            subprocess.call(cmd)
    else:
        # Setup the filepaths for the input and output files
        base_name = "copy_eos"
        job_file_executable = os.path.join(jobs_dir, f"{base_name}.sh")
        job_file_submit = os.path.join(jobs_dir, f"{base_name}.sub")
        job_file_out = os.path.join(jobs_dir, f"{base_name}.$(ClusterId).$(ProcId).out")
        job_file_err = os.path.join(jobs_dir, f"{base_name}.$(ClusterId).$(ProcId).err")
        job_file_log = os.path.join(jobs_dir, f"{base_name}.$(ClusterId).log")
        n_jobs = len(files_to_copy)

        # Write the executable file
        with open(job_file_executable, "w") as executable_file:
            # Shabang and x509 proxy
            executable_file.write("#!/bin/bash\n")
            executable_file.write(f"export X509_USER_PROXY={'/srv'+proxy[proxy.rfind('/'):]}\n")

            # Transfer files
            executable_file.write("echo \"Start of job $1\"\n")
            executable_file.write("echo \"-------------------------------------\"\n")

            for i, file_to_copy in enumerate(files_to_copy):
                executable_file.write(f"if [ $1 -eq {i} ]; then\n")
                # executable_file.write(f"    echo \"Transfering {file_to_copy}\"\n")
                executable_file.write(f"    xrdcp{' -f' if force else ''} {origin_redirector}{origin_filepath}{file_to_copy} {destination_redirector}{destination_filepath}{file_to_copy}\n")
                executable_file.write("fi\n")

            executable_file.write("echo \"Finished job $1\"\n")
            executable_file.write("echo \"-------------------------------------\"\n")
        os.system(f"chmod 775 {job_file_executable}")

        # Write the submit file
        with open(job_file_submit, "w") as submit_file:
            submit_file.write(f"executable = {job_file_executable}\n")
            submit_file.write("arguments = $(ProcId)\n")
            submit_file.write(f"output = {job_file_out}\n")
            submit_file.write(f"error = {job_file_err}\n")
            submit_file.write(f"log = {job_file_log}\n")
            submit_file.write(f"request_memory = {memory}\n")
            submit_file.write("getenv = True\n")
            submit_file.write(f'+JobFlavour = "{queue}"\n')
            submit_file.write(f"should_transfer_files = YES\n")
            submit_file.write(f"Transfer_Input_Files = {proxy}\n")
            submit_file.write(f"Transfer_Output_Files = \"\"\n")
            submit_file.write(f'when_to_transfer_output = ON_EXIT\n')

            submit_file.write('on_exit_remove = (ExitBySignal == False) && (ExitCode == 0)\n')
            submit_file.write('max_retries = 0\n')
            submit_file.write(f"queue {n_jobs}\n")

        # Submit the condor jobs
        if os.getcwd().startswith("/eos"):
            # see https://batchdocs.web.cern.ch/troubleshooting/eos.html#no-eos-submission-allowed
            subprocess.run(["condor_submit", "-spool", job_file_submit])
        else:
            subprocess.run("condor_submit {}".format(job_file_submit), shell=True)
