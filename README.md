# EOS-Utils

A small project to hold some useful python scripts for working with EOS and the CERN distributed computing cluster systems. A short description of the various files is given below

File copying:
 * `copy_eos.py` - Copies a file (can be a directory or a file) files from a source path (can be local or remote, can include an EOS redirector) to a target path (can be local or remote, can include a redirector). There are optional features such as filepath grepping and filetype restriction. See the arguments of the script for details. 
 * `get_voms.py` - Checks if the user's VOMS proxy is valid for a minimal number of hours, minimal time can be passed as an argument.
