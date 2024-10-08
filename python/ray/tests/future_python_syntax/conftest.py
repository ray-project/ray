import sys

collect_ignore_glob = []

if sys.version_info < (3, 11):
    collect_ignore_glob.append("*_py311.py")
