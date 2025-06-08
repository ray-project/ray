import sys
import psutil
import subprocess

from libcpp.string cimport string as c_string
from ray.includes.setproctitle cimport setproctitle as c_setproctitle

current_proctitle = None

def setproctitle(title: str):
    global current_proctitle
    current_proctitle = title
    cdef c_string c_title = title.encode("utf-8")
    c_setproctitle(c_title)

def getproctitle() -> str:
    global current_proctitle
    if current_proctitle is None:
        current_proctitle = subprocess.list2cmdline(psutil.Process().cmdline())
    return current_proctitle
