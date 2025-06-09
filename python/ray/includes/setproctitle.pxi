import sys
import psutil
import subprocess

from libcpp.string cimport string as c_string
from ray.includes.setproctitle cimport (
    spt_setup,
    set_ps_display
)

current_proctitle = None

def setproctitle(title: str):
    global current_proctitle
    current_proctitle = title
    cdef c_string c_title = title.encode("utf-8")
    spt_setup()
    set_ps_display(c_title.c_str(), True)

def getproctitle() -> str:
    global current_proctitle
    if current_proctitle is None:
        current_proctitle = subprocess.list2cmdline(psutil.Process().cmdline())
    return current_proctitle
