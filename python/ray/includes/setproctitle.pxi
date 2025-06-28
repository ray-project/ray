import sys
import psutil
import subprocess
import threading

from libcpp.string cimport string as c_string
from ray.includes.setproctitle cimport (
    spt_setup,
    set_ps_display
)

_current_proctitle = None
_current_proctitle_lock = threading.Lock()

def setproctitle(title: str):
    global _current_proctitle
    cdef c_string c_title = title.encode("utf-8")

    with _current_proctitle_lock:
        spt_setup()
        set_ps_display(c_title.c_str(), True)

        _current_proctitle = title

def getproctitle() -> str:
    global _current_proctitle

    with _current_proctitle_lock:
        if _current_proctitle is None:
            # The process title is not change so getting the process cmdline as the
            # initial title.
            _current_proctitle = subprocess.list2cmdline(psutil.Process().cmdline())
        return _current_proctitle
