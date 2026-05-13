import sys
import psutil
import subprocess
import threading
import logging

from libcpp.string cimport string as c_string
from ray.includes.setproctitle cimport (
    spt_setup,
    set_ps_display
)

_current_proctitle = None
_current_proctitle_lock = threading.Lock()
_logger = logging.getLogger(__name__)
_spt_setup_warning_logged = False
# spt_setup walks environ to recover the original argv memory region; see
# src/ray/thirdparty/setproctitle/spt_setup.c and spt_status.c.
_spt_setup_result = spt_setup()

def setproctitle(title: str):
    global _current_proctitle, _spt_setup_warning_logged
    cdef c_string c_title = title.encode("utf-8")

    with _current_proctitle_lock:
        if _spt_setup_result < 0:
            if not _spt_setup_warning_logged:
                _logger.warning(
                    "Failed to initialize setproctitle; process titles will "
                    "not be updated."
                )
                _spt_setup_warning_logged = True
            return
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
