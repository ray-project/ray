from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.worker import (register_class, error_info, init, connect, disconnect,
                        get, put, wait, remote, log_event, log_span,
                        flush_log, get_gpu_ids)
from ray.actor import actor
from ray.worker import SCRIPT_MODE, WORKER_MODE, PYTHON_MODE, SILENT_MODE
from ray.worker import global_state

# Ray version string. TODO(rkn): This is also defined separately in setup.py.
# Fix this.
__version__ = "0.1.2"

__all__ = ["register_class", "error_info", "init", "connect", "disconnect",
           "get", "put", "wait", "remote", "log_event", "log_span",
           "flush_log", "actor", "get_gpu_ids", "SCRIPT_MODE", "WORKER_MODE",
           "PYTHON_MODE", "SILENT_MODE", "global_state", "__version__"]

import ctypes
# Windows only
if hasattr(ctypes, "windll"):
  # Makes sure that all child processes die when we die. Also makes sure that
  # fatal crashes result in process termination rather than an error dialog
  # (the latter is annoying since we have a lot of processes). This is done by
  # associating all child processes with a "job" object that imposes this
  # behavior.
  (lambda kernel32: (lambda job: (lambda n: kernel32.SetInformationJobObject(job, 9, "\0" * 17 + chr(0x8 | 0x4 | 0x20) + "\0" * (n - 18), n))(0x90 if ctypes.sizeof(ctypes.c_void_p) > ctypes.sizeof(ctypes.c_int) else 0x70) and kernel32.AssignProcessToJobObject(job, ctypes.c_void_p(kernel32.GetCurrentProcess())))(ctypes.c_void_p(kernel32.CreateJobObjectW(None, None))) if kernel32 is not None else None)(ctypes.windll.kernel32)  # noqa: E501
