# Ray version string
__version__ = "0.1"

import ctypes
# Windows only
if hasattr(ctypes, "windll"):
  # Makes sure that all child processes die when we die
  # Also makes sure that fatal crashes result in process termination rather than an error dialog (the latter is annoying since we have a lot of processes)
  # This is done by associating all child processes with a "job" object that imposes this behavior.
  (lambda kernel32: (lambda job: (lambda n: kernel32.SetInformationJobObject(job, 9, "\0" * 17 + chr(0x8 | 0x4 | 0x20) + "\0" * (n - 18), n))(0x90 if ctypes.sizeof(ctypes.c_void_p) > ctypes.sizeof(ctypes.c_int) else 0x70) and kernel32.AssignProcessToJobObject(job, ctypes.c_void_p(kernel32.GetCurrentProcess())))(ctypes.c_void_p(kernel32.CreateJobObjectW(None, None))) if kernel32 is not None else None)(ctypes.windll.kernel32)

import config
import serialization
from worker import scheduler_info, visualize_computation_graph, task_info, register_module, init, connect, disconnect, get, put, remote, kill_workers, restart_workers_local
from worker import Reusable, reusables
from worker import SCRIPT_MODE, WORKER_MODE, SHELL_MODE, PYTHON_MODE, SILENT_MODE
from libraylib import ObjectID
import internal
