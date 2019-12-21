from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
from os.path import dirname
import sys

# MUST add pickle5 to the import path because it will be imported by some
# raylet modules.

if "pickle5" in sys.modules:
    raise ImportError("Ray must be imported before pickle5 because Ray "
                      "requires a specific version of pickle5 (which is "
                      "packaged along with Ray).")

# Add the directory containing pickle5 to the Python path so that we find the
# pickle5 version packaged with ray and not a pre-existing pickle5.
pickle5_path = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "pickle5_files")
sys.path.insert(0, pickle5_path)

# Expose ray ABI symbols which may be dependent by other shared
# libraries such as _streaming.so. See BUILD.bazel:_raylet
so_path = os.path.join(dirname(__file__), "_raylet.so")
if os.path.exists(so_path):
    import ctypes
    from ctypes import CDLL
    CDLL(so_path, ctypes.RTLD_GLOBAL)

# MUST import ray._raylet before pyarrow to initialize some global variables.
# It seems the library related to memory allocation in pyarrow will destroy the
# initialization of grpc if we import pyarrow at first.
# NOTE(JoeyJiang): See https://github.com/ray-project/ray/issues/5219 for more
# details.
import ray._raylet  # noqa: E402

if "pyarrow" in sys.modules:
    raise ImportError("Ray must be imported before pyarrow because Ray "
                      "requires a specific version of pyarrow (which is "
                      "packaged along with Ray).")

# Add the directory containing pyarrow to the Python path so that we find the
# pyarrow version packaged with ray and not a pre-existing pyarrow.
pyarrow_path = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "pyarrow_files")
sys.path.insert(0, pyarrow_path)

# See https://github.com/ray-project/ray/issues/131.
helpful_message = """

If you are using Anaconda, try fixing this problem by running:

    conda install libgcc
"""

try:
    import pyarrow  # noqa: F401

    # pyarrow is not imported inside of _raylet because of the issue described
    # above. In order for Cython to compile _raylet, pyarrow is set to None
    # in _raylet instead, so we give _raylet a real reference to it here.
    # We first do the attribute checks here so that building the documentation
    # succeeds without fully installing ray..
    # TODO(edoakes): Fix this.
    if hasattr(ray, "_raylet") and hasattr(ray._raylet, "pyarrow"):
        ray._raylet.pyarrow = pyarrow
except ImportError as e:
    if ((hasattr(e, "msg") and isinstance(e.msg, str)
         and ("libstdc++" in e.msg or "CXX" in e.msg))):
        # This code path should be taken with Python 3.
        e.msg += helpful_message
    elif (hasattr(e, "message") and isinstance(e.message, str)
          and ("libstdc++" in e.message or "CXX" in e.message)):
        # This code path should be taken with Python 2.
        condition = (hasattr(e, "args") and isinstance(e.args, tuple)
                     and len(e.args) == 1 and isinstance(e.args[0], str))
        if condition:
            e.args = (e.args[0] + helpful_message, )
        else:
            if not hasattr(e, "args"):
                e.args = ()
            elif not isinstance(e.args, tuple):
                e.args = (e.args, )
            e.args += (helpful_message, )
    raise

from ray._raylet import (
    ActorCheckpointID,
    ActorClassID,
    ActorID,
    ClientID,
    Config as _Config,
    JobID,
    WorkerID,
    FunctionID,
    ObjectID,
    TaskID,
    UniqueID,
)  # noqa: E402

_config = _Config()

from ray.profiling import profile  # noqa: E402
from ray.state import (global_state, jobs, nodes, tasks, objects, timeline,
                       object_transfer_timeline, cluster_resources,
                       available_resources, errors)  # noqa: E402
from ray.worker import (
    LOCAL_MODE,
    SCRIPT_MODE,
    WORKER_MODE,
    connect,
    disconnect,
    get,
    get_gpu_ids,
    get_resource_ids,
    get_webui_url,
    init,
    is_initialized,
    put,
    register_custom_serializer,
    remote,
    shutdown,
    wait,
)  # noqa: E402
import ray.internal  # noqa: E402
import ray.projects  # noqa: E402
# We import ray.actor because some code is run in actor.py which initializes
# some functions in the worker.
import ray.actor  # noqa: F401
from ray.actor import method  # noqa: E402
from ray.runtime_context import _get_runtime_context  # noqa: E402

# Ray version string.
__version__ = "0.8.0.dev7"

__all__ = [
    "global_state",
    "jobs",
    "nodes",
    "tasks",
    "objects",
    "timeline",
    "object_transfer_timeline",
    "cluster_resources",
    "available_resources",
    "errors",
    "LOCAL_MODE",
    "PYTHON_MODE",
    "SCRIPT_MODE",
    "WORKER_MODE",
    "__version__",
    "_config",
    "_get_runtime_context",
    "actor",
    "connect",
    "disconnect",
    "get",
    "get_gpu_ids",
    "get_resource_ids",
    "get_webui_url",
    "init",
    "internal",
    "is_initialized",
    "method",
    "profile",
    "projects",
    "put",
    "register_custom_serializer",
    "remote",
    "shutdown",
    "wait",
]

# ID types
__all__ += [
    "ActorCheckpointID",
    "ActorClassID",
    "ActorID",
    "ClientID",
    "JobID",
    "WorkerID",
    "FunctionID",
    "ObjectID",
    "TaskID",
    "UniqueID",
]

import ctypes  # noqa: E402
# Windows only
if hasattr(ctypes, "windll"):
    # Makes sure that all child processes die when we die. Also makes sure that
    # fatal crashes result in process termination rather than an error dialog
    # (the latter is annoying since we have a lot of processes). This is done
    # by associating all child processes with a "job" object that imposes this
    # behavior.
    (lambda kernel32: (lambda job: (lambda n: kernel32.SetInformationJobObject(job, 9, "\0" * 17 + chr(0x8 | 0x4 | 0x20) + "\0" * (n - 18), n))(0x90 if ctypes.sizeof(ctypes.c_void_p) > ctypes.sizeof(ctypes.c_int) else 0x70) and kernel32.AssignProcessToJobObject(job, ctypes.c_void_p(kernel32.GetCurrentProcess())))(ctypes.c_void_p(kernel32.CreateJobObjectW(None, None))) if kernel32 is not None else None)(ctypes.windll.kernel32)  # noqa: E501
