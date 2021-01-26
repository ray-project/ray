import os
import logging
from os.path import dirname
import platform
import sys

logger = logging.getLogger(__name__)

# MUST add pickle5 to the import path because it will be imported by some
# raylet modules.

if "pickle5" in sys.modules:
    import pkg_resources
    try:
        version_info = pkg_resources.require("pickle5")
        version = tuple(int(n) for n in version_info[0].version.split("."))
        if version < (0, 0, 10):
            raise ImportError("You are using an old version of pickle5 "
                              "that leaks memory, please run "
                              "'pip install pickle5 -U' to upgrade")
    except pkg_resources.DistributionNotFound:
        logger.warning("You are using the 'pickle5' module, but "
                       "the exact version is unknown (possibly carried as "
                       "an internal component by another module). Please "
                       "make sure you are using pickle5 >= 0.0.10 because "
                       "previous versions may leak memory.")

if "OMP_NUM_THREADS" not in os.environ:
    logger.debug("[ray] Forcing OMP_NUM_THREADS=1 to avoid performance "
                 "degradation with many workers (issue #6998). You can "
                 "override this by explicitly setting OMP_NUM_THREADS.")
    os.environ["OMP_NUM_THREADS"] = "1"

# Add the directory containing pickle5 to the Python path so that we find the
# pickle5 version packaged with ray and not a pre-existing pickle5.
pickle5_path = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "pickle5_files")
sys.path.insert(0, pickle5_path)

# Importing psutil & setproctitle. Must be before ray._raylet is initialized.
thirdparty_files = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "thirdparty_files")
sys.path.insert(0, thirdparty_files)

if sys.platform == "win32":
    import ray.compat  # noqa: E402
    ray.compat.patch_redis_empty_recv()

if (platform.system() == "Linux"
        and "Microsoft".lower() in platform.release().lower()):
    import ray.compat  # noqa: E402
    ray.compat.patch_psutil()

# Expose ray ABI symbols which may be dependent by other shared
# libraries such as _streaming.so. See BUILD.bazel:_raylet
python_shared_lib_suffix = ".so" if sys.platform != "win32" else ".pyd"
so_path = os.path.join(dirname(__file__), "_raylet" + python_shared_lib_suffix)
if os.path.exists(so_path):
    import ctypes
    from ctypes import CDLL
    CDLL(so_path, ctypes.RTLD_GLOBAL)

import ray._raylet  # noqa: E402

from ray._raylet import (  # noqa: E402
    ActorClassID, ActorID, NodeID, Config as _Config, JobID, WorkerID,
    FunctionID, ObjectID, ObjectRef, TaskID, UniqueID, Language,
    PlacementGroupID,
)

_config = _Config()

from ray.profiling import profile  # noqa: E402
from ray.state import (  # noqa: E402
    jobs, nodes, actors, objects, timeline, object_transfer_timeline,
    cluster_resources, available_resources,
)
from ray.worker import (  # noqa: E402,F401
    LOCAL_MODE, SCRIPT_MODE, WORKER_MODE, RESTORE_WORKER_MODE,
    SPILL_WORKER_MODE, cancel, get, get_actor, get_gpu_ids, get_resource_ids,
    get_dashboard_url, init, is_initialized, put, kill, remote, shutdown,
    show_in_dashboard, wait,
)
import ray.internal  # noqa: E402
# We import ray.actor because some code is run in actor.py which initializes
# some functions in the worker.
import ray.actor  # noqa: E402,F401
from ray.actor import method  # noqa: E402
from ray.cross_language import java_function, java_actor_class  # noqa: E402
from ray.runtime_context import get_runtime_context  # noqa: E402
from ray import util  # noqa: E402

# Replaced with the current commit when building the wheels.
__commit__ = "{{RAY_COMMIT_SHA}}"
__version__ = "2.0.0.dev0"

__all__ = [
    "__version__",
    "_config",
    "get_runtime_context",
    "actor",
    "actors",
    "workers",
    "available_resources",
    "cancel",
    "cluster_resources",
    "get",
    "get_actor",
    "get_gpu_ids",
    "get_resource_ids",
    "get_dashboard_url",
    "init",
    "internal",
    "is_initialized",
    "java_actor_class",
    "java_function",
    "jobs",
    "kill",
    "Language",
    "method",
    "nodes",
    "objects",
    "object_transfer_timeline",
    "profile",
    "put",
    "remote",
    "shutdown",
    "show_in_dashboard",
    "timeline",
    "util",
    "wait",
    "LOCAL_MODE",
    "SCRIPT_MODE",
    "WORKER_MODE",
]

# ID types
__all__ += [
    "ActorClassID",
    "ActorID",
    "NodeID",
    "JobID",
    "WorkerID",
    "FunctionID",
    "ObjectID",
    "ObjectRef",
    "TaskID",
    "UniqueID",
    "PlacementGroupID",
]
