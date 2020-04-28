import os
import logging
from os.path import dirname
import sys

logger = logging.getLogger(__name__)

# MUST add pickle5 to the import path because it will be imported by some
# raylet modules.

if "pickle5" in sys.modules:
    raise ImportError("Ray must be imported before pickle5 because Ray "
                      "requires a specific version of pickle5 (which is "
                      "packaged along with Ray).")

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

# Expose ray ABI symbols which may be dependent by other shared
# libraries such as _streaming.so. See BUILD.bazel:_raylet
python_shared_lib_suffix = ".so" if sys.platform != "win32" else ".pyd"
so_path = os.path.join(dirname(__file__), "_raylet" + python_shared_lib_suffix)
if os.path.exists(so_path):
    import ctypes
    from ctypes import CDLL
    CDLL(so_path, ctypes.RTLD_GLOBAL)

import ray._raylet  # noqa: E402

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
    Language,
)  # noqa: E402

_config = _Config()

from ray.profiling import profile  # noqa: E402
from ray.state import (jobs, nodes, actors, objects, timeline,
                       object_transfer_timeline, cluster_resources,
                       available_resources, errors)  # noqa: E402
from ray.worker import (
    LOCAL_MODE,
    SCRIPT_MODE,
    WORKER_MODE,
    cancel,
    connect,
    disconnect,
    get,
    get_gpu_ids,
    get_resource_ids,
    get_webui_url,
    init,
    is_initialized,
    put,
    kill,
    register_custom_serializer,
    remote,
    shutdown,
    show_in_webui,
    wait,
)  # noqa: E402
import ray.internal  # noqa: E402
import ray.projects  # noqa: E402
# We import ray.actor because some code is run in actor.py which initializes
# some functions in the worker.
import ray.actor  # noqa: F401
from ray.actor import method  # noqa: E402
from ray.cross_language import java_function, java_actor_class  # noqa: E402
from ray import util  # noqa: E402

# Replaced with the current commit when building the wheels.
__commit__ = "{{RAY_COMMIT_SHA}}"
__version__ = "0.8.5"

__all__ = [
    "jobs",
    "nodes",
    "actors",
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
    "cancel",
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
    "kill",
    "register_custom_serializer",
    "remote",
    "shutdown",
    "show_in_webui",
    "wait",
    "Language",
    "java_function",
    "java_actor_class",
    "util",
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
