import logging
import os

logger = logging.getLogger(__name__)


def _configure_system():
    import os
    import platform
    import sys
    """Wraps system configuration to avoid 'leaking' variables into ray."""

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

    # Check that grpc can actually be imported on Apple Silicon. Some package
    # managers (such as `pip`) can't properly install the grpcio library yet,
    # so provide a proactive error message if that's the case.
    if platform.system() == "Darwin" and platform.machine() == "arm64":
        try:
            import grpc  # noqa: F401
        except ImportError:
            raise ImportError(
                "Failed to import grpc on Apple Silicon. On Apple"
                " Silicon machines, try `pip uninstall grpcio; conda "
                "install grpcio`. Check out "
                "https://docs.ray.io/en/master/installation.html"
                "#apple-silicon-support for more details.")

    if "OMP_NUM_THREADS" not in os.environ:
        logger.debug("[ray] Forcing OMP_NUM_THREADS=1 to avoid performance "
                     "degradation with many workers (issue #6998). You can "
                     "override this by explicitly setting OMP_NUM_THREADS.")
        os.environ["OMP_NUM_THREADS"] = "1"

    # Add the directory containing pickle5 to the Python path so that we find
    # the pickle5 version packaged with ray and not a pre-existing pickle5.
    pickle5_path = os.path.join(
        os.path.abspath(os.path.dirname(__file__)), "pickle5_files")
    sys.path.insert(0, pickle5_path)

    # Importing psutil & setproctitle. Must be before ray._raylet is
    # initialized.
    thirdparty_files = os.path.join(
        os.path.abspath(os.path.dirname(__file__)), "thirdparty_files")
    sys.path.insert(0, thirdparty_files)

    if sys.platform == "win32":
        import ray._private.compat  # noqa: E402
        ray._private.compat.patch_redis_empty_recv()

    if (platform.system() == "Linux"
            and "Microsoft".lower() in platform.release().lower()):
        import ray._private.compat  # noqa: E402
        ray._private.compat.patch_psutil()

    # Expose ray ABI symbols which may be dependent by other shared
    # libraries such as _streaming.so. See BUILD.bazel:_raylet
    python_shared_lib_suffix = ".so" if sys.platform != "win32" else ".pyd"
    so_path = os.path.join(
        os.path.dirname(__file__), "_raylet" + python_shared_lib_suffix)
    if os.path.exists(so_path):
        import ctypes
        from ctypes import CDLL
        CDLL(so_path, ctypes.RTLD_GLOBAL)


_configure_system()
# Delete configuration function.
del _configure_system

# Replaced with the current commit when building the wheels.
__commit__ = "{{RAY_COMMIT_SHA}}"
__version__ = "2.0.0.dev0"

import ray._raylet  # noqa: E402

from ray._raylet import (  # noqa: E402
    ActorClassID, ActorID, NodeID, Config as _Config, JobID, WorkerID,
    FunctionID, ObjectID, ObjectRef, TaskID, UniqueID, Language,
    PlacementGroupID)

_config = _Config()

from ray.state import (  # noqa: E402
    nodes, timeline, cluster_resources, available_resources,
)
from ray.worker import (  # noqa: E402,F401
    LOCAL_MODE, SCRIPT_MODE, WORKER_MODE, RESTORE_WORKER_MODE,
    SPILL_WORKER_MODE, cancel, get, get_actor, get_gpu_ids, init,
    is_initialized, put, kill, remote, shutdown, wait,
)
import ray.internal  # noqa: E402
# We import ray.actor because some code is run in actor.py which initializes
# some functions in the worker.
import ray.actor  # noqa: E402,F401
from ray.actor import method  # noqa: E402
from ray.cross_language import java_function, java_actor_class  # noqa: E402
from ray.runtime_context import get_runtime_context  # noqa: E402
from ray import data  # noqa: E402,F401
from ray import util  # noqa: E402
from ray import _private  # noqa: E402,F401
from ray import workflow  # noqa: E402,F401
# We import ClientBuilder so that modules can inherit from `ray.ClientBuilder`.
from ray.client_builder import client, ClientBuilder  # noqa: E402

__all__ = [
    "__version__",
    "_config",
    "get_runtime_context",
    "actor",
    "available_resources",
    "cancel",
    "client",
    "ClientBuilder",
    "cluster_resources",
    "data",
    "get",
    "get_actor",
    "get_gpu_ids",
    "init",
    "internal",
    "is_initialized",
    "java_actor_class",
    "java_function",
    "kill",
    "Language",
    "method",
    "nodes",
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


# Remove modules from top-level ray
def _ray_user_setup_function():
    import os
    user_setup_fn = os.environ.get("RAY_USER_SETUP_FUNCTION")
    if user_setup_fn is not None:
        try:
            module_name, fn_name = user_setup_fn.rsplit(".", 1)
            m = __import__(module_name, globals(), locals(), [fn_name])
            getattr(m, fn_name)()
        except Exception as e:
            # We still need to allow ray to be imported, even there is
            # something in the setup function.
            logger.warning(
                f"Failed to run user setup function: {user_setup_fn}. "
                f"Error message {e}")


_ray_user_setup_function()

del os
del logging
del _ray_user_setup_function
