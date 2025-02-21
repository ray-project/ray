# isort: skip_file
from ray._private import log  # isort: skip # noqa: F401
import logging
import os
import sys
from typing import TYPE_CHECKING

log.generate_logging_config()
logger = logging.getLogger(__name__)


def _configure_system():
    import os
    import platform
    import sys

    """Wraps system configuration to avoid 'leaking' variables into ray."""

    # Sanity check pickle5 if it has been installed.
    if "pickle5" in sys.modules:
        if sys.version_info >= (3, 8):
            logger.warning(
                "Package pickle5 becomes unnecessary in Python 3.8 and above. "
                "Its presence may confuse libraries including Ray. "
                "Please uninstall the package."
            )

        import importlib.metadata

        try:
            version_str = importlib.metadata.version("pickle5")
            version = tuple(int(n) for n in version_str.split("."))
            if version < (0, 0, 10):
                logger.warning(
                    "Although not used by Ray, a version of pickle5 that leaks memory "
                    "is found in the environment. Please run 'pip install pickle5 -U' "
                    "to upgrade."
                )
        except importlib.metadata.PackageNotFoundError:
            logger.warning(
                "You are using the 'pickle5' module, but "
                "the exact version is unknown (possibly carried as "
                "an internal component by another module). Please "
                "make sure you are using pickle5 >= 0.0.10 because "
                "previous versions may leak memory."
            )

    # Importing psutil & setproctitle. Must be before ray._raylet is
    # initialized.
    thirdparty_files = os.path.join(
        os.path.abspath(os.path.dirname(__file__)), "thirdparty_files"
    )
    sys.path.insert(0, thirdparty_files)

    if (
        platform.system() == "Linux"
        and "Microsoft".lower() in platform.release().lower()
    ):
        from ray._private import compat  # noqa: E402

        compat.patch_psutil()

    # Expose ray ABI symbols which may be dependent by other shared
    # libraries such as _streaming.so. See BUILD.bazel:_raylet
    python_shared_lib_suffix = ".so" if sys.platform != "win32" else ".pyd"
    so_path = os.path.join(
        os.path.dirname(__file__), "_raylet" + python_shared_lib_suffix
    )
    if os.path.exists(so_path):
        import ctypes
        from ctypes import CDLL

        CDLL(so_path, ctypes.RTLD_GLOBAL)


_configure_system()
# Delete configuration function.
del _configure_system

from ray import _version  # noqa: E402

__commit__ = _version.commit
__version__ = _version.version

import ray._raylet  # noqa: E402

from ray._raylet import (  # noqa: E402,F401
    ActorClassID,
    ActorID,
    NodeID,
    Config as _Config,
    JobID,
    WorkerID,
    FunctionID,
    ObjectID,
    ObjectRef,
    ObjectRefGenerator,
    DynamicObjectRefGenerator,
    TaskID,
    UniqueID,
    Language,
    PlacementGroupID,
    ClusterID,
)

_config = _Config()

from ray._private.state import (  # noqa: E402,F401
    nodes,
    timeline,
    cluster_resources,
    available_resources,
)
from ray._private.worker import (  # noqa: E402,F401
    LOCAL_MODE,
    SCRIPT_MODE,
    WORKER_MODE,
    RESTORE_WORKER_MODE,
    SPILL_WORKER_MODE,
    cancel,
    get,
    get_actor,
    get_gpu_ids,
    init,
    is_initialized,
    put,
    kill,
    remote,
    shutdown,
    wait,
)

from ray._private.ray_logging.logging_config import LoggingConfig  # noqa: E402

# We import ray.actor because some code is run in actor.py which initializes
# some functions in the worker.
import ray.actor  # noqa: E402,F401
from ray.actor import method  # noqa: E402,F401

# TODO(qwang): We should remove this exporting in Ray2.0.
from ray.cross_language import java_function, java_actor_class  # noqa: E402,F401
from ray.runtime_context import get_runtime_context  # noqa: E402,F401
from ray import internal  # noqa: E402,F401
from ray import util  # noqa: E402,F401
from ray import _private  # noqa: E402,F401

# We import ClientBuilder so that modules can inherit from `ray.ClientBuilder`.
from ray.client_builder import client, ClientBuilder  # noqa: E402,F401


class _DeprecationWrapper:
    def __init__(self, name, real_worker):
        self._name = name
        self._real_worker = real_worker
        self._warned = set()

    def __getattr__(self, attr):
        value = getattr(self._real_worker, attr)
        if attr not in self._warned:
            self._warned.add(attr)
            logger.warning(
                f"DeprecationWarning: `ray.{self._name}.{attr}` is a private "
                "attribute and access will be removed in a future Ray version."
            )
        return value


# TODO(ekl) remove this entirely after 3rd party libraries are all migrated.
worker = _DeprecationWrapper("worker", ray._private.worker)
ray_constants = _DeprecationWrapper("ray_constants", ray._private.ray_constants)
serialization = _DeprecationWrapper("serialization", ray._private.serialization)
state = _DeprecationWrapper("state", ray._private.state)


# Pulic Ray APIs
__all__ = [
    "__version__",
    "_config",
    "get_runtime_context",
    "autoscaler",
    "available_resources",
    "cancel",
    "client",
    "ClientBuilder",
    "cluster_resources",
    "get",
    "get_actor",
    "get_gpu_ids",
    "init",
    "is_initialized",
    "java_actor_class",
    "java_function",
    "cpp_function",
    "kill",
    "Language",
    "method",
    "nodes",
    "put",
    "remote",
    "shutdown",
    "show_in_dashboard",
    "timeline",
    "wait",
    "LOCAL_MODE",
    "SCRIPT_MODE",
    "WORKER_MODE",
    "LoggingConfig",
]

# Public APIs that should automatically trigger ray.init().
AUTO_INIT_APIS = {
    "cancel",
    "get",
    "get_actor",
    "get_gpu_ids",
    "kill",
    "put",
    "wait",
    "get_runtime_context",
}

# Public APIs that should not automatically trigger ray.init().
NON_AUTO_INIT_APIS = {
    "ClientBuilder",
    "LOCAL_MODE",
    "Language",
    "SCRIPT_MODE",
    "WORKER_MODE",
    "__version__",
    "_config",
    "autoscaler",
    "available_resources",
    "client",
    "cluster_resources",
    "cpp_function",
    "init",
    "is_initialized",
    "java_actor_class",
    "java_function",
    "method",
    "nodes",
    "remote",
    "show_in_dashboard",
    "shutdown",
    "timeline",
    "LoggingConfig",
}

assert set(__all__) == AUTO_INIT_APIS | NON_AUTO_INIT_APIS
from ray._private.auto_init_hook import wrap_auto_init_for_all_apis  # noqa: E402

wrap_auto_init_for_all_apis(AUTO_INIT_APIS)
del wrap_auto_init_for_all_apis

# Subpackages
__all__ += [
    "actor",
    "autoscaler",
    "data",
    "internal",
    "util",
    "widgets",
    "workflow",
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
    "ObjectRefGenerator",
    "DynamicObjectRefGenerator",
    "TaskID",
    "UniqueID",
    "PlacementGroupID",
]


# Delay importing of expensive, isolated subpackages. Note that for proper type
# checking support these imports must be kept in sync between type checking and
# runtime behavior.
if TYPE_CHECKING:
    from ray import autoscaler
    from ray import data
    from ray import workflow
else:

    def __getattr__(name: str):
        import importlib

        if name in ["data", "workflow", "autoscaler"]:
            return importlib.import_module("." + name, __name__)
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


del os
del logging
del sys
del TYPE_CHECKING
