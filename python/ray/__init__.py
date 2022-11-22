# isort: skip_file
import logging
import os
import sys

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

        import pkg_resources

        try:
            version_info = pkg_resources.require("pickle5")
            version = tuple(int(n) for n in version_info[0].version.split("."))
            if version < (0, 0, 10):
                logger.warning(
                    "Although not used by Ray, a version of pickle5 that leaks memory "
                    "is found in the environment. Please run 'pip install pickle5 -U' "
                    "to upgrade."
                )
        except pkg_resources.DistributionNotFound:
            logger.warning(
                "You are using the 'pickle5' module, but "
                "the exact version is unknown (possibly carried as "
                "an internal component by another module). Please "
                "make sure you are using pickle5 >= 0.0.10 because "
                "previous versions may leak memory."
            )

    # MUST add pickle5 to the import path because it will be imported by some
    # raylet modules.
    #
    # When running Python version < 3.8, Ray needs to use pickle5 instead of
    # Python's built-in pickle. Add the directory containing pickle5 to the
    # Python path so that we find the pickle5 version packaged with Ray and
    # not a pre-existing pickle5.
    if sys.version_info < (3, 8):
        pickle5_path = os.path.join(
            os.path.abspath(os.path.dirname(__file__)), "pickle5_files"
        )
        sys.path.insert(0, pickle5_path)

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
                "https://docs.ray.io/en/master/ray-overview/installation.html"
                "#m1-mac-apple-silicon-support for more details."
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
        import ray._private.compat  # noqa: E402

        ray._private.compat.patch_psutil()

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

# Replaced with the current commit when building the wheels.
__commit__ = "{{RAY_COMMIT_SHA}}"
__version__ = "3.0.0.dev0"

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
    TaskID,
    UniqueID,
    Language,
    PlacementGroupID,
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

# We import ray.actor because some code is run in actor.py which initializes
# some functions in the worker.
import ray.actor  # noqa: E402,F401
from ray.actor import method  # noqa: E402,F401

# TODO(qwang): We should remove this exporting in Ray2.0.
from ray.cross_language import java_function, java_actor_class  # noqa: E402,F401
from ray.runtime_context import get_runtime_context  # noqa: E402,F401
from ray import autoscaler  # noqa: E402,F401
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


_subpackages = [
    "data",
    "workflow",
]

__all__ = [
    "__version__",
    "_config",
    "get_runtime_context",
    "actor",
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
    "internal",
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
    "util",
    "wait",
    "widgets",
    "LOCAL_MODE",
    "SCRIPT_MODE",
    "WORKER_MODE",
] + _subpackages

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
    "TaskID",
    "UniqueID",
    "PlacementGroupID",
]

if sys.version_info < (3, 7):
    # TODO(Clark): Remove this one we drop Python 3.6 support.
    from ray import data  # noqa: F401
    from ray import workflow  # noqa: F401
else:
    # Delay importing of expensive, isolated subpackages.
    def __getattr__(name: str):
        import importlib

        if name in _subpackages:
            return importlib.import_module("." + name, __name__)
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


del os
del logging
del sys
