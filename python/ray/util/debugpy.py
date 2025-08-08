import logging
import os
import sys
import threading
import importlib

import ray
from ray._common.network_utils import build_address
from ray.util.annotations import DeveloperAPI

log = logging.getLogger(__name__)

POST_MORTEM_ERROR_UUID = "post_mortem_error_uuid"


def _try_import_debugpy():
    try:
        debugpy = importlib.import_module("debugpy")
        if not hasattr(debugpy, "__version__") or debugpy.__version__ < "1.8.0":
            raise ImportError()
        return debugpy
    except (ModuleNotFoundError, ImportError):
        log.error(
            "Module 'debugpy>=1.8.0' cannot be loaded. "
            "Ray Debugpy Debugger will not work without 'debugpy>=1.8.0' installed. "
            "Install this module using 'pip install debugpy==1.8.0' "
        )
        return None


# A lock to ensure that only one thread can open the debugger port.
debugger_port_lock = threading.Lock()


def _override_breakpoint_hooks():
    """
    This method overrides the breakpoint() function to set_trace()
    so that other threads can reuse the same setup logic.
    This is based on: https://github.com/microsoft/debugpy/blob/ef9a67fe150179ee4df9997f9273723c26687fab/src/debugpy/_vendored/pydevd/pydev_sitecustomize/sitecustomize.py#L87 # noqa: E501
    """
    sys.__breakpointhook__ = set_trace
    sys.breakpointhook = set_trace
    import builtins as __builtin__

    __builtin__.breakpoint = set_trace


def _ensure_debugger_port_open_thread_safe():
    """
    This is a thread safe method that ensure that the debugger port
    is open, and if not, open it.
    """

    # The lock is acquired before checking the debugger port so only
    # one thread can open the debugger port.
    with debugger_port_lock:
        debugpy = _try_import_debugpy()
        if not debugpy:
            return

        debugger_port = ray._private.worker.global_worker.debugger_port
        if not debugger_port:
            (host, port) = debugpy.listen(
                (ray._private.worker.global_worker.node_ip_address, 0)
            )
            ray._private.worker.global_worker.set_debugger_port(port)
            log.info(f"Ray debugger is listening on {build_address(host, port)}")
        else:
            log.info(f"Ray debugger is already open on {debugger_port}")


@DeveloperAPI
def set_trace(breakpoint_uuid=None):
    """Interrupt the flow of the program and drop into the Ray debugger.
    Can be used within a Ray task or actor.
    """
    debugpy = _try_import_debugpy()
    if not debugpy:
        return

    _ensure_debugger_port_open_thread_safe()

    # debugpy overrides the breakpoint() function, so we need to set it back
    # so other threads can reuse it.
    _override_breakpoint_hooks()

    with ray._private.worker.global_worker.worker_paused_by_debugger():
        msg = (
            "Waiting for debugger to attach (see "
            "https://docs.ray.io/en/latest/ray-observability/"
            "ray-distributed-debugger.html)..."
        )
        log.info(msg)
        debugpy.wait_for_client()

    log.info("Debugger client is connected")
    if breakpoint_uuid == POST_MORTEM_ERROR_UUID:
        _debugpy_excepthook()
    else:
        _debugpy_breakpoint()


def _debugpy_breakpoint():
    """
    Drop the user into the debugger on a breakpoint.
    """
    import pydevd

    pydevd.settrace(stop_at_frame=sys._getframe().f_back)


def _debugpy_excepthook():
    """
    Drop the user into the debugger on an unhandled exception.
    """
    import threading

    import pydevd

    py_db = pydevd.get_global_debugger()
    thread = threading.current_thread()
    additional_info = py_db.set_additional_thread_info(thread)
    additional_info.is_tracing += 1
    try:
        error = sys.exc_info()
        py_db.stop_on_unhandled_exception(py_db, thread, additional_info, error)
        sys.excepthook(error[0], error[1], error[2])
    finally:
        additional_info.is_tracing -= 1


def _is_ray_debugger_post_mortem_enabled():
    return os.environ.get("RAY_DEBUG_POST_MORTEM", "0") == "1"


def _post_mortem():
    return set_trace(POST_MORTEM_ERROR_UUID)
