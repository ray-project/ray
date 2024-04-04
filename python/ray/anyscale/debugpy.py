import logging
import os
import sys

import ray
from ray.util.annotations import DeveloperAPI

log = logging.getLogger(__name__)

POST_MORTEM_ERROR_UUID = "post_mortem_error_uuid"


@DeveloperAPI
def set_trace(breakpoint_uuid=None):
    """Interrupt the flow of the program and drop into the Ray debugger.

    Can be used within a Ray task or actor.
    """
    import debugpy

    if not ray._private.worker.global_worker.debugger_port:
        (host, port) = debugpy.listen(
            (ray._private.worker.global_worker.node_ip_address, 0)
        )
        ray._private.worker.global_worker.set_debugger_port(port)
        print(f"Ray debugger is listening on {host}:{port}")

    with ray._private.worker.global_worker.task_paused_by_debugger():
        debugpy.wait_for_client()

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


def _is_ray_debugger_enabled():
    return "RAY_DEBUG" in os.environ


def _post_mortem():
    return set_trace(POST_MORTEM_ERROR_UUID)
