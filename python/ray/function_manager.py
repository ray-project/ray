from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import hashlib
import inspect
import time
import traceback
from collections import (
    namedtuple,
    defaultdict,
)
import ray
from ray import profiling
from ray import ray_constants
from ray import cloudpickle as pickle
from ray.utils import (
    is_cython,
    check_oversized_pickle,
    decode,
    format_error_message,
    push_error_to_driver
)

NIL_ID = ray_constants.ID_SIZE * b"\xff"
NIL_LOCAL_SCHEDULER_ID = NIL_ID
NIL_FUNCTION_ID = NIL_ID
NIL_ACTOR_ID = NIL_ID
NIL_ACTOR_HANDLE_ID = NIL_ID
NIL_CLIENT_ID = ray_constants.ID_SIZE * b"\xff"


FunctionExecutionInfo = namedtuple(
    "FunctionExecutionInfo", ["function", "function_name", "max_calls"])
"""FunctionExecutionInfo: A named tuple storing remote function information."""


class FunctionManager(object):
    """
    A class used to export/load remote functions.

    Attributes:

    """

    def __init__(self, worker):
        self._worker = worker
        self._functions_to_export = []
        # This field is a dictionary that maps a driver ID to a dictionary of
        # functions (and information about those functions) that have been
        # registered for that driver (this inner dictionary maps function IDs
        # to a FunctionExecutionInfo object. This should only be used on
        # workers that execute remote functions.
        self.function_execution_info = defaultdict(lambda: {})

    def export_cached(self):
        """Export cached remote functions

        Note: this should be called only once when worker is connected.
        """
        for remote_function in self._functions_to_export:
            self._do_export(remote_function)
        self._functions_to_export = None

    def export(self, remote_function):
        """Export a remote function.

        Args:
            remote_function: the RemoteFunction object.
        """
        if self._worker.mode is None:
            # If the worker isn't connected, cache the function
            # and export it later.
            self._functions_to_export.append(remote_function)
            return
        if self._worker.mode != ray.worker.SCRIPT_MODE:
            # Don't need to export if the worker is not a driver.
            return
        self._do_export(remote_function)

    def _do_export(self, remote_function):
        """
        Pickle a remote function and export it to redis.

        Args:
            remote_function: the RemoteFunction object.
        """
        # Work around limitations of Python pickling.
        function = remote_function._function
        function_name_global_valid = function.__name__ in function.__globals__
        function_name_global_value = function.__globals__.get(
            function.__name__)
        # Allow the function to reference itself as a global variable
        if not is_cython(function):
            function.__globals__[function.__name__] = remote_function
        try:
            pickled_function = pickle.dumps(function)
        finally:
            # Undo our changes
            if function_name_global_valid:
                function.__globals__[function.__name__] = (
                    function_name_global_value)
            else:
                del function.__globals__[function.__name__]

        check_oversized_pickle(pickled_function,
                               remote_function._function_name,
                               "remote function", self._worker)

        key = (b"RemoteFunction:" + self._worker.task_driver_id.id() + b":" +
               remote_function._function_id)
        self._worker.redis_client.hmset(
            key, {
                "driver_id": self._worker.task_driver_id.id(),
                "function_id": remote_function._function_id,
                "name": remote_function._function_name,
                "module": function.__module__,
                "function": pickled_function,
                "max_calls": remote_function._max_calls
            })
        self._worker.redis_client.rpush("Exports", key)

    def fetch_and_register_remote_function(self, key):
        """Import a remote function."""
        (driver_id, function_id_str, function_name, serialized_function,
         num_return_vals, module, resources,
         max_calls) = self._worker.redis_client.hmget(key, [
             "driver_id", "function_id", "name", "function", "num_return_vals",
             "module", "resources", "max_calls"
         ])
        function_id = ray.ObjectID(function_id_str)
        function_name = decode(function_name)
        max_calls = int(max_calls)
        module = decode(module)

        # This is a placeholder in case the function can't be unpickled. This
        # will be overwritten if the function is successfully registered.
        def f():
            raise Exception("This function was not imported properly.")

        self.function_execution_info[driver_id][function_id.id()] = (
            FunctionExecutionInfo(
                function=f, function_name=function_name, max_calls=max_calls))
        self._worker.num_task_executions[driver_id][function_id.id()] = 0

        try:
            function = pickle.loads(serialized_function)
        except Exception as e:
            # If an exception was thrown when the remote function was imported,
            # we record the traceback and notify the scheduler of the failure.
            traceback_str = format_error_message(traceback.format_exc())
            # Log the error message.
            push_error_to_driver(
                self._worker,
                ray_constants.REGISTER_REMOTE_FUNCTION_PUSH_ERROR,
                traceback_str,
                driver_id=driver_id,
                data={
                    "function_id": function_id.id(),
                    "function_name": function_name
                })
        else:
            # The below line is necessary. Because in the driver process,
            # if the function is defined in the file where the python script
            # was started from, its module is `__main__`.
            # However in the worker process, the `__main__` module is a
            # different module, which is `default_worker.py`
            function.__module__ = module
            self.function_execution_info[driver_id][
                function_id.id()] = (FunctionExecutionInfo(
                    function=function,
                    function_name=function_name,
                    max_calls=max_calls))
            # Add the function to the function table.
            self._worker.redis_client.rpush(
                b"FunctionTable:" + function_id.id(), self._worker.worker_id)

    def get_execution_info(self, driver_id, function_id):
        """
        Get the FunctionExecutionInfo of a remote function.

        Args:
            driver_id: id of the driver that the function belongs to.
            function_id: id of the function to get.

        Returns:
            A FunctionExecutionInfo object.
        """
        # Wait until the function to be executed has actually been registered
        # on this worker. We will push warnings to the user if we spend too
        # long in this loop.
        with profiling.profile("wait_for_function", worker=self._worker):
            self._wait_for_function(function_id, driver_id)
        return self.function_execution_info[driver_id][function_id.id()]

    def _wait_for_function(self, function_id, driver_id, timeout=10):
        """Wait until the function to be executed is present on this worker.

        This method will simply loop until the import thread has imported the
        relevant function. If we spend too long in this loop, that may indicate
        a problem somewhere and we will push an error message to the user.

        If this worker is an actor, then this will wait until the actor has
        been defined.

        Args:
            function_id (str): The ID of the function that we want to execute.
            driver_id (str): The ID of the driver to push the error message to
                if this times out.
        """
        start_time = time.time()
        # Only send the warning once.
        warning_sent = False
        while True:
            with self._worker.lock:
                if (self._worker.actor_id == NIL_ACTOR_ID
                        and (function_id.id() in
                             self.function_execution_info[driver_id])):
                    break
                elif self._worker.actor_id != NIL_ACTOR_ID and (
                        self._worker.actor_id in self.actors):
                    break
                if time.time() - start_time > timeout:
                    warning_message = ("This worker was asked to execute a "
                                       "function that it does not have "
                                       "registered. You may have to restart "
                                       "Ray.")
                    if not warning_sent:
                        ray.utils.push_error_to_driver(
                            self._worker,
                            ray_constants.WAIT_FOR_FUNCTION_PUSH_ERROR,
                            warning_message,
                            driver_id=driver_id)
                    warning_sent = True
            time.sleep(0.001)
