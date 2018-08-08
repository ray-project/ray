"""
This module provider utils for distributing functions.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import hashlib

import ray
import ray.cloudpickle as pickle
import ray.dataflow.execution_info as execution_info
import ray.ray_constants as ray_constants
import ray.utils as utils

# Namespace
EXPORTS = 'Exports'

# Key name constants
FUNCTIONS_TO_RUN = b'FunctionsToRun'
REMOTE_FUNCTION = b'RemoteFunction'
ACTOR_CLASS = b'ActorClass'
CACHED_REMOTE_FUNCTION = 'remote_function'
CACHED_ACTOR = 'actor'

# This must match the definition of NIL_ACTOR_ID in task.h.
NIL_ID = ray_constants.ID_SIZE * b"\xff"
NIL_ACTOR_ID = NIL_ID


class Distributor(execution_info.ExecutionInfo):
    """A class that controls function import & export.
    Attributes:
        worker: the worker object in this process.
        cached_functions_to_run (List): A list of functions to run on all of
            the workers that should be exported as soon as connect is called.
        cached_remote_functions_and_actors: A list of information for exporting
            remote functions and actor classes definitions that were defined
            before the worker called connect. When the worker eventually does
            call connect, if it is a driver, it will export these functions and
            actors. If cached_remote_functions_and_actors is None, that means
            that connect has been called already.
    """

    def __init__(self, worker, polling_interval=0.001):
        super(Distributor, self).__init__()
        self.worker = worker
        self.cached_functions_to_run = []
        self.cached_remote_functions_and_actors = []

        # The interval of checking results.
        self.polling_interval = polling_interval

    def enter_startup(self):
        """Begin caching functions. No works will be done."""
        self.cached_functions_to_run = []
        self.cached_remote_functions_and_actors = []

    def finish_startup(self):
        """Finish caching functions. Start to work."""
        self.cached_functions_to_run = None
        self.cached_remote_functions_and_actors = None

    def is_startup(self):
        return (self.cached_functions_to_run is not None
                and self.cached_remote_functions_and_actors is not None)

    @property
    def mode(self):
        return self.worker.mode

    @property
    def lock(self):
        return self.worker.lock

    @property
    def worker_id(self):
        return self.worker.worker_id

    @property
    def actor_id(self):
        return self.worker.actor_id

    @property
    def redis_client(self):
        return self.worker.redis_client

    @property
    def task_driver_id(self):
        return self.worker.task_driver_id

    def _push_exports(self, key, info):
        self.redis_client.hmset(key, info)
        self.redis_client.rpush(EXPORTS, key)

    def export_remote_function(self, function_id, function_name, function,
                               max_calls, decorated_function):
        """Export a remote function.
        Args:
            function_id: The ID of the function.
            function_name: The name of the function.
            function: The raw undecorated function to export.
            max_calls: The maximum number of times a given worker can execute
                this function before exiting.
            decorated_function: The decorated function (this is used to enable
                the remote function to recursively call itself).
        """
        if not self.worker.is_driver:
            raise Exception("export_remote_function can only be called on a "
                            "driver.")

        key = (REMOTE_FUNCTION + b":" + self.task_driver_id.id() + b":" +
               function_id.id())

        # Work around limitations of Python pickling.
        function_name_global_valid = function.__name__ in function.__globals__
        function_name_global_value = function.__globals__.get(
            function.__name__)
        # Allow the function to reference itself as a global variable
        if not utils.is_cython(function):
            function.__globals__[function.__name__] = decorated_function
        try:
            pickled_function = pickle.dumps(function)
        finally:
            # Undo our changes
            if function_name_global_valid:
                function.__globals__[function.__name__] = (
                    function_name_global_value)
            else:
                del function.__globals__[function.__name__]

        utils.check_oversized_pickle(pickled_function, function_name,
                                     "remote function", self.worker)

        self._push_exports(
            key, {
                "driver_id": self.task_driver_id.id(),
                "function_id": function_id.id(),
                "name": function_name,
                "module": function.__module__,
                "function": pickled_function,
                "max_calls": max_calls
            })

    def run_function_on_all_workers(self, function,
                                    run_on_other_drivers=False):
        """Run arbitrary code on all of the workers.
        This function will first be run on the driver, and then it will be
        exported to all of the workers to be run. It will also be run on any
        new workers that register later. If ray.init has not been called yet,
        then cache the function and export it later.
        Args:
            function (Callable): The function to run on all of the workers. It
                should not take any arguments. If it returns anything, its
                return values will not be used.
            run_on_other_drivers: The boolean that indicates whether we want to
                run this funtion on other drivers. One case is we may need to
                share objects across drivers.
        """
        # If ray.init has not been called yet, then cache the function and
        # export it when connect is called. Otherwise, run the function on all
        # workers.
        if self.mode is None:
            self.cached_functions_to_run.append(function)
        else:
            # Attempt to pickle the function before we need it. This could
            # fail, and it is more convenient if the failure happens before we
            # actually run the function locally.
            pickled_function = pickle.dumps(function)

            function_to_run_id = hashlib.sha1(pickled_function).digest()
            key = FUNCTIONS_TO_RUN + b":" + function_to_run_id

            # First run the function on the driver.
            # We always run the task locally.
            function({"worker": self.worker})
            # Check if the function has already been put into redis.
            function_exported = self.redis_client.setnx(b"Lock:" + key, 1)
            if not function_exported:
                # In this case, the function has already been exported, so
                # we don't need to export it again.
                return

            utils.check_oversized_pickle(pickled_function, function.__name__,
                                         "function", self.worker)

            # Run the function on all workers.
            self._push_exports(
                key, {
                    "driver_id": self.task_driver_id.id(),
                    "function_id": function_to_run_id,
                    "function": pickled_function,
                    "run_on_other_drivers": run_on_other_drivers
                })
            # TODO(rkn): If the worker fails after it calls setnx and before it
            # successfully completes the hmset and rpush, then the program will
            # most likely hang. This could be fixed by making these three
            # operations into a transaction (or by implementing a custom
            # command that does all three things).

    def fetch_and_execute_function_to_run(self, key):
        """Run on arbitrary function on the worker."""
        (driver_id, serialized_function,
         run_on_other_drivers) = self.redis_client.hmget(
             key, ["driver_id", "function", "run_on_other_drivers"])

        if (run_on_other_drivers == "False" and self.worker.is_driver
                and driver_id != self.task_driver_id.id()):
            return

        try:
            # Deserialize the function.
            function = pickle.loads(serialized_function)
            # Run the function.
            function({"worker": self.worker})
        except Exception:
            # Log the error message.
            name = function.__name__ if ("function" in locals() and hasattr(
                function, "__name__")) else ""

            # If an exception was thrown when the function was run, we record
            # the traceback and notify the scheduler of the failure.
            self.worker.logger.push_exception_to_driver(
                ray_constants.FUNCTION_TO_RUN_PUSH_ERROR,
                driver_id=driver_id,
                data={"name": name})

    def fetch_and_register_remote_function(self, key):
        """Import a remote function."""
        (driver_id, function_id_str, function_name, serialized_function,
         num_return_vals, module, resources,
         max_calls) = self.redis_client.hmget(key, [
             "driver_id", "function_id", "name", "function", "num_return_vals",
             "module", "resources", "max_calls"
         ])
        function_id = ray.ObjectID(function_id_str)
        function_name = utils.decode(function_name)
        max_calls = int(max_calls)
        module = utils.decode(module)

        # This is a placeholder in case the function can't be unpickled. This
        # will be overwritten if the function is successfully registered.
        def f():
            raise Exception("This function was not imported properly.")

        self.add_function_info(
            driver_id,
            function_id=function_id,
            function=f,
            function_name=function_name,
            max_calls=max_calls)

        try:
            function = pickle.loads(serialized_function)
        except Exception:
            # If an exception was thrown when the remote function was imported,
            # we record the traceback and notify the scheduler of the failure.
            self.worker.logger.push_exception_to_driver(
                ray_constants.REGISTER_REMOTE_FUNCTION_PUSH_ERROR,
                driver_id=driver_id,
                data={
                    "function_id": function_id.id(),
                    "function_name": function_name
                },
                format_exc=True)
        else:
            # TODO(rkn): Why is the below line necessary?
            function.__module__ = module
            self.add_function_info(
                driver_id,
                function_id=function_id,
                function=function,
                function_name=function_name,
                max_calls=max_calls,
                reset_execution_count=False,
            )
            # Add the function to the function table.
            self.redis_client.rpush(b"FunctionTable:" + function_id.id(),
                                    self.worker_id)

    def append_cached_remote_function(self, remote_function):
        self.cached_remote_functions_and_actors.append((CACHED_REMOTE_FUNCTION,
                                                        remote_function))