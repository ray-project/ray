"""
This module provider utils for distributing functions.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import hashlib
import os
import sys
import threading
import time

import redis

import ray
from ray import profiling
import ray.cloudpickle as pickle
from ray.dataflow.execution_info import ExecutionInfo, TasksCache
import ray.ray_constants as ray_constants
import ray.utils as utils

# Namespace
EXPORTS = 'Exports'

# Key name constants
FUNCTIONS_TO_RUN = b'FunctionsToRun'
REMOTE_FUNCTION = b'RemoteFunction'
ACTOR_CLASS = b'ActorClass'

# This must match the definition of NIL_ACTOR_ID in task.h.
NIL_ID = ray_constants.ID_SIZE * b"\xff"
NIL_ACTOR_ID = NIL_ID


class Distributor(ExecutionInfo, TasksCache):
    """A class that controls function import & export.
    Attributes:
        worker: the worker object in this process.

    """

    def __init__(self, worker, polling_interval=0.001):
        super(Distributor, self).__init__()
        TasksCache.__init__(self)

        self.worker = worker
        # The interval of checking results.
        self.polling_interval = polling_interval

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


class DistributorWithActor(Distributor, TasksCache):
    def __init__(self, worker):
        super(DistributorWithActor, self).__init__(worker)
        TasksCache.__init__(self)

    def wait_for_actor_class(self, key):
        """Wait for the actor class key to have been imported by the import
        thread.

        TODO(rkn): It shouldn't be possible to end up in an infinite
        loop here, but we should push an error to the driver if too much time
        is spent here.
        """

        while not self.has_imported_actor(key):
            time.sleep(self.polling_interval)

    def publish_actor_class_to_key(self, key, actor_class_info):
        """Push an actor class definition to Redis.

        The is factored out as a separate function because it is also called
        on cached actor class definitions when a worker connects for the first
        time.

        Args:
            key: The key to store the actor class info at.
            actor_class_info: Information about the actor class.
        """
        # We set the driver ID here because it may not have been
        # available when the actor class was defined.
        actor_class_info["driver_id"] = self.task_driver_id.id()
        self._push_exports(key, actor_class_info)

    def export_for_driver(self):
        # Add the directory containing the script that is running to the Python
        # paths of the workers. Also add the current directory. Note that this
        # assumes that the directory structures on the machines in the clusters
        # are the same.
        script_directory = os.path.abspath(os.path.dirname(sys.argv[0]))
        current_directory = os.path.abspath(os.path.curdir)
        self.run_function_on_all_workers(
            lambda worker_info: sys.path.insert(1, script_directory))
        self.run_function_on_all_workers(
            lambda worker_info: sys.path.insert(1, current_directory))
        # TODO(rkn): Here we first export functions to run, then remote
        # functions. The order matters. For example, one of the functions to
        # run may set the Python path, which is needed to import a module used
        # to define a remote function. We may want to change the order to
        # simply be the order in which the exports were defined on the driver.
        # In addition, we will need to retain the ability to decide what the
        # first few exports are (mostly to set the Python path). Additionally,
        # note that the first exports to be defined on the driver will be the
        # ones defined in separate modules that are imported by the driver.

        self.visit_caches(
            function_executor=self.run_function_on_all_workers,
            remote_function_executor=lambda x: x._export(),
            actor_executor=self.publish_actor_class_to_key)


class DistributorWithImportThread(DistributorWithActor):
    """A thread used to import exports from the driver or other workers.

    Note:
    The driver also has an import thread, which is used only to
    import custom class definitions from calls to register_custom_serializer
    that happen under the hood on workers.
    """

    def __init__(self, worker):
        super(DistributorWithImportThread, self).__init__(worker)

    def start_import_thread(self):
        """Start the import thread."""
        t = threading.Thread(target=self._run)
        # Making the thread a daemon causes it to exit
        # when the main thread exits.
        t.daemon = True
        t.start()

    def _run(self):
        import_pubsub_client = self.redis_client.pubsub()
        # Exports that are published after the call to
        # import_pubsub_client.subscribe and before the call to
        # import_pubsub_client.listen will still be processed in the loop.
        import_pubsub_client.subscribe("__keyspace@0__:" + EXPORTS)
        # Keep track of the number of imports that we've imported.
        num_imported = 0

        # Get the exports that occurred before the call to subscribe.
        with self.lock:
            export_keys = self.redis_client.lrange(EXPORTS, 0, -1)
            for key in export_keys:
                num_imported += 1
                self._process_key(key)
        try:
            for msg in import_pubsub_client.listen():
                with self.lock:
                    if msg["type"] == "subscribe":
                        continue
                    assert msg["data"] == b"rpush"
                    num_imports = self.redis_client.llen(EXPORTS)
                    assert num_imports >= num_imported
                    for i in range(num_imported, num_imports):
                        num_imported += 1
                        key = self.redis_client.lindex(EXPORTS, i)
                        self._process_key(key)
        except redis.ConnectionError:
            # When Redis terminates the listen call will throw a
            # ConnectionError, which we catch here.
            pass

    def _process_key(self, key):
        """Process the given export key from redis."""
        # Handle the driver case first.
        if not self.worker.is_worker:
            if key.startswith(FUNCTIONS_TO_RUN):
                with profiling.profile(
                        "fetch_and_run_function", worker=self.worker):
                    self.fetch_and_execute_function_to_run(key)
            # Return because FunctionsToRun are the only things that
            # the driver should import.
            return

        if key.startswith(REMOTE_FUNCTION):
            with profiling.profile(
                    "register_remote_function", worker=self.worker):
                self.fetch_and_register_remote_function(key)
        elif key.startswith(FUNCTIONS_TO_RUN):
            with profiling.profile(
                    "fetch_and_run_function", worker=self.worker):
                self.fetch_and_execute_function_to_run(key)
        elif key.startswith(ACTOR_CLASS):
            # Keep track of the fact that this actor class has been
            # exported so that we know it is safe to turn this worker
            # into an actor of that class.
            self.add_actor_class(key)
        # TODO(rkn): We may need to bring back the case of
        # fetching actor classes here.
        else:
            raise Exception("This code should be unreachable.")
