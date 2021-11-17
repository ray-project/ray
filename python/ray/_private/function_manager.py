import dis
import hashlib
import importlib
import inspect
import json
import logging
import sys
import time
import threading
import traceback
from collections import (
    namedtuple,
    defaultdict,
)

import ray
import ray._private.profiling as profiling
from ray import ray_constants
from ray import cloudpickle as pickle
from ray._raylet import PythonFunctionDescriptor
from ray._private.utils import (
    check_oversized_function,
    ensure_str,
    format_error_message,
)
from ray.ray_constants import KV_NAMESPACE_FUNCTION_TABLE
from ray.util.inspect import (
    is_function_or_method,
    is_class_method,
    is_static_method,
)

FunctionExecutionInfo = namedtuple("FunctionExecutionInfo",
                                   ["function", "function_name", "max_calls"])
"""FunctionExecutionInfo: A named tuple storing remote function information."""

logger = logging.getLogger(__name__)


class FunctionActorManager:
    """A class used to export/load remote functions and actors.
    Attributes:
        _worker: The associated worker that this manager related.
        _functions_to_export: The remote functions to export when
            the worker gets connected.
        _actors_to_export: The actors to export when the worker gets
            connected.
        _function_execution_info: The function_id
            and execution_info.
        _num_task_executions: The function
            execution times.
        imported_actor_classes: The set of actor classes keys (format:
            ActorClass:function_id) that are already in GCS.
    """

    def __init__(self, worker):
        self._worker = worker
        self._functions_to_export = []
        self._actors_to_export = []
        # This field is a dictionary that maps function IDs
        # to a FunctionExecutionInfo object. This should only be used on
        # workers that execute remote functions.
        self._function_execution_info = defaultdict(lambda: {})
        self._num_task_executions = defaultdict(lambda: {})
        # A set of all of the actor class keys that have been imported by the
        # import thread. It is safe to convert this worker into an actor of
        # these types.
        self.imported_actor_classes = set()
        self._loaded_actor_classes = {}
        # Deserialize an ActorHandle will call load_actor_class(). If a
        # function closure captured an ActorHandle, the deserialization of the
        # function will be:
        #     import_thread.py
        #         -> fetch_and_register_remote_function (acquire lock)
        #         -> _load_actor_class_from_gcs (acquire lock, too)
        # So, the lock should be a reentrant lock.
        self.lock = threading.RLock()
        self.cv = threading.Condition(lock=self.lock)
        self.execution_infos = {}

    def increase_task_counter(self, function_descriptor):
        function_id = function_descriptor.function_id
        self._num_task_executions[function_id] += 1

    def get_task_counter(self, function_descriptor):
        function_id = function_descriptor.function_id
        return self._num_task_executions[function_id]

    def compute_collision_identifier(self, function_or_class):
        """The identifier is used to detect excessive duplicate exports.
        The identifier is used to determine when the same function or class is
        exported many times. This can yield false positives.
        Args:
            function_or_class: The function or class to compute an identifier
                for.
        Returns:
            The identifier. Note that different functions or classes can give
                rise to same identifier. However, the same function should
                hopefully always give rise to the same identifier. TODO(rkn):
                verify if this is actually the case. Note that if the
                identifier is incorrect in any way, then we may give warnings
                unnecessarily or fail to give warnings, but the application's
                behavior won't change.
        """
        import io
        string_file = io.StringIO()
        if sys.version_info[1] >= 7:
            dis.dis(function_or_class, file=string_file, depth=2)
        else:
            dis.dis(function_or_class, file=string_file)
        collision_identifier = (
            function_or_class.__name__ + ":" + string_file.getvalue())

        # Return a hash of the identifier in case it is too large.
        return hashlib.sha1(collision_identifier.encode("utf-8")).digest()

    def load_function_or_class_from_local(self, module_name,
                                          function_or_class_name):
        """Try to load a function or class in the module from local."""
        module = importlib.import_module(module_name)
        parts = [part for part in function_or_class_name.split(".") if part]
        object = module
        try:
            for part in parts:
                object = getattr(object, part)
            return object
        except Exception:
            return None

    def export(self, remote_function):
        """Pickle a remote function and export it to redis.
        Args:
            remote_function: the RemoteFunction object.
        """
        if self._worker.load_code_from_local:
            function_descriptor = remote_function._function_descriptor
            module_name, function_name = (
                function_descriptor.module_name,
                function_descriptor.function_name,
            )
            # If the function is dynamic, we still export it to GCS
            # even if load_code_from_local is set True.
            if self.load_function_or_class_from_local(
                    module_name, function_name) is not None:
                return
        function = remote_function._function
        pickled_function = remote_function._pickled_function

        check_oversized_function(pickled_function,
                                 remote_function._function_name,
                                 "remote function", self._worker)
        key = (b"RemoteFunction:" + self._worker.current_job_id.binary() + b":"
               + remote_function._function_descriptor.function_id.binary())
        if self._worker.gcs_client.internal_kv_exists(
                key, KV_NAMESPACE_FUNCTION_TABLE):
            return
        val = pickle.dumps({
            "job_id": self._worker.current_job_id.binary(),
            "function_id": remote_function._function_descriptor.function_id.
            binary(),
            "function_name": remote_function._function_name,
            "module": function.__module__,
            "function": pickled_function,
            "collision_identifier": self.compute_collision_identifier(
                function),
            "max_calls": remote_function._max_calls
        })
        self._worker.gcs_client.internal_kv_put(key, val, True,
                                                KV_NAMESPACE_FUNCTION_TABLE)
        self._worker.redis_client.rpush("Exports", key)

    def fetch_and_register_remote_function(self, key):
        """Import a remote function."""
        vals = self._worker.gcs_client.internal_kv_get(
            key, KV_NAMESPACE_FUNCTION_TABLE)
        if vals is None:
            vals = {}
        else:
            vals = pickle.loads(vals)
        fields = [
            "job_id", "function_id", "function_name", "function", "module",
            "max_calls"
        ]
        (job_id_str, function_id_str, function_name, serialized_function,
         module, max_calls) = (vals.get(field) for field in fields)

        if ray_constants.ISOLATE_EXPORTS and \
                job_id_str != self._worker.current_job_id.binary():
            # A worker only executes tasks from the assigned job.
            # TODO(jjyao): If fetching unrelated remote functions
            # becomes a perf issue, we can also consider having export
            # queue per job.
            return

        function_id = ray.FunctionID(function_id_str)
        job_id = ray.JobID(job_id_str)
        max_calls = int(max_calls)

        # This function is called by ImportThread. This operation needs to be
        # atomic. Otherwise, there is race condition. Another thread may use
        # the temporary function above before the real function is ready.
        with self.lock:
            self._num_task_executions[function_id] = 0

            try:
                function = pickle.loads(serialized_function)
            except Exception:

                # If an exception was thrown when the remote function was
                # imported, we record the traceback and notify the scheduler
                # of the failure.
                traceback_str = format_error_message(traceback.format_exc())

                def f(*args, **kwargs):
                    raise RuntimeError(
                        "The remote function failed to import on the "
                        "worker. This may be because needed library "
                        "dependencies are not installed in the worker "
                        "environment:\n\n{}".format(traceback_str))

                # Use a placeholder method when function pickled failed
                self._function_execution_info[function_id] = (
                    FunctionExecutionInfo(
                        function=f,
                        function_name=function_name,
                        max_calls=max_calls))

                # Log the error message. Log at DEBUG level to avoid overly
                # spamming the log on import failure. The user gets the error
                # via the RuntimeError message above.
                logger.debug("Failed to unpickle the remote function "
                             f"'{function_name}' with "
                             f"function ID {function_id.hex()}. "
                             f"Job ID:{job_id}."
                             f"Traceback:\n{traceback_str}. ")
            else:
                # The below line is necessary. Because in the driver process,
                # if the function is defined in the file where the python
                # script was started from, its module is `__main__`.
                # However in the worker process, the `__main__` module is a
                # different module, which is `default_worker.py`
                function.__module__ = module
                self._function_execution_info[function_id] = (
                    FunctionExecutionInfo(
                        function=function,
                        function_name=function_name,
                        max_calls=max_calls))
                # Add the function to the function table.
                self._worker.redis_client.rpush(
                    b"FunctionTable:" + function_id.binary(),
                    self._worker.worker_id)

    def get_execution_info(self, job_id, function_descriptor):
        """Get the FunctionExecutionInfo of a remote function.
        Args:
            job_id: ID of the job that the function belongs to.
            function_descriptor: The FunctionDescriptor of the function to get.
        Returns:
            A FunctionExecutionInfo object.
        """
        function_id = function_descriptor.function_id
        # If the function has already been loaded,
        # There's no need to load again
        if function_id in self._function_execution_info:
            return self._function_execution_info[function_id]
        if self._worker.load_code_from_local:
            # Load function from local code.
            if not function_descriptor.is_actor_method():
                # If the function is not able to be loaded,
                # try to load it from GCS,
                # even if load_code_from_local is set True
                if self._load_function_from_local(function_descriptor) is True:
                    return self._function_execution_info[function_id]
        # Load function from GCS.
        # Wait until the function to be executed has actually been
        # registered on this worker. We will push warnings to the user if
        # we spend too long in this loop.
        # The driver function may not be found in sys.path. Try to load
        # the function from GCS.
        with profiling.profile("wait_for_function"):
            self._wait_for_function(function_descriptor, job_id)
        try:
            function_id = function_descriptor.function_id
            info = self._function_execution_info[function_id]
        except KeyError as e:
            message = ("Error occurs in get_execution_info: "
                       "job_id: %s, function_descriptor: %s. Message: %s" %
                       (job_id, function_descriptor, e))
            raise KeyError(message)
        return info

    def _load_function_from_local(self, function_descriptor):
        assert not function_descriptor.is_actor_method()
        function_id = function_descriptor.function_id

        module_name, function_name = (
            function_descriptor.module_name,
            function_descriptor.function_name,
        )

        object = self.load_function_or_class_from_local(
            module_name, function_name)
        if object is not None:
            function = object._function
            self._function_execution_info[function_id] = (
                FunctionExecutionInfo(
                    function=function,
                    function_name=function_name,
                    max_calls=0,
                ))
            self._num_task_executions[function_id] = 0
            return True
        else:
            return False

    def _wait_for_function(self, function_descriptor, job_id, timeout=10):
        """Wait until the function to be executed is present on this worker.
        This method will simply loop until the import thread has imported the
        relevant function. If we spend too long in this loop, that may indicate
        a problem somewhere and we will push an error message to the user.
        If this worker is an actor, then this will wait until the actor has
        been defined.
        Args:
            function_descriptor : The FunctionDescriptor of the function that
                we want to execute.
            job_id (str): The ID of the job to push the error message to
                if this times out.
        """
        start_time = time.time()
        # Only send the warning once.
        warning_sent = False
        while True:
            with self.lock:
                if (self._worker.actor_id.is_nil()
                        and (function_descriptor.function_id in
                             self._function_execution_info)):
                    break
                elif not self._worker.actor_id.is_nil() and (
                        self._worker.actor_id in self._worker.actors):
                    break
            if time.time() - start_time > timeout:
                warning_message = ("This worker was asked to execute a "
                                   "function that it does not have "
                                   "registered. You may have to restart "
                                   "Ray.")
                if not warning_sent:
                    ray._private.utils.push_error_to_driver(
                        self._worker,
                        ray_constants.WAIT_FOR_FUNCTION_PUSH_ERROR,
                        warning_message,
                        job_id=job_id)
                warning_sent = True
            time.sleep(0.001)

    def _publish_actor_class_to_key(self, key, actor_class_info):
        """Push an actor class definition to Redis.
        The is factored out as a separate function because it is also called
        on cached actor class definitions when a worker connects for the first
        time.
        Args:
            key: The key to store the actor class info at.
            actor_class_info: Information about the actor class.
        """
        # We set the driver ID here because it may not have been available when
        # the actor class was defined.
        self._worker.gcs_client.internal_kv_put(key,
                                                pickle.dumps(actor_class_info),
                                                True,
                                                KV_NAMESPACE_FUNCTION_TABLE)
        self._worker.redis_client.rpush("Exports", key)

    def export_actor_class(self, Class, actor_creation_function_descriptor,
                           actor_method_names):
        if self._worker.load_code_from_local:
            module_name, class_name = (
                actor_creation_function_descriptor.module_name,
                actor_creation_function_descriptor.class_name)
            # If the class is dynamic, we still export it to GCS
            # even if load_code_from_local is set True.
            if self.load_function_or_class_from_local(module_name,
                                                      class_name) is not None:
                return

        # `current_job_id` shouldn't be NIL, unless:
        # 1) This worker isn't an actor;
        # 2) And a previous task started a background thread, which didn't
        #    finish before the task finished, and still uses Ray API
        #    after that.
        assert not self._worker.current_job_id.is_nil(), (
            "You might have started a background thread in a non-actor "
            "task, please make sure the thread finishes before the "
            "task finishes.")
        job_id = self._worker.current_job_id
        key = (b"ActorClass:" + job_id.binary() + b":" +
               actor_creation_function_descriptor.function_id.binary())
        try:
            serialized_actor_class = pickle.dumps(Class)
        except TypeError as e:
            msg = (
                "Could not serialize the actor class "
                f"{actor_creation_function_descriptor.repr}. "
                "Check https://docs.ray.io/en/master/serialization.html#troubleshooting "  # noqa
                "for more information.")
            raise TypeError(msg) from e
        actor_class_info = {
            "class_name": actor_creation_function_descriptor.class_name.split(
                ".")[-1],
            "module": actor_creation_function_descriptor.module_name,
            "class": serialized_actor_class,
            "job_id": job_id.binary(),
            "collision_identifier": self.compute_collision_identifier(Class),
            "actor_method_names": json.dumps(list(actor_method_names))
        }

        check_oversized_function(actor_class_info["class"],
                                 actor_class_info["class_name"], "actor",
                                 self._worker)

        self._publish_actor_class_to_key(key, actor_class_info)
        # TODO(rkn): Currently we allow actor classes to be defined
        # within tasks. I tried to disable this, but it may be necessary
        # because of https://github.com/ray-project/ray/issues/1146.

    def load_actor_class(self, job_id, actor_creation_function_descriptor):
        """Load the actor class.
        Args:
            job_id: job ID of the actor.
            actor_creation_function_descriptor: Function descriptor of
                the actor constructor.
        Returns:
            The actor class.
        """
        function_id = actor_creation_function_descriptor.function_id
        # Check if the actor class already exists in the cache.
        actor_class = self._loaded_actor_classes.get(function_id, None)
        if actor_class is None:
            # Load actor class.
            if self._worker.load_code_from_local:
                # Load actor class from local code first.
                actor_class = self._load_actor_class_from_local(
                    actor_creation_function_descriptor)
                # If the actor is unable to be loaded
                # from local, try to load it
                # from GCS even if load_code_from_local is set True
                if actor_class is None:
                    actor_class = self._load_actor_class_from_gcs(
                        job_id, actor_creation_function_descriptor)

            else:
                # Load actor class from GCS.
                actor_class = self._load_actor_class_from_gcs(
                    job_id, actor_creation_function_descriptor)
            # Save the loaded actor class in cache.
            self._loaded_actor_classes[function_id] = actor_class

            # Generate execution info for the methods of this actor class.
            module_name = actor_creation_function_descriptor.module_name
            actor_class_name = actor_creation_function_descriptor.class_name
            actor_methods = inspect.getmembers(
                actor_class, predicate=is_function_or_method)
            for actor_method_name, actor_method in actor_methods:
                # Actor creation function descriptor use a unique function
                # hash to solve actor name conflict. When constructing an
                # actor, the actor creation function descriptor will be the
                # key to find __init__ method execution info. So, here we
                # use actor creation function descriptor as method descriptor
                # for generating __init__ method execution info.
                if actor_method_name == "__init__":
                    method_descriptor = actor_creation_function_descriptor
                else:
                    method_descriptor = PythonFunctionDescriptor(
                        module_name, actor_method_name, actor_class_name)
                method_id = method_descriptor.function_id
                executor = self._make_actor_method_executor(
                    actor_method_name,
                    actor_method,
                    actor_imported=True,
                )
                self._function_execution_info[method_id] = (
                    FunctionExecutionInfo(
                        function=executor,
                        function_name=actor_method_name,
                        max_calls=0,
                    ))
                self._num_task_executions[method_id] = 0
            self._num_task_executions[function_id] = 0
        return actor_class

    def _load_actor_class_from_local(self, actor_creation_function_descriptor):
        """Load actor class from local code."""
        module_name, class_name = (
            actor_creation_function_descriptor.module_name,
            actor_creation_function_descriptor.class_name)

        object = self.load_function_or_class_from_local(
            module_name, class_name)

        if object is not None:
            if isinstance(object, ray.actor.ActorClass):
                return object.__ray_metadata__.modified_class
            else:
                return object
        else:
            return None

    def _create_fake_actor_class(self, actor_class_name, actor_method_names,
                                 traceback_str):
        class TemporaryActor:
            pass

        def temporary_actor_method(*args, **kwargs):
            raise RuntimeError(
                f"The actor with name {actor_class_name} "
                "failed to import on the worker. This may be because "
                "needed library dependencies are not installed in the "
                f"worker environment:\n\n{traceback_str}")

        for method in actor_method_names:
            setattr(TemporaryActor, method, temporary_actor_method)

        return TemporaryActor

    def _load_actor_class_from_gcs(self, job_id,
                                   actor_creation_function_descriptor):
        """Load actor class from GCS."""
        key = (b"ActorClass:" + job_id.binary() + b":" +
               actor_creation_function_descriptor.function_id.binary())
        # Wait for the actor class key to have been imported by the
        # import thread. TODO(rkn): It shouldn't be possible to end
        # up in an infinite loop here, but we should push an error to
        # the driver if too much time is spent here.
        while key not in self.imported_actor_classes:
            try:
                # If we're in the process of deserializing an ActorHandle
                # and we hold the function_manager lock, we may be blocking
                # the import_thread from loading the actor class. Use cv.wait
                # to temporarily yield control to the import thread.
                self.cv.wait()
            except RuntimeError:
                # We don't hold the function_manager lock, just sleep regularly
                time.sleep(0.001)

        # Fetch raw data from GCS.
        vals = self._worker.gcs_client.internal_kv_get(
            key, KV_NAMESPACE_FUNCTION_TABLE)
        fields = [
            "job_id", "class_name", "module", "class", "actor_method_names"
        ]
        if vals is None:
            vals = {}
        else:
            vals = pickle.loads(vals)
        (job_id_str, class_name, module, pickled_class,
         actor_method_names) = (vals.get(field) for field in fields)

        class_name = ensure_str(class_name)
        module_name = ensure_str(module)
        job_id = ray.JobID(job_id_str)
        actor_method_names = json.loads(ensure_str(actor_method_names))

        actor_class = None
        try:
            with self.lock:
                actor_class = pickle.loads(pickled_class)
        except Exception:
            logger.debug("Failed to load actor class %s.", class_name)
            # If an exception was thrown when the actor was imported, we record
            # the traceback and notify the scheduler of the failure.
            traceback_str = format_error_message(traceback.format_exc())
            # The actor class failed to be unpickled, create a fake actor
            # class instead (just to produce error messages and to prevent
            # the driver from hanging).
            actor_class = self._create_fake_actor_class(
                class_name, actor_method_names, traceback_str)

        # The below line is necessary. Because in the driver process,
        # if the function is defined in the file where the python script
        # was started from, its module is `__main__`.
        # However in the worker process, the `__main__` module is a
        # different module, which is `default_worker.py`
        actor_class.__module__ = module_name
        return actor_class

    def _make_actor_method_executor(self, method_name, method, actor_imported):
        """Make an executor that wraps a user-defined actor method.
        The wrapped method updates the worker's internal state and performs any
        necessary checkpointing operations.
        Args:
            method_name (str): The name of the actor method.
            method (instancemethod): The actor method to wrap. This should be a
                method defined on the actor class and should therefore take an
                instance of the actor as the first argument.
            actor_imported (bool): Whether the actor has been imported.
                Checkpointing operations will not be run if this is set to
                False.
        Returns:
            A function that executes the given actor method on the worker's
                stored instance of the actor. The function also updates the
                worker's internal state to record the executed method.
        """

        def actor_method_executor(__ray_actor, *args, **kwargs):
            # Execute the assigned method.
            is_bound = (is_class_method(method)
                        or is_static_method(type(__ray_actor), method_name))
            if is_bound:
                return method(*args, **kwargs)
            else:
                return method(__ray_actor, *args, **kwargs)

        # Set method_name and method as attributes to the executor closure
        # so we can make decision based on these attributes in task executor.
        # Precisely, asyncio support requires to know whether:
        # - the method is a ray internal method: starts with __ray
        # - the method is a coroutine function: defined by async def
        actor_method_executor.name = method_name
        actor_method_executor.method = method

        return actor_method_executor
