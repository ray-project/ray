from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import hashlib
import importlib
import inspect
import json
import logging
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
    binary_to_hex,
    is_cython,
    is_function_or_method,
    is_class_method,
    check_oversized_pickle,
    decode,
    format_error_message,
    push_error_to_driver,
)

FunctionExecutionInfo = namedtuple("FunctionExecutionInfo",
                                   ["function", "function_name", "max_calls"])
"""FunctionExecutionInfo: A named tuple storing remote function information."""

# Avoid circle import of worker.py.
NIL_FUNCTION_ID = ray_constants.NIL_JOB_ID
logger = logging.getLogger(__name__)


class FunctionDescriptor(object):
    """A class used to describe a python function.

    Attributes:
    """

    def __init__(self,
                 module_name,
                 function_name,
                 class_name="",
                 function_source_hash=b"",
                 is_driver_task=False):
        self._module_name = module_name
        self._class_name = class_name
        self._function_name = function_name
        self._function_source_hash = function_source_hash
        self._is_driver_task = is_driver_task
        self._function_id = self._get_function_id()

    def __repr__(self):
        return ("FunctionDescriptor:" + self._module_name + "." +
                self._class_name + "." + self._function_name + "." +
                binary_to_hex(self._function_source_hash))

    @classmethod
    def from_bytes_list(cls, function_descriptor_list):
        """Create a FunctionDescriptor instance from list of bytes.
        This function is used to load the function descriptor from
        backend data.
        """
        assert isinstance(function_descriptor_list, list)
        if len(function_descriptor_list) == 0:
            # This is a function descriptor of driver task.
            return cls("", "", "", b"", True)
        elif (len(function_descriptor_list) == 3 or
            len(function_descriptor_list) == 4):
            module_name = function_descriptor_list[0].decode()
            class_name = function_descriptor_list[1].decode()
            function_name = function_descriptor_list[2].decode()
            if len(function_descriptor_list) == 4:
                return cls(module_name, function_name, class_name,
                           function_descriptor_list[3])
            else:
                return cls(module_name, function_name, class_name)
        else:
            raise Exception(
                "Invalid input for FunctionDescriptor.from_bytes_list")

    @classmethod
    def from_function(cls, function, function_class=None):
        """Create a FunctionDescriptorm from a function instance.
        function_class could be None if this function does not belong to
        any Classes.
        """
        module_name = function.__module__
        function_name = function.__name__
        if function_class is not None:
            class_name = function_class.__name__
        else:
            class_name = ""

        function_source_hasher = hashlib.sha1()
        try:
            # If we are running a script or are in IPython, include the source
            # code in the hash.
            source = inspect.getsource(function).encode("ascii")
            function_source_hasher.update(source)
            function_source_hash = function_source_hasher.digest()
        except (IOError, OSError, TypeError):
            # Source code may not be available:
            # e.g. Cython or Python interpreter.
            function_source_hash = b""

        return cls(module_name, function_name, class_name,
                   function_source_hash)

    @classmethod
    def from_class(cls, target_class):
        """Create a FunctionDescriptorm for a class."""
        module_name = target_class.__module__
        class_name = target_class.__name__
        return cls(module_name, "", class_name)

    @classmethod
    def create_driver_task(cls):
        return cls("", "", "", b"", True)

    @property
    def module_name(self):
        """str: the module name that the function belongs to."""
        return self._module_name

    @property
    def class_name(self):
        """str: the class name that the function belongs to if exists.
        It could be empty is the function is not a class method.
        """
        return self._class_name

    @property
    def function_name(self):
        """str: the function name of the function."""
        return self._function_name

    @property
    def function_hash(self):
        """str: the hash code of the function source code."""
        return self._function_source_hash

    @property
    def function_id(self):
        return ray.ObjectID(self._function_id)

    def get_actor_descriptor(self):
        """ FunctionDescriptor: a function descriptor
        Return a function descriptor that represent a actor uniquely.
        """
        return self.__class__(self.module_name, "", self.class_name)

    def _get_function_id(self):
        """str: The function id of the function.
        This function id is calculated from all the fields of function
        descriptor.
        """
        if self._is_driver_task:
            return NIL_FUNCTION_ID.id()
        function_id_hash = hashlib.sha1()
        # Include the function module and name in the hash.
        function_id_hash.update(self.module_name.encode("ascii"))
        function_id_hash.update(self.function_name.encode("ascii"))
        function_id_hash.update(self.class_name.encode("ascii"))
        function_id_hash.update(self._function_source_hash)
        # Compute the function ID.
        function_id = function_id_hash.digest()
        assert len(function_id) == ray_constants.ID_SIZE
        return function_id

    def get_function_descriptor_list(self):
        """Return a list of bytes which is needed by the backend."""
        descriptor_list = []
        if self._is_driver_task:
            # Driver task returns an empty list.
            return descriptor_list
        else:
            descriptor_list.append(self.module_name.encode("ascii"))
            descriptor_list.append(self.class_name.encode("ascii"))
            descriptor_list.append(self.function_name.encode("ascii"))
            if len(self._function_source_hash) != 0:
                descriptor_list.append(self._function_source_hash)
            return descriptor_list


class FunctionActorManager(object):
    """A class used to export/load remote functions and actors.

    Attributes:
        _worker: The associated worker that this manager related.
        _functions_to_export: The remote functions to export when
            the worker gets connected.
        _actors_to_export: The actors to export when the worker gets
            connected.
        _function_execution_info: The map from driver_id to finction_id
            and execution_info.
        _num_task_executions: The map from driver_id to function
            execution times.
    """

    def __init__(self, worker):
        self._worker = worker
        self._functions_to_export = []
        self._actors_to_export = []
        # This field is a dictionary that maps a driver ID to a dictionary of
        # functions (and information about those functions) that have been
        # registered for that driver (this inner dictionary maps function IDs
        # to a FunctionExecutionInfo object. This should only be used on
        # workers that execute remote functions.
        self._function_execution_info = defaultdict(lambda: {})
        self._num_task_executions = defaultdict(lambda: {})
        # A set of all of the actor class keys that have been imported by the
        # import thread. It is safe to convert this worker into an actor of
        # these types.
        self.imported_actor_classes = set()
        self._loaded_actor_classes = {}

    def increase_task_counter(self, driver_id, function_descriptor):
        function_id = function_descriptor.function_id.id()
        self._num_task_executions[driver_id][function_id] += 1

    def get_task_counter(self, driver_id, function_descriptor):
        function_id = function_descriptor.function_id.id()
        return self._num_task_executions[driver_id][function_id]

    def export_cached(self):
        """Export cached remote functions

        Note: this should be called only once when worker is connected.
        """
        for remote_function in self._functions_to_export:
            self._do_export(remote_function)
        self._functions_to_export = None
        for info in self._actors_to_export:
            (key, actor_class_info) = info
            self._publish_actor_class_to_key(key, actor_class_info)

    def reset_cache(self):
        self._functions_to_export = []
        self._actors_to_export = []

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
        """Pickle a remote function and export it to redis.

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
               remote_function._function_descriptor.function_id.id())
        self._worker.redis_client.hmset(
            key, {
                "driver_id": self._worker.task_driver_id.id(),
                "function_id": remote_function._function_descriptor.
                function_id.id(),
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

        self._function_execution_info[driver_id][function_id.id()] = (
            FunctionExecutionInfo(
                function=f, function_name=function_name, max_calls=max_calls))
        self._num_task_executions[driver_id][function_id.id()] = 0

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
            self._function_execution_info[driver_id][function_id.id()] = (
                FunctionExecutionInfo(
                    function=function,
                    function_name=function_name,
                    max_calls=max_calls))
            # Add the function to the function table.
            self._worker.redis_client.rpush(
                b"FunctionTable:" + function_id.id(), self._worker.worker_id)

    def get_execution_info(self, driver_id, function_descriptor):
        """Get the FunctionExecutionInfo of a remote function.

        Args:
            driver_id: ID of the driver that the function belongs to.
            function_descriptor: The FunctionDescriptor of the function to get.

        Returns:
            A FunctionExecutionInfo object.
        """
        function_id = function_descriptor.function_id.id()

        # Wait until the function to be executed has actually been
        # registered on this worker. We will push warnings to the user if
        # we spend too long in this loop.
        # The driver function may not be found in worker lib path. Try to load
        # the function from GCS.
        with profiling.profile("wait_for_function", worker=self._worker):
            self._wait_for_function(function_descriptor, driver_id)
        return self._function_execution_info[driver_id][function_id]

    def _wait_for_function(self, function_descriptor, driver_id, timeout=10):
        """Wait until the function to be executed is present on this worker.

        This method will simply loop until the import thread has imported the
        relevant function. If we spend too long in this loop, that may indicate
        a problem somewhere and we will push an error message to the user.

        If this worker is an actor, then this will wait until the actor has
        been defined.

        Args:
            function_descriptor : The FunctionDescriptor of the function that
                we want to execute.
            driver_id (str): The ID of the driver to push the error message to
                if this times out.
        """
        start_time = time.time()
        # Only send the warning once.
        warning_sent = False
        while True:
            with self._worker.lock:
                if (self._worker.actor_id == ray.worker.NIL_ACTOR_ID
                        and (function_descriptor.function_id.id() in
                             self._function_execution_info[driver_id])):
                    break
                elif self._worker.actor_id != ray.worker.NIL_ACTOR_ID and (
                        self._worker.actor_id in self._worker.actors):
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

    def _publish_actor_class_to_key(self, key, actor_class_info):
        """Push an actor class definition to Redis.

        The is factored out as a separate function because it is also called
        on cached actor class definitions when a worker connects for the first
        time.

        Args:
            key: The key to store the actor class info at.
            actor_class_info: Information about the actor class.
            worker: The worker to use to connect to Redis.
        """
        # We set the driver ID here because it may not have been available when
        # the actor class was defined.
        actor_class_info["driver_id"] = self._worker.task_driver_id.id()
        self._worker.redis_client.hmset(key, actor_class_info)
        self._worker.redis_client.rpush("Exports", key)

    def export_actor_class(self, Class, actor_method_names,
                           checkpoint_interval):
        func_desc = FunctionDescriptor.from_class(Class)
        key = b"ActorClass:" + func_desc.function_id.id()
        actor_class_info = {
            "class_name": Class.__name__,
            "module": Class.__module__,
            "class": pickle.dumps(Class),
            "checkpoint_interval": checkpoint_interval,
            "actor_method_names": json.dumps(list(actor_method_names))
        }

        check_oversized_pickle(actor_class_info["class"],
                               actor_class_info["class_name"], "actor",
                               self._worker)

        if self._worker.mode is None:
            # This means that 'ray.init()' has not been called yet and so we
            # must cache the actor class definition and export it when
            # 'ray.init()' is called.
            assert self._actors_to_export is not None
            self._actors_to_export.append((key, actor_class_info))
            # This caching code path is currently not used because we only
            # export actor class definitions lazily when we instantiate the
            # actor for the first time.
            assert False, "This should be unreachable."
        else:
            self._publish_actor_class_to_key(key, actor_class_info)
            # TODO(rkn): Currently we allow actor classes to be defined
            # within tasks. I tried to disable this, but it may be necessary
            # because of https://github.com/ray-project/ray/issues/1146.

    def load_actor(self, driver_id, function_descriptor):
        function_id = function_descriptor.function_id.id()
        actor_class = self._loaded_actor_classes.get(function_id, None)
        if actor_class is None:
            key = b"ActorClass:" + function_descriptor.function_id.id()
            # Wait for the actor class key to have been imported by the
            # import thread. TODO(rkn): It shouldn't be possible to end
            # up in an infinite loop here, but we should push an error to
            # the driver if too much time is spent here.
            while key not in self.imported_actor_classes:
                time.sleep(0.001)
            with self._worker.lock:
                self.fetch_and_register_actor(key)

    def fetch_and_register_actor(self, actor_class_key):
        """Import an actor.

        This will be called by the worker's import thread when the worker
        receives the actor_class export, assuming that the worker is an actor
        for that class.

        Args:
            actor_class_key: The key in Redis to use to fetch the actor.
            worker: The worker to use.
        """
        actor_id_str = self._worker.actor_id
        (driver_id, class_name, module, pickled_class,
         checkpoint_interval,
         actor_method_names) = self._worker.redis_client.hmget(
             actor_class_key, [
                 "driver_id", "class_name", "module", "class",
                 "checkpoint_interval", "actor_method_names"
             ])

        class_name = decode(class_name)
        module = decode(module)
        checkpoint_interval = int(checkpoint_interval)
        actor_method_names = json.loads(decode(actor_method_names))

        # Create a temporary actor with some temporary methods so that if
        # the actor fails to be unpickled, the temporary actor can be used
        # (just to produce error messages and to prevent the driver from
        # hanging).
        class TemporaryActor(object):
            pass

        self._worker.actors[actor_id_str] = TemporaryActor()
        self._worker.actor_checkpoint_interval = checkpoint_interval

        def temporary_actor_method(*xs):
            raise Exception(
                "The actor with name {} failed to be imported, "
                "and so cannot execute this method".format(class_name))

        # Register the actor method executors.
        for actor_method_name in actor_method_names:
            function_descriptor = FunctionDescriptor(module, actor_method_name,
                                                     class_name)
            function_id = function_descriptor.function_id.id()
            temporary_executor = self._make_actor_method_executor(
                actor_method_name,
                temporary_actor_method,
                actor_imported=False)
            self._function_execution_info[driver_id][function_id] = (
                FunctionExecutionInfo(
                    function=temporary_executor,
                    function_name=actor_method_name,
                    max_calls=0))
            self._num_task_executions[driver_id][function_id] = 0

        try:
            unpickled_class = pickle.loads(pickled_class)
            self._worker.actor_class = unpickled_class
        except Exception:
            # If an exception was thrown when the actor was imported, we record
            # the traceback and notify the scheduler of the failure.
            traceback_str = ray.utils.format_error_message(
                traceback.format_exc())
            # Log the error message.
            push_error_to_driver(
                self._worker,
                ray_constants.REGISTER_ACTOR_PUSH_ERROR,
                traceback_str,
                driver_id,
                data={"actor_id": actor_id_str})
            # TODO(rkn): In the future, it might make sense to have the worker
            # exit here. However, currently that would lead to hanging if
            # someone calls ray.get on a method invoked on the actor.
        else:
            # TODO(pcm): Why is the below line necessary?
            unpickled_class.__module__ = module
            self._worker.actors[actor_id_str] = unpickled_class.__new__(
                unpickled_class)

            actor_methods = inspect.getmembers(
                unpickled_class, predicate=is_function_or_method)
            for actor_method_name, actor_method in actor_methods:
                function_descriptor = FunctionDescriptor(
                    module, actor_method_name, class_name)
                function_id = function_descriptor.function_id.id()
                executor = self._make_actor_method_executor(
                    actor_method_name, actor_method, actor_imported=True)
                self._function_execution_info[driver_id][function_id] = (
                    FunctionExecutionInfo(
                        function=executor,
                        function_name=actor_method_name,
                        max_calls=0))
                # We do not set function_properties[driver_id][function_id]
                # because we currently do need the actor worker to submit new
                # tasks for the actor.

    def _make_actor_method_executor(self, method_name, method, actor_imported):
        """Make an executor that wraps a user-defined actor method.

        The wrapped method updates the worker's internal state and performs any
        necessary checkpointing operations.

        Args:
            worker (Worker): The worker that is executing the actor.
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

        def actor_method_executor(dummy_return_id, actor, *args):
            # Update the actor's task counter to reflect the task we're about
            # to execute.
            self._worker.actor_task_counter += 1

            # If this is the first task to execute on the actor, try to resume
            # from a checkpoint.
            if actor_imported and self._worker.actor_task_counter == 1:
                checkpoint_resumed = ray.actor.restore_and_log_checkpoint(
                    self._worker, actor)
                if checkpoint_resumed:
                    # NOTE(swang): Since we did not actually execute the
                    # __init__ method, this will put None as the return value.
                    # If the __init__ method is supposed to return multiple
                    # values, an exception will be logged.
                    return

            # Determine whether we should checkpoint the actor.
            checkpointing_on = (actor_imported
                                and self._worker.actor_checkpoint_interval > 0)
            # We should checkpoint the actor if user checkpointing is on, we've
            # executed checkpoint_interval tasks since the last checkpoint, and
            # the method we're about to execute is not a checkpoint.
            save_checkpoint = (checkpointing_on
                               and (self._worker.actor_task_counter %
                                    self._worker.actor_checkpoint_interval == 0
                                    and method_name != "__ray_checkpoint__"))

            # Execute the assigned method and save a checkpoint if necessary.
            try:
                if is_class_method(method):
                    method_returns = method(*args)
                else:
                    method_returns = method(actor, *args)
            except Exception:
                # Save the checkpoint before allowing the method exception
                # to be thrown.
                if save_checkpoint:
                    ray.actor.save_and_log_checkpoint(self._worker, actor)
                raise
            else:
                # Save the checkpoint before returning the method's return
                # values.
                if save_checkpoint:
                    ray.actor.save_and_log_checkpoint(self._worker, actor)
                return method_returns

        return actor_method_executor
