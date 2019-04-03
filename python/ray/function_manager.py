from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import hashlib
import importlib
import inspect
import json
import logging
import sys
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
    ensure_str,
    format_error_message,
    push_error_to_driver,
)

FunctionExecutionInfo = namedtuple("FunctionExecutionInfo",
                                   ["function", "function_name", "max_calls"])
"""FunctionExecutionInfo: A named tuple storing remote function information."""

logger = logging.getLogger(__name__)


class FunctionDescriptor(object):
    """A class used to describe a python function.

    Attributes:
        module_name: the module name that the function belongs to.
        class_name: the class name that the function belongs to if exists.
            It could be empty is the function is not a class method.
        function_name: the function name of the function.
        function_hash: the hash code of the function source code if the
            function code is available.
        function_id: the function id calculated from this descriptor.
        is_for_driver_task: whether this descriptor is for driver task.
    """

    def __init__(self,
                 module_name,
                 function_name,
                 class_name="",
                 function_source_hash=b""):
        self._module_name = module_name
        self._class_name = class_name
        self._function_name = function_name
        self._function_source_hash = function_source_hash
        self._function_id = self._get_function_id()

    def __repr__(self):
        return ("FunctionDescriptor:" + self._module_name + "." +
                self._class_name + "." + self._function_name + "." +
                binary_to_hex(self._function_source_hash))

    @classmethod
    def from_bytes_list(cls, function_descriptor_list):
        """Create a FunctionDescriptor instance from list of bytes.

        This function is used to create the function descriptor from
        backend data.

        Args:
            cls: Current class which is required argument for classmethod.
            function_descriptor_list: list of bytes to represent the
                function descriptor.

        Returns:
            The FunctionDescriptor instance created from the bytes list.
        """
        assert isinstance(function_descriptor_list, list)
        if len(function_descriptor_list) == 0:
            # This is a function descriptor of driver task.
            return FunctionDescriptor.for_driver_task()
        elif (len(function_descriptor_list) == 3
              or len(function_descriptor_list) == 4):
            module_name = ensure_str(function_descriptor_list[0])
            class_name = ensure_str(function_descriptor_list[1])
            function_name = ensure_str(function_descriptor_list[2])
            if len(function_descriptor_list) == 4:
                return cls(module_name, function_name, class_name,
                           function_descriptor_list[3])
            else:
                return cls(module_name, function_name, class_name)
        else:
            raise Exception(
                "Invalid input for FunctionDescriptor.from_bytes_list")

    @classmethod
    def from_function(cls, function):
        """Create a FunctionDescriptor from a function instance.

        This function is used to create the function descriptor from
        a python function. If a function is a class function, it should
        not be used by this function.

        Args:
            cls: Current class which is required argument for classmethod.
            function: the python function used to create the function
                descriptor.

        Returns:
            The FunctionDescriptor instance created according to the function.
        """
        module_name = function.__module__
        function_name = function.__name__
        class_name = ""

        function_source_hasher = hashlib.sha1()
        try:
            # If we are running a script or are in IPython, include the source
            # code in the hash.
            source = inspect.getsource(function)
            if sys.version_info[0] >= 3:
                source = source.encode()
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
        """Create a FunctionDescriptor from a class.

        Args:
            cls: Current class which is required argument for classmethod.
            target_class: the python class used to create the function
                descriptor.

        Returns:
            The FunctionDescriptor instance created according to the class.
        """
        module_name = target_class.__module__
        class_name = target_class.__name__
        return cls(module_name, "__init__", class_name)

    @classmethod
    def for_driver_task(cls):
        """Create a FunctionDescriptor instance for a driver task."""
        return cls("", "", "", b"")

    @property
    def is_for_driver_task(self):
        """See whether this function descriptor is for a driver or not.

        Returns:
            True if this function descriptor is for driver tasks.
        """
        return all(
            len(x) == 0
            for x in [self.module_name, self.class_name, self.function_name])

    @property
    def module_name(self):
        """Get the module name of current function descriptor.

        Returns:
            The module name of the function descriptor.
        """
        return self._module_name

    @property
    def class_name(self):
        """Get the class name of current function descriptor.

        Returns:
            The class name of the function descriptor. It could be
                empty if the function is not a class method.
        """
        return self._class_name

    @property
    def function_name(self):
        """Get the function name of current function descriptor.

        Returns:
            The function name of the function descriptor.
        """
        return self._function_name

    @property
    def function_hash(self):
        """Get the hash code of the function source code.

        Returns:
            The bytes with length of ray_constants.ID_SIZE if the source
                code is available. Otherwise, the bytes length will be 0.
        """
        return self._function_source_hash

    @property
    def function_id(self):
        """Get the function id calculated from this descriptor.

        Returns:
            The value of ray.ObjectID that represents the function id.
        """
        return self._function_id

    def _get_function_id(self):
        """Calculate the function id of current function descriptor.

        This function id is calculated from all the fields of function
        descriptor.

        Returns:
            ray.ObjectID to represent the function descriptor.
        """
        if self.is_for_driver_task:
            return ray.FunctionID.nil()
        function_id_hash = hashlib.sha1()
        # Include the function module and name in the hash.
        function_id_hash.update(self.module_name.encode("ascii"))
        function_id_hash.update(self.function_name.encode("ascii"))
        function_id_hash.update(self.class_name.encode("ascii"))
        function_id_hash.update(self._function_source_hash)
        # Compute the function ID.
        function_id = function_id_hash.digest()
        return ray.FunctionID(function_id)

    def get_function_descriptor_list(self):
        """Return a list of bytes representing the function descriptor.

        This function is used to pass this function descriptor to backend.

        Returns:
            A list of bytes.
        """
        descriptor_list = []
        if self.is_for_driver_task:
            # Driver task returns an empty list.
            return descriptor_list
        else:
            descriptor_list.append(self.module_name.encode("ascii"))
            descriptor_list.append(self.class_name.encode("ascii"))
            descriptor_list.append(self.function_name.encode("ascii"))
            if len(self._function_source_hash) != 0:
                descriptor_list.append(self._function_source_hash)
            return descriptor_list

    def is_actor_method(self):
        """Wether this function descriptor is an actor method.

        Returns:
            True if it's an actor method, False if it's a normal function.
        """
        return len(self._class_name) > 0


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
        imported_actor_classes: The set of actor classes keys (format:
            ActorClass:function_id) that are already in GCS.
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
        function_id = function_descriptor.function_id
        if self._worker.load_code_from_local:
            driver_id = ray.DriverID.nil()
        self._num_task_executions[driver_id][function_id] += 1

    def get_task_counter(self, driver_id, function_descriptor):
        function_id = function_descriptor.function_id
        if self._worker.load_code_from_local:
            driver_id = ray.DriverID.nil()
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
        if self._worker.load_code_from_local:
            return
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
        key = (b"RemoteFunction:" + self._worker.task_driver_id.binary() + b":"
               + remote_function._function_descriptor.function_id.binary())
        self._worker.redis_client.hmset(
            key, {
                "driver_id": self._worker.task_driver_id.binary(),
                "function_id": remote_function._function_descriptor.
                function_id.binary(),
                "name": remote_function._function_name,
                "module": function.__module__,
                "function": pickled_function,
                "max_calls": remote_function._max_calls
            })
        self._worker.redis_client.rpush("Exports", key)

    def fetch_and_register_remote_function(self, key):
        """Import a remote function."""
        (driver_id_str, function_id_str, function_name, serialized_function,
         num_return_vals, module, resources,
         max_calls) = self._worker.redis_client.hmget(key, [
             "driver_id", "function_id", "name", "function", "num_return_vals",
             "module", "resources", "max_calls"
         ])
        function_id = ray.FunctionID(function_id_str)
        driver_id = ray.DriverID(driver_id_str)
        function_name = decode(function_name)
        max_calls = int(max_calls)
        module = decode(module)

        # This is a placeholder in case the function can't be unpickled. This
        # will be overwritten if the function is successfully registered.
        def f():
            raise Exception("This function was not imported properly.")

        self._function_execution_info[driver_id][function_id] = (
            FunctionExecutionInfo(
                function=f, function_name=function_name, max_calls=max_calls))
        self._num_task_executions[driver_id][function_id] = 0

        try:
            function = pickle.loads(serialized_function)
        except Exception:
            # If an exception was thrown when the remote function was imported,
            # we record the traceback and notify the scheduler of the failure.
            traceback_str = format_error_message(traceback.format_exc())
            # Log the error message.
            push_error_to_driver(
                self._worker,
                ray_constants.REGISTER_REMOTE_FUNCTION_PUSH_ERROR,
                "Failed to unpickle the remote function '{}' with function ID "
                "{}. Traceback:\n{}".format(function_name, function_id.hex(),
                                            traceback_str),
                driver_id=driver_id)
        else:
            # The below line is necessary. Because in the driver process,
            # if the function is defined in the file where the python script
            # was started from, its module is `__main__`.
            # However in the worker process, the `__main__` module is a
            # different module, which is `default_worker.py`
            function.__module__ = module
            self._function_execution_info[driver_id][function_id] = (
                FunctionExecutionInfo(
                    function=function,
                    function_name=function_name,
                    max_calls=max_calls))
            # Add the function to the function table.
            self._worker.redis_client.rpush(
                b"FunctionTable:" + function_id.binary(),
                self._worker.worker_id)

    def get_execution_info(self, driver_id, function_descriptor):
        """Get the FunctionExecutionInfo of a remote function.

        Args:
            driver_id: ID of the driver that the function belongs to.
            function_descriptor: The FunctionDescriptor of the function to get.

        Returns:
            A FunctionExecutionInfo object.
        """
        if self._worker.load_code_from_local:
            # Load function from local code.
            # Currently, we don't support isolating code by drivers,
            # thus always set driver ID to NIL here.
            driver_id = ray.DriverID.nil()
            if not function_descriptor.is_actor_method():
                self._load_function_from_local(driver_id, function_descriptor)
        else:
            # Load function from GCS.
            # Wait until the function to be executed has actually been
            # registered on this worker. We will push warnings to the user if
            # we spend too long in this loop.
            # The driver function may not be found in sys.path. Try to load
            # the function from GCS.
            with profiling.profile("wait_for_function"):
                self._wait_for_function(function_descriptor, driver_id)
        try:
            function_id = function_descriptor.function_id
            info = self._function_execution_info[driver_id][function_id]
        except KeyError as e:
            message = ("Error occurs in get_execution_info: "
                       "driver_id: %s, function_descriptor: %s. Message: %s" %
                       (driver_id, function_descriptor, e))
            raise KeyError(message)
        return info

    def _load_function_from_local(self, driver_id, function_descriptor):
        assert not function_descriptor.is_actor_method()
        function_id = function_descriptor.function_id
        if (driver_id in self._function_execution_info
                and function_id in self._function_execution_info[function_id]):
            return
        module_name, function_name = (
            function_descriptor.module_name,
            function_descriptor.function_name,
        )
        try:
            module = importlib.import_module(module_name)
            function = getattr(module, function_name)._function
            self._function_execution_info[driver_id][function_id] = (
                FunctionExecutionInfo(
                    function=function,
                    function_name=function_name,
                    max_calls=0,
                ))
            self._num_task_executions[driver_id][function_id] = 0
        except Exception:
            logger.exception(
                "Failed to load function %s.".format(function_name))
            raise Exception(
                "Function {} failed to be loaded from local code.".format(
                    function_descriptor))

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
                if (self._worker.actor_id.is_nil()
                        and (function_descriptor.function_id in
                             self._function_execution_info[driver_id])):
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
        """
        # We set the driver ID here because it may not have been available when
        # the actor class was defined.
        self._worker.redis_client.hmset(key, actor_class_info)
        self._worker.redis_client.rpush("Exports", key)

    def export_actor_class(self, Class, actor_method_names):
        if self._worker.load_code_from_local:
            return
        function_descriptor = FunctionDescriptor.from_class(Class)
        # `task_driver_id` shouldn't be NIL, unless:
        # 1) This worker isn't an actor;
        # 2) And a previous task started a background thread, which didn't
        #    finish before the task finished, and still uses Ray API
        #    after that.
        assert not self._worker.task_driver_id.is_nil(), (
            "You might have started a background thread in a non-actor task, "
            "please make sure the thread finishes before the task finishes.")
        driver_id = self._worker.task_driver_id
        key = (b"ActorClass:" + driver_id.binary() + b":" +
               function_descriptor.function_id.binary())
        actor_class_info = {
            "class_name": Class.__name__,
            "module": Class.__module__,
            "class": pickle.dumps(Class),
            "driver_id": driver_id.binary(),
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

    def load_actor_class(self, driver_id, function_descriptor):
        """Load the actor class.

        Args:
            driver_id: Driver ID of the actor.
            function_descriptor: Function descriptor of the actor constructor.

        Returns:
            The actor class.
        """
        function_id = function_descriptor.function_id
        # Check if the actor class already exists in the cache.
        actor_class = self._loaded_actor_classes.get(function_id, None)
        if actor_class is None:
            # Load actor class.
            if self._worker.load_code_from_local:
                driver_id = ray.DriverID.nil()
                # Load actor class from local code.
                actor_class = self._load_actor_from_local(
                    driver_id, function_descriptor)
            else:
                # Load actor class from GCS.
                actor_class = self._load_actor_class_from_gcs(
                    driver_id, function_descriptor)
            # Save the loaded actor class in cache.
            self._loaded_actor_classes[function_id] = actor_class

            # Generate execution info for the methods of this actor class.
            module_name = function_descriptor.module_name
            actor_class_name = function_descriptor.class_name
            actor_methods = inspect.getmembers(
                actor_class, predicate=is_function_or_method)
            for actor_method_name, actor_method in actor_methods:
                method_descriptor = FunctionDescriptor(
                    module_name, actor_method_name, actor_class_name)
                method_id = method_descriptor.function_id
                executor = self._make_actor_method_executor(
                    actor_method_name,
                    actor_method,
                    actor_imported=True,
                )
                self._function_execution_info[driver_id][method_id] = (
                    FunctionExecutionInfo(
                        function=executor,
                        function_name=actor_method_name,
                        max_calls=0,
                    ))
                self._num_task_executions[driver_id][method_id] = 0
            self._num_task_executions[driver_id][function_id] = 0
        return actor_class

    def _load_actor_from_local(self, driver_id, function_descriptor):
        """Load actor class from local code."""
        module_name, class_name = (function_descriptor.module_name,
                                   function_descriptor.class_name)
        try:
            module = importlib.import_module(module_name)
            actor_class = getattr(module, class_name)
            if isinstance(actor_class, ray.actor.ActorClass):
                return actor_class._modified_class
            else:
                return actor_class
        except Exception:
            logger.exception(
                "Failed to load actor_class %s.".format(class_name))
            raise Exception(
                "Actor {} failed to be imported from local code.".format(
                    class_name))

    def _create_fake_actor_class(self, actor_class_name, actor_method_names):
        class TemporaryActor(object):
            pass

        def temporary_actor_method(*xs):
            raise Exception(
                "The actor with name {} failed to be imported, "
                "and so cannot execute this method.".format(actor_class_name))

        for method in actor_method_names:
            setattr(TemporaryActor, method, temporary_actor_method)

        return TemporaryActor

    def _load_actor_class_from_gcs(self, driver_id, function_descriptor):
        """Load actor class from GCS."""
        key = (b"ActorClass:" + driver_id.binary() + b":" +
               function_descriptor.function_id.binary())
        # Wait for the actor class key to have been imported by the
        # import thread. TODO(rkn): It shouldn't be possible to end
        # up in an infinite loop here, but we should push an error to
        # the driver if too much time is spent here.
        while key not in self.imported_actor_classes:
            time.sleep(0.001)

        # Fetch raw data from GCS.
        (driver_id_str, class_name, module, pickled_class,
         actor_method_names) = self._worker.redis_client.hmget(
             key, [
                 "driver_id", "class_name", "module", "class",
                 "actor_method_names"
             ])

        class_name = ensure_str(class_name)
        module_name = ensure_str(module)
        driver_id = ray.DriverID(driver_id_str)
        actor_method_names = json.loads(ensure_str(actor_method_names))

        actor_class = None
        try:
            with self._worker.lock:
                actor_class = pickle.loads(pickled_class)
        except Exception:
            logger.exception(
                "Failed to load actor class %s.".format(class_name))
            # The actor class failed to be unpickled, create a fake actor
            # class instead (just to produce error messages and to prevent
            # the driver from hanging).
            actor_class = self._create_fake_actor_class(
                class_name, actor_method_names)
            # If an exception was thrown when the actor was imported, we record
            # the traceback and notify the scheduler of the failure.
            traceback_str = ray.utils.format_error_message(
                traceback.format_exc())
            # Log the error message.
            push_error_to_driver(
                self._worker, ray_constants.REGISTER_ACTOR_PUSH_ERROR,
                "Failed to unpickle actor class '{}' for actor ID {}. "
                "Traceback:\n{}".format(class_name,
                                        self._worker.actor_id.hex(),
                                        traceback_str), driver_id)
            # TODO(rkn): In the future, it might make sense to have the worker
            # exit here. However, currently that would lead to hanging if
            # someone calls ray.get on a method invoked on the actor.

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

        def actor_method_executor(dummy_return_id, actor, *args):
            # Update the actor's task counter to reflect the task we're about
            # to execute.
            self._worker.actor_task_counter += 1

            # Execute the assigned method and save a checkpoint if necessary.
            try:
                if is_class_method(method):
                    method_returns = method(*args)
                else:
                    method_returns = method(actor, *args)
            except Exception as e:
                # Save the checkpoint before allowing the method exception
                # to be thrown, but don't save the checkpoint for actor
                # creation task.
                if (isinstance(actor, ray.actor.Checkpointable)
                        and self._worker.actor_task_counter != 1):
                    self._save_and_log_checkpoint(actor)
                raise e
            else:
                # Handle any checkpointing operations before storing the
                # method's return values.
                # NOTE(swang): If method_returns is a pointer to the actor's
                # state and the checkpointing operations can modify the return
                # values if they mutate the actor's state. Is this okay?
                if isinstance(actor, ray.actor.Checkpointable):
                    # If this is the first task to execute on the actor, try to
                    # resume from a checkpoint.
                    if self._worker.actor_task_counter == 1:
                        if actor_imported:
                            self._restore_and_log_checkpoint(actor)
                    else:
                        # Save the checkpoint before returning the method's
                        # return values.
                        self._save_and_log_checkpoint(actor)
                return method_returns

        return actor_method_executor

    def _save_and_log_checkpoint(self, actor):
        """Save an actor checkpoint if necessary and log any errors.

        Args:
            actor: The actor to checkpoint.

        Returns:
            The result of the actor's user-defined `save_checkpoint` method.
        """
        actor_id = self._worker.actor_id
        checkpoint_info = self._worker.actor_checkpoint_info[actor_id]
        checkpoint_info.num_tasks_since_last_checkpoint += 1
        now = int(1000 * time.time())
        checkpoint_context = ray.actor.CheckpointContext(
            actor_id, checkpoint_info.num_tasks_since_last_checkpoint,
            now - checkpoint_info.last_checkpoint_timestamp)
        # If we should take a checkpoint, notify raylet to prepare a checkpoint
        # and then call `save_checkpoint`.
        if actor.should_checkpoint(checkpoint_context):
            try:
                now = int(1000 * time.time())
                checkpoint_id = (self._worker.raylet_client.
                                 prepare_actor_checkpoint(actor_id))
                checkpoint_info.checkpoint_ids.append(checkpoint_id)
                actor.save_checkpoint(actor_id, checkpoint_id)
                if (len(checkpoint_info.checkpoint_ids) >
                        ray._config.num_actor_checkpoints_to_keep()):
                    actor.checkpoint_expired(
                        actor_id,
                        checkpoint_info.checkpoint_ids.pop(0),
                    )
                checkpoint_info.num_tasks_since_last_checkpoint = 0
                checkpoint_info.last_checkpoint_timestamp = now
            except Exception:
                # Checkpoint save or reload failed. Notify the driver.
                traceback_str = ray.utils.format_error_message(
                    traceback.format_exc())
                ray.utils.push_error_to_driver(
                    self._worker,
                    ray_constants.CHECKPOINT_PUSH_ERROR,
                    traceback_str,
                    driver_id=self._worker.task_driver_id)

    def _restore_and_log_checkpoint(self, actor):
        """Restore an actor from a checkpoint if available and log any errors.

        This should only be called on workers that have just executed an actor
        creation task.

        Args:
            actor: The actor to restore from a checkpoint.
        """
        actor_id = self._worker.actor_id
        try:
            checkpoints = ray.actor.get_checkpoints_for_actor(actor_id)
            if len(checkpoints) > 0:
                # If we found previously saved checkpoints for this actor,
                # call the `load_checkpoint` callback.
                checkpoint_id = actor.load_checkpoint(actor_id, checkpoints)
                if checkpoint_id is not None:
                    # Check that the returned checkpoint id is in the
                    # `available_checkpoints` list.
                    msg = (
                        "`load_checkpoint` must return a checkpoint id that " +
                        "exists in the `available_checkpoints` list, or eone.")
                    assert any(checkpoint_id == checkpoint.checkpoint_id
                               for checkpoint in checkpoints), msg
                    # Notify raylet that this actor has been resumed from
                    # a checkpoint.
                    (self._worker.raylet_client.
                     notify_actor_resumed_from_checkpoint(
                         actor_id, checkpoint_id))
        except Exception:
            # Checkpoint save or reload failed. Notify the driver.
            traceback_str = ray.utils.format_error_message(
                traceback.format_exc())
            ray.utils.push_error_to_driver(
                self._worker,
                ray_constants.CHECKPOINT_PUSH_ERROR,
                traceback_str,
                driver_id=self._worker.task_driver_id)
