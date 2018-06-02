from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import atexit
import collections
import colorama
import hashlib
import inspect
import json
import numpy as np
import os
import redis
import signal
import sys
import threading
import time
import traceback

# Ray modules
import pyarrow
import pyarrow.plasma as plasma
import ray.cloudpickle as pickle
import ray.experimental.state as state
import ray.remote_function
import ray.serialization as serialization
import ray.services as services
import ray.signature
import ray.local_scheduler
import ray.plasma
from ray.utils import random_string, binary_to_hex, is_cython

# Import flatbuffer bindings.
from ray.core.generated.ClientTableData import ClientTableData

SCRIPT_MODE = 0
WORKER_MODE = 1
PYTHON_MODE = 2
SILENT_MODE = 3

LOG_POINT = 0
LOG_SPAN_START = 1
LOG_SPAN_END = 2

ERROR_KEY_PREFIX = b"Error:"
DRIVER_ID_LENGTH = 20
ERROR_ID_LENGTH = 20

# This must match the definition of NIL_ACTOR_ID in task.h.
NIL_ID = 20 * b"\xff"
NIL_LOCAL_SCHEDULER_ID = NIL_ID
NIL_FUNCTION_ID = NIL_ID
NIL_ACTOR_ID = NIL_ID
NIL_ACTOR_HANDLE_ID = NIL_ID
NIL_CLIENT_ID = 20 * b"\xff"

# This must be kept in sync with the `error_types` array in
# common/state/error_table.h.
OBJECT_HASH_MISMATCH_ERROR_TYPE = b"object_hash_mismatch"
PUT_RECONSTRUCTION_ERROR_TYPE = b"put_reconstruction"

# This must be kept in sync with the `scheduling_state` enum in common/task.h.
TASK_STATUS_RUNNING = 8

# Default resource requirements for actors when no resource requirements are
# specified.
DEFAULT_ACTOR_METHOD_CPUS_SIMPLE_CASE = 1
DEFAULT_ACTOR_CREATION_CPUS_SIMPLE_CASE = 0
# Default resource requirements for actors when some resource requirements are
# specified.
DEFAULT_ACTOR_METHOD_CPUS_SPECIFIED_CASE = 0
DEFAULT_ACTOR_CREATION_CPUS_SPECIFIED_CASE = 1


class RayTaskError(Exception):
    """An object used internally to represent a task that threw an exception.

    If a task throws an exception during execution, a RayTaskError is stored in
    the object store for each of the task's outputs. When an object is
    retrieved from the object store, the Python method that retrieved it checks
    to see if the object is a RayTaskError and if it is then an exception is
    thrown propagating the error message.

    Currently, we either use the exception attribute or the traceback attribute
    but not both.

    Attributes:
        function_name (str): The name of the function that failed and produced
            the RayTaskError.
        exception (Exception): The exception object thrown by the failed task.
        traceback_str (str): The traceback from the exception.
    """

    def __init__(self, function_name, exception, traceback_str):
        """Initialize a RayTaskError."""
        self.function_name = function_name
        if (isinstance(exception, RayGetError)
                or isinstance(exception, RayGetArgumentError)):
            self.exception = exception
        else:
            self.exception = None
        self.traceback_str = traceback_str

    def __str__(self):
        """Format a RayTaskError as a string."""
        if self.traceback_str is None:
            # This path is taken if getting the task arguments failed.
            return ("Remote function {}{}{} failed with:\n\n{}".format(
                colorama.Fore.RED, self.function_name, colorama.Fore.RESET,
                self.exception))
        else:
            # This path is taken if the task execution failed.
            return ("Remote function {}{}{} failed with:\n\n{}".format(
                colorama.Fore.RED, self.function_name, colorama.Fore.RESET,
                self.traceback_str))


class RayGetError(Exception):
    """An exception used when get is called on an output of a failed task.

    Attributes:
        objectid (lib.ObjectID): The ObjectID that get was called on.
        task_error (RayTaskError): The RayTaskError object created by the
            failed task.
    """

    def __init__(self, objectid, task_error):
        """Initialize a RayGetError object."""
        self.objectid = objectid
        self.task_error = task_error

    def __str__(self):
        """Format a RayGetError as a string."""
        return ("Could not get objectid {}. It was created by remote function "
                "{}{}{} which failed with:\n\n{}".format(
                    self.objectid, colorama.Fore.RED,
                    self.task_error.function_name, colorama.Fore.RESET,
                    self.task_error))


class RayGetArgumentError(Exception):
    """An exception used when a task's argument was produced by a failed task.

    Attributes:
        argument_index (int): The index (zero indexed) of the failed argument
            in present task's remote function call.
        function_name (str): The name of the function for the current task.
        objectid (lib.ObjectID): The ObjectID that was passed in as the
            argument.
        task_error (RayTaskError): The RayTaskError object created by the
            failed task.
    """

    def __init__(self, function_name, argument_index, objectid, task_error):
        """Initialize a RayGetArgumentError object."""
        self.argument_index = argument_index
        self.function_name = function_name
        self.objectid = objectid
        self.task_error = task_error

    def __str__(self):
        """Format a RayGetArgumentError as a string."""
        return ("Failed to get objectid {} as argument {} for remote function "
                "{}{}{}. It was created by remote function {}{}{} which "
                "failed with:\n{}".format(
                    self.objectid, self.argument_index, colorama.Fore.RED,
                    self.function_name, colorama.Fore.RESET, colorama.Fore.RED,
                    self.task_error.function_name, colorama.Fore.RESET,
                    self.task_error))


FunctionExecutionInfo = collections.namedtuple(
    "FunctionExecutionInfo", ["function", "function_name", "max_calls"])
"""FunctionExecutionInfo: A named tuple storing remote function information."""


class Worker(object):
    """A class used to define the control flow of a worker process.

    Note:
        The methods in this class are considered unexposed to the user. The
        functions outside of this class are considered exposed.

    Attributes:
        function_execution_info (Dict[str, FunctionExecutionInfo]): A
            dictionary mapping the name of a remote function to the remote
            function itself. This is the set of remote functions that can be
            executed by this worker.
        connected (bool): True if Ray has been started and False otherwise.
        mode: The mode of the worker. One of SCRIPT_MODE, PYTHON_MODE,
            SILENT_MODE, and WORKER_MODE.
        cached_remote_functions_and_actors: A list of information for exporting
            remote functions and actor classes definitions that were defined
            before the worker called connect. When the worker eventually does
            call connect, if it is a driver, it will export these functions and
            actors. If cached_remote_functions_and_actors is None, that means
            that connect has been called already.
        cached_functions_to_run (List): A list of functions to run on all of
            the workers that should be exported as soon as connect is called.
    """

    def __init__(self):
        """Initialize a Worker object."""
        # This field is a dictionary that maps a driver ID to a dictionary of
        # functions (and information about those functions) that have been
        # registered for that driver (this inner dictionary maps function IDs
        # to a FunctionExecutionInfo object. This should only be used on
        # workers that execute remote functions.
        self.function_execution_info = collections.defaultdict(lambda: {})
        # This is a dictionary mapping driver ID to a dictionary that maps
        # remote function IDs for that driver to a counter of the number of
        # times that remote function has been executed on this worker. The
        # counter is incremented every time the function is executed on this
        # worker. When the counter reaches the maximum number of executions
        # allowed for a particular function, the worker is killed.
        self.num_task_executions = collections.defaultdict(lambda: {})
        self.connected = False
        self.mode = None
        self.cached_remote_functions_and_actors = []
        self.cached_functions_to_run = []
        self.fetch_and_register_actor = None
        self.make_actor = None
        self.actors = {}
        self.actor_task_counter = 0
        # A set of all of the actor class keys that have been imported by the
        # import thread. It is safe to convert this worker into an actor of
        # these types.
        self.imported_actor_classes = set()
        # The number of threads Plasma should use when putting an object in the
        # object store.
        self.memcopy_threads = 12
        # When the worker is constructed. Record the original value of the
        # CUDA_VISIBLE_DEVICES environment variable.
        self.original_gpu_ids = ray.utils.get_cuda_visible_devices()

    def check_connected(self):
        """Check if the worker is connected.

        Raises:
          Exception: An exception is raised if the worker is not connected.
        """
        if not self.connected:
            raise RayConnectionError("Ray has not been started yet. You can "
                                     "start Ray with 'ray.init()'.")

    def set_mode(self, mode):
        """Set the mode of the worker.

        The mode SCRIPT_MODE should be used if this Worker is a driver that is
        being run as a Python script or interactively in a shell. It will print
        information about task failures.

        The mode WORKER_MODE should be used if this Worker is not a driver. It
        will not print information about tasks.

        The mode PYTHON_MODE should be used if this Worker is a driver and if
        you want to run the driver in a manner equivalent to serial Python for
        debugging purposes. It will not send remote function calls to the
        scheduler and will insead execute them in a blocking fashion.

        The mode SILENT_MODE should be used only during testing. It does not
        print any information about errors because some of the tests
        intentionally fail.

        Args:
            mode: One of SCRIPT_MODE, WORKER_MODE, PYTHON_MODE, and
                SILENT_MODE.
        """
        self.mode = mode

    def store_and_register(self, object_id, value, depth=100):
        """Store an object and attempt to register its class if needed.

        Args:
            object_id: The ID of the object to store.
            value: The value to put in the object store.
            depth: The maximum number of classes to recursively register.

        Raises:
            Exception: An exception is raised if the attempt to store the
                object fails. This can happen if there is already an object
                with the same ID in the object store or if the object store is
                full.
        """
        counter = 0
        while True:
            if counter == depth:
                raise Exception("Ray exceeded the maximum number of classes "
                                "that it will recursively serialize when "
                                "attempting to serialize an object of "
                                "type {}.".format(type(value)))
            counter += 1
            try:
                self.plasma_client.put(
                    value,
                    object_id=pyarrow.plasma.ObjectID(object_id.id()),
                    memcopy_threads=self.memcopy_threads,
                    serialization_context=self.serialization_context)
                break
            except pyarrow.SerializationCallbackError as e:
                try:
                    register_custom_serializer(
                        type(e.example_object), use_dict=True)
                    warning_message = ("WARNING: Serializing objects of type "
                                       "{} by expanding them as dictionaries "
                                       "of their fields. This behavior may "
                                       "be incorrect in some cases.".format(
                                           type(e.example_object)))
                    print(warning_message)
                except (serialization.RayNotDictionarySerializable,
                        serialization.CloudPickleError,
                        pickle.pickle.PicklingError, Exception):
                    # We also handle generic exceptions here because
                    # cloudpickle can fail with many different types of errors.
                    try:
                        register_custom_serializer(
                            type(e.example_object), use_pickle=True)
                        warning_message = ("WARNING: Falling back to "
                                           "serializing objects of type {} by "
                                           "using pickle. This may be "
                                           "inefficient.".format(
                                               type(e.example_object)))
                        print(warning_message)
                    except serialization.CloudPickleError:
                        register_custom_serializer(
                            type(e.example_object),
                            use_pickle=True,
                            local=True)
                        warning_message = ("WARNING: Pickling the class {} "
                                           "failed, so we are using pickle "
                                           "and only registering the class "
                                           "locally.".format(
                                               type(e.example_object)))
                        print(warning_message)

    def put_object(self, object_id, value):
        """Put value in the local object store with object id objectid.

        This assumes that the value for objectid has not yet been placed in the
        local object store.

        Args:
            object_id (object_id.ObjectID): The object ID of the value to be
                put.
            value: The value to put in the object store.

        Raises:
            Exception: An exception is raised if the attempt to store the
                object fails. This can happen if there is already an object
                with the same ID in the object store or if the object store is
                full.
        """
        # Make sure that the value is not an object ID.
        if isinstance(value, ray.ObjectID):
            raise Exception("Calling 'put' on an ObjectID is not allowed "
                            "(similarly, returning an ObjectID from a remote "
                            "function is not allowed). If you really want to "
                            "do this, you can wrap the ObjectID in a list and "
                            "call 'put' on it (or return it).")

        # Serialize and put the object in the object store.
        try:
            self.store_and_register(object_id, value)
        except pyarrow.PlasmaObjectExists as e:
            # The object already exists in the object store, so there is no
            # need to add it again. TODO(rkn): We need to compare the hashes
            # and make sure that the objects are in fact the same. We also
            # should return an error code to the caller instead of printing a
            # message.
            print("The object with ID {} already exists in the object store."
                  .format(object_id))

    def retrieve_and_deserialize(self, object_ids, timeout, error_timeout=10):
        start_time = time.time()
        # Only send the warning once.
        warning_sent = False
        while True:
            try:
                # We divide very large get requests into smaller get requests
                # so that a single get request doesn't block the store for a
                # long time, if the store is blocked, it can block the manager
                # as well as a consequence.
                results = []
                for i in range(0, len(object_ids),
                               ray._config.worker_get_request_size()):
                    results += self.plasma_client.get(
                        object_ids[i:(
                            i + ray._config.worker_get_request_size())],
                        timeout, self.serialization_context)
                return results
            except pyarrow.lib.ArrowInvalid as e:
                # TODO(ekl): the local scheduler could include relevant
                # metadata in the task kill case for a better error message
                invalid_error = RayTaskError(
                    "<unknown>", None,
                    "Invalid return value: likely worker died or was killed "
                    "while executing the task.")
                return [invalid_error] * len(object_ids)
            except pyarrow.DeserializationCallbackError as e:
                # Wait a little bit for the import thread to import the class.
                # If we currently have the worker lock, we need to release it
                # so that the import thread can acquire it.
                if self.mode == WORKER_MODE:
                    self.lock.release()
                time.sleep(0.01)
                if self.mode == WORKER_MODE:
                    self.lock.acquire()

                if time.time() - start_time > error_timeout:
                    warning_message = ("This worker or driver is waiting to "
                                       "receive a class definition so that it "
                                       "can deserialize an object from the "
                                       "object store. This may be fine, or it "
                                       "may be a bug.")
                    if not warning_sent:
                        ray.utils.push_error_to_driver(
                            self.redis_client,
                            "wait_for_class",
                            warning_message,
                            driver_id=self.task_driver_id.id())
                    warning_sent = True

    def get_object(self, object_ids):
        """Get the value or values in the object store associated with the IDs.

        Return the values from the local object store for object_ids. This will
        block until all the values for object_ids have been written to the
        local object store.

        Args:
            object_ids (List[object_id.ObjectID]): A list of the object IDs
                whose values should be retrieved.
        """
        # Make sure that the values are object IDs.
        for object_id in object_ids:
            if not isinstance(object_id, ray.ObjectID):
                raise Exception("Attempting to call `get` on the value {}, "
                                "which is not an ObjectID.".format(object_id))
        # Do an initial fetch for remote objects. We divide the fetch into
        # smaller fetches so as to not block the manager for a prolonged period
        # of time in a single call.
        plain_object_ids = [
            plasma.ObjectID(object_id.id()) for object_id in object_ids
        ]
        for i in range(0, len(object_ids),
                       ray._config.worker_fetch_request_size()):
            if not self.use_raylet:
                self.plasma_client.fetch(plain_object_ids[i:(
                    i + ray._config.worker_fetch_request_size())])
            else:
                print("plasma_client.fetch has not been implemented yet")

        # Get the objects. We initially try to get the objects immediately.
        final_results = self.retrieve_and_deserialize(plain_object_ids, 0)
        # Construct a dictionary mapping object IDs that we haven't gotten yet
        # to their original index in the object_ids argument.
        unready_ids = {
            plain_object_ids[i].binary(): i
            for (i, val) in enumerate(final_results)
            if val is plasma.ObjectNotAvailable
        }
        was_blocked = (len(unready_ids) > 0)
        # Try reconstructing any objects we haven't gotten yet. Try to get them
        # until at least get_timeout_milliseconds milliseconds passes, then
        # repeat.
        while len(unready_ids) > 0:
            for unready_id in unready_ids:
                self.local_scheduler_client.reconstruct_object(unready_id)
            # Do another fetch for objects that aren't available locally yet,
            # in case they were evicted since the last fetch. We divide the
            # fetch into smaller fetches so as to not block the manager for a
            # prolonged period of time in a single call.
            object_ids_to_fetch = list(
                map(plasma.ObjectID, unready_ids.keys()))
            for i in range(0, len(object_ids_to_fetch),
                           ray._config.worker_fetch_request_size()):
                if not self.use_raylet:
                    self.plasma_client.fetch(object_ids_to_fetch[i:(
                        i + ray._config.worker_fetch_request_size())])
                else:
                    print("plasma_client.fetch has not been implemented yet")
            results = self.retrieve_and_deserialize(
                object_ids_to_fetch,
                max([
                    ray._config.get_timeout_milliseconds(),
                    int(0.01 * len(unready_ids))
                ]))
            # Remove any entries for objects we received during this iteration
            # so we don't retrieve the same object twice.
            for i, val in enumerate(results):
                if val is not plasma.ObjectNotAvailable:
                    object_id = object_ids_to_fetch[i].binary()
                    index = unready_ids[object_id]
                    final_results[index] = val
                    unready_ids.pop(object_id)

        # If there were objects that we weren't able to get locally, let the
        # local scheduler know that we're now unblocked.
        if was_blocked:
            self.local_scheduler_client.notify_unblocked()

        assert len(final_results) == len(object_ids)
        return final_results

    def submit_task(self,
                    function_id,
                    args,
                    actor_id=None,
                    actor_handle_id=None,
                    actor_counter=0,
                    is_actor_checkpoint_method=False,
                    actor_creation_id=None,
                    actor_creation_dummy_object_id=None,
                    execution_dependencies=None,
                    num_return_vals=None,
                    resources=None,
                    driver_id=None):
        """Submit a remote task to the scheduler.

        Tell the scheduler to schedule the execution of the function with ID
        function_id with arguments args. Retrieve object IDs for the outputs of
        the function from the scheduler and immediately return them.

        Args:
            function_id: The ID of the function to execute.
            args: The arguments to pass into the function. Arguments can be
                object IDs or they can be values. If they are values, they must
                be serializable objecs.
            actor_id: The ID of the actor that this task is for.
            actor_counter: The counter of the actor task.
            is_actor_checkpoint_method: True if this is an actor checkpoint
                task and false otherwise.
            actor_creation_id: The ID of the actor to create, if this is an
                actor creation task.
            actor_creation_dummy_object_id: If this task is an actor method,
                then this argument is the dummy object ID associated with the
                actor creation task for the corresponding actor.
            execution_dependencies: The execution dependencies for this task.
            num_return_vals: The number of return values this function should
                have.
            resources: The resource requirements for this task.
            driver_id: The ID of the relevant driver. This is almost always the
                driver ID of the driver that is currently running. However, in
                the exceptional case that an actor task is being dispatched to
                an actor created by a different driver, this should be the
                driver ID of the driver that created the actor.

        Returns:
            The return object IDs for this task.
        """
        with log_span("ray:submit_task", worker=self):
            check_main_thread()
            if actor_id is None:
                assert actor_handle_id is None
                actor_id = ray.ObjectID(NIL_ACTOR_ID)
                actor_handle_id = ray.ObjectID(NIL_ACTOR_HANDLE_ID)
            else:
                assert actor_handle_id is not None

            if actor_creation_id is None:
                actor_creation_id = ray.ObjectID(NIL_ACTOR_ID)

            if actor_creation_dummy_object_id is None:
                actor_creation_dummy_object_id = (ray.ObjectID(NIL_ID))

            # Put large or complex arguments that are passed by value in the
            # object store first.
            args_for_local_scheduler = []
            for arg in args:
                if isinstance(arg, ray.ObjectID):
                    args_for_local_scheduler.append(arg)
                elif ray.local_scheduler.check_simple_value(arg):
                    args_for_local_scheduler.append(arg)
                else:
                    args_for_local_scheduler.append(put(arg))

            # By default, there are no execution dependencies.
            if execution_dependencies is None:
                execution_dependencies = []

            if driver_id is None:
                driver_id = self.task_driver_id

            if resources is None:
                raise ValueError("The resources dictionary is required.")

            # Submit the task to local scheduler.
            task = ray.local_scheduler.Task(
                driver_id, ray.ObjectID(
                    function_id.id()), args_for_local_scheduler,
                num_return_vals, self.current_task_id, self.task_index,
                actor_creation_id, actor_creation_dummy_object_id, actor_id,
                actor_handle_id, actor_counter, is_actor_checkpoint_method,
                execution_dependencies, resources, self.use_raylet)
            # Increment the worker's task index to track how many tasks have
            # been submitted by the current task so far.
            self.task_index += 1
            self.local_scheduler_client.submit(task)

            return task.returns()

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
        check_main_thread()
        if self.mode not in [SCRIPT_MODE, SILENT_MODE]:
            raise Exception("export_remote_function can only be called on a "
                            "driver.")

        key = (b"RemoteFunction:" + self.task_driver_id.id() + b":" +
               function_id.id())

        # Work around limitations of Python pickling.
        function_name_global_valid = function.__name__ in function.__globals__
        function_name_global_value = function.__globals__.get(
            function.__name__)
        # Allow the function to reference itself as a global variable
        if not is_cython(function):
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

        self.redis_client.hmset(
            key, {
                "driver_id": self.task_driver_id.id(),
                "function_id": function_id.id(),
                "name": function_name,
                "module": function.__module__,
                "function": pickled_function,
                "max_calls": max_calls
            })
        self.redis_client.rpush("Exports", key)

    def run_function_on_all_workers(self, function):
        """Run arbitrary code on all of the workers.

        This function will first be run on the driver, and then it will be
        exported to all of the workers to be run. It will also be run on any
        new workers that register later. If ray.init has not been called yet,
        then cache the function and export it later.

        Args:
            function (Callable): The function to run on all of the workers. It
                should not take any arguments. If it returns anything, its
                return values will not be used.
        """
        check_main_thread()
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
            key = b"FunctionsToRun:" + function_to_run_id
            # First run the function on the driver.
            # We always run the task locally.
            function({"worker": self})
            # Check if the function has already been put into redis.
            function_exported = self.redis_client.setnx(b"Lock:" + key, 1)
            if not function_exported:
                # In this case, the function has already been exported, so
                # we don't need to export it again.
                return
            # Run the function on all workers.
            self.redis_client.hmset(
                key, {
                    "driver_id": self.task_driver_id.id(),
                    "function_id": function_to_run_id,
                    "function": pickled_function
                })
            self.redis_client.rpush("Exports", key)
            # TODO(rkn): If the worker fails after it calls setnx and before it
            # successfully completes the hmset and rpush, then the program will
            # most likely hang. This could be fixed by making these three
            # operations into a transaction (or by implementing a custom
            # command that does all three things).

    def _wait_for_function(self, function_id, driver_id, timeout=10):
        """Wait until the function to be executed is present on this worker.

        This method will simply loop until the import thread has imported the
        relevant function. If we spend too long in this loop, that may indicate
        a problem somewhere and we will push an error message to the user.

        If this worker is an actor, then this will wait until the actor has
        been defined.

        Args:
            is_actor (bool): True if this worker is an actor, and false
                otherwise.
            function_id (str): The ID of the function that we want to execute.
            driver_id (str): The ID of the driver to push the error message to
                if this times out.
        """
        start_time = time.time()
        # Only send the warning once.
        warning_sent = False
        while True:
            with self.lock:
                if (self.actor_id == NIL_ACTOR_ID
                        and (function_id.id() in
                             self.function_execution_info[driver_id])):
                    break
                elif self.actor_id != NIL_ACTOR_ID and (
                        self.actor_id in self.actors):
                    break
                if time.time() - start_time > timeout:
                    warning_message = ("This worker was asked to execute a "
                                       "function that it does not have "
                                       "registered. You may have to restart "
                                       "Ray.")
                    if not warning_sent:
                        ray.utils.push_error_to_driver(
                            self.redis_client,
                            "wait_for_function",
                            warning_message,
                            driver_id=driver_id)
                    warning_sent = True
            time.sleep(0.001)

    def _get_arguments_for_execution(self, function_name, serialized_args):
        """Retrieve the arguments for the remote function.

        This retrieves the values for the arguments to the remote function that
        were passed in as object IDs. Argumens that were passed by value are
        not changed. This is called by the worker that is executing the remote
        function.

        Args:
            function_name (str): The name of the remote function whose
                arguments are being retrieved.
            serialized_args (List): The arguments to the function. These are
                either strings representing serialized objects passed by value
                or they are ObjectIDs.

        Returns:
            The retrieved arguments in addition to the arguments that were
                passed by value.

        Raises:
            RayGetArgumentError: This exception is raised if a task that
                created one of the arguments failed.
        """
        arguments = []
        for (i, arg) in enumerate(serialized_args):
            if isinstance(arg, ray.ObjectID):
                # get the object from the local object store
                argument = self.get_object([arg])[0]
                if isinstance(argument, RayTaskError):
                    # If the result is a RayTaskError, then the task that
                    # created this object failed, and we should propagate the
                    # error message here.
                    raise RayGetArgumentError(function_name, i, arg, argument)
            else:
                # pass the argument by value
                argument = arg

            arguments.append(argument)
        return arguments

    def _store_outputs_in_objstore(self, object_ids, outputs):
        """Store the outputs of a remote function in the local object store.

        This stores the values that were returned by a remote function in the
        local object store. If any of the return values are object IDs, then
        these object IDs are aliased with the object IDs that the scheduler
        assigned for the return values. This is called by the worker that
        executes the remote function.

        Note:
            The arguments object_ids and outputs should have the same length.

        Args:
            object_ids (List[ObjectID]): The object IDs that were assigned to
                the outputs of the remote function call.
            outputs (Tuple): The value returned by the remote function. If the
                remote function was supposed to only return one value, then its
                output was wrapped in a tuple with one element prior to being
                passed into this function.
        """
        for i in range(len(object_ids)):
            if isinstance(outputs[i], ray.actor.ActorHandle):
                raise Exception("Returning an actor handle from a remote "
                                "function is not allowed).")

            self.put_object(object_ids[i], outputs[i])

    def _process_task(self, task):
        """Execute a task assigned to this worker.

        This method deserializes a task from the scheduler, and attempts to
        execute the task. If the task succeeds, the outputs are stored in the
        local object store. If the task throws an exception, RayTaskError
        objects are stored in the object store to represent the failed task
        (these will be retrieved by calls to get or by subsequent tasks that
        use the outputs of this task).
        """
        # The ID of the driver that this task belongs to. This is needed so
        # that if the task throws an exception, we propagate the error
        # message to the correct driver.
        self.task_driver_id = task.driver_id()
        self.current_task_id = task.task_id()
        self.task_index = 0
        self.put_index = 1
        function_id = task.function_id()
        args = task.arguments()
        return_object_ids = task.returns()
        if task.actor_id().id() != NIL_ACTOR_ID:
            dummy_return_id = return_object_ids.pop()
        function_executor = self.function_execution_info[
            self.task_driver_id.id()][function_id.id()].function
        function_name = self.function_execution_info[self.task_driver_id.id()][
            function_id.id()].function_name

        # Get task arguments from the object store.
        try:
            with log_span("ray:task:get_arguments", worker=self):
                arguments = self._get_arguments_for_execution(
                    function_name, args)
        except (RayGetError, RayGetArgumentError) as e:
            self._handle_process_task_failure(function_id, return_object_ids,
                                              e, None)
            return
        except Exception as e:
            self._handle_process_task_failure(
                function_id, return_object_ids, e,
                ray.utils.format_error_message(traceback.format_exc()))
            return

        # Execute the task.
        try:
            with log_span("ray:task:execute", worker=self):
                if task.actor_id().id() == NIL_ACTOR_ID:
                    outputs = function_executor(*arguments)
                else:
                    outputs = function_executor(
                        dummy_return_id, self.actors[task.actor_id().id()],
                        *arguments)
        except Exception as e:
            # Determine whether the exception occured during a task, not an
            # actor method.
            task_exception = task.actor_id().id() == NIL_ACTOR_ID
            traceback_str = ray.utils.format_error_message(
                traceback.format_exc(), task_exception=task_exception)
            self._handle_process_task_failure(function_id, return_object_ids,
                                              e, traceback_str)
            return

        # Store the outputs in the local object store.
        try:
            with log_span("ray:task:store_outputs", worker=self):
                # If this is an actor task, then the last object ID returned by
                # the task is a dummy output, not returned by the function
                # itself. Decrement to get the correct number of return values.
                num_returns = len(return_object_ids)
                if num_returns == 1:
                    outputs = (outputs, )
                self._store_outputs_in_objstore(return_object_ids, outputs)
        except Exception as e:
            self._handle_process_task_failure(
                function_id, return_object_ids, e,
                ray.utils.format_error_message(traceback.format_exc()))

    def _handle_process_task_failure(self, function_id, return_object_ids,
                                     error, backtrace):
        function_name = self.function_execution_info[self.task_driver_id.id()][
            function_id.id()].function_name
        failure_object = RayTaskError(function_name, error, backtrace)
        failure_objects = [
            failure_object for _ in range(len(return_object_ids))
        ]
        self._store_outputs_in_objstore(return_object_ids, failure_objects)
        # Log the error message.
        ray.utils.push_error_to_driver(
            self.redis_client,
            "task",
            str(failure_object),
            driver_id=self.task_driver_id.id(),
            data={
                "function_id": function_id.id(),
                "function_name": function_name
            })

    def _become_actor(self, task):
        """Turn this worker into an actor.

        Args:
            task: The actor creation task.
        """
        assert self.actor_id == NIL_ACTOR_ID
        arguments = task.arguments()
        assert len(arguments) == 1
        self.actor_id = task.actor_creation_id().id()
        class_id = arguments[0]

        key = b"ActorClass:" + class_id

        # Wait for the actor class key to have been imported by the import
        # thread. TODO(rkn): It shouldn't be possible to end up in an infinite
        # loop here, but we should push an error to the driver if too much time
        # is spent here.
        while key not in self.imported_actor_classes:
            time.sleep(0.001)

        with self.lock:
            self.fetch_and_register_actor(key, self)

    def _wait_for_and_process_task(self, task):
        """Wait for a task to be ready and process the task.

        Args:
            task: The task to execute.
        """
        function_id = task.function_id()
        driver_id = task.driver_id().id()

        # TODO(rkn): It would be preferable for actor creation tasks to share
        # more of the code path with regular task execution.
        if (task.actor_creation_id() != ray.ObjectID(NIL_ACTOR_ID)):
            self._become_actor(task)
            return

        # Wait until the function to be executed has actually been registered
        # on this worker. We will push warnings to the user if we spend too
        # long in this loop.
        with log_span("ray:wait_for_function", worker=self):
            self._wait_for_function(function_id, driver_id)

        # Execute the task.
        # TODO(rkn): Consider acquiring this lock with a timeout and pushing a
        # warning to the user if we are waiting too long to acquire the lock
        # because that may indicate that the system is hanging, and it'd be
        # good to know where the system is hanging.
        log(event_type="ray:acquire_lock", kind=LOG_SPAN_START, worker=self)
        with self.lock:
            log(event_type="ray:acquire_lock", kind=LOG_SPAN_END, worker=self)

            function_name = (self.function_execution_info[driver_id][
                function_id.id()]).function_name
            contents = {
                "function_name": function_name,
                "task_id": task.task_id().hex(),
                "worker_id": binary_to_hex(self.worker_id)
            }
            with log_span("ray:task", contents=contents, worker=self):
                self._process_task(task)

        # Push all of the log events to the global state store.
        flush_log()

        # Increase the task execution counter.
        self.num_task_executions[driver_id][function_id.id()] += 1

        reached_max_executions = (
            self.num_task_executions[driver_id][function_id.id()] == self.
            function_execution_info[driver_id][function_id.id()].max_calls)
        if reached_max_executions:
            self.local_scheduler_client.disconnect()
            os._exit(0)

    def _get_next_task_from_local_scheduler(self):
        """Get the next task from the local scheduler.

        Returns:
            A task from the local scheduler.
        """
        with log_span("ray:get_task", worker=self):
            task = self.local_scheduler_client.get_task()

        # Automatically restrict the GPUs available to this task.
        ray.utils.set_cuda_visible_devices(ray.get_gpu_ids())

        return task

    def main_loop(self):
        """The main loop a worker runs to receive and execute tasks."""

        def exit(signum, frame):
            cleanup(worker=self)
            sys.exit(0)

        signal.signal(signal.SIGTERM, exit)

        check_main_thread()
        while True:
            task = self._get_next_task_from_local_scheduler()
            self._wait_for_and_process_task(task)


def get_gpu_ids():
    """Get the IDs of the GPUs that are available to the worker.

    If the CUDA_VISIBLE_DEVICES environment variable was set when the worker
    started up, then the IDs returned by this method will be a subset of the
    IDs in CUDA_VISIBLE_DEVICES. If not, the IDs will fall in the range
    [0, NUM_GPUS - 1], where NUM_GPUS is the number of GPUs that the node has.

    Returns:
        A list of GPU IDs.
    """
    if _mode() == PYTHON_MODE:
        raise Exception("ray.get_gpu_ids() currently does not work in PYTHON "
                        "MODE.")

    assigned_ids = global_worker.local_scheduler_client.gpu_ids()
    # If the user had already set CUDA_VISIBLE_DEVICES, then respect that (in
    # the sense that only GPU IDs that appear in CUDA_VISIBLE_DEVICES should be
    # returned).
    if global_worker.original_gpu_ids is not None:
        assigned_ids = [
            global_worker.original_gpu_ids[gpu_id] for gpu_id in assigned_ids
        ]

    return assigned_ids


def _webui_url_helper(client):
    """Parsing for getting the url of the web UI.

    Args:
        client: A redis client to use to query the primary Redis shard.

    Returns:
        The URL of the web UI as a string.
    """
    result = client.hmget("webui", "url")[0]
    return result.decode("ascii") if result is not None else result


def get_webui_url():
    """Get the URL to access the web UI.

    Note that the URL does not specify which node the web UI is on.

    Returns:
        The URL of the web UI as a string.
    """
    if _mode() == PYTHON_MODE:
        raise Exception("ray.get_webui_url() currently does not work in "
                        "PYTHON MODE.")
    return _webui_url_helper(global_worker.redis_client)


global_worker = Worker()
"""Worker: The global Worker object for this worker process.

We use a global Worker object to ensure that there is a single worker object
per worker process.
"""

global_state = state.GlobalState()


class RayConnectionError(Exception):
    pass


def check_main_thread():
    """Check that we are currently on the main thread.

    Raises:
        Exception: An exception is raised if this is called on a thread other
            than the main thread.
    """
    if threading.current_thread().getName() != "MainThread":
        raise Exception("The Ray methods are not thread safe and must be "
                        "called from the main thread. This method was called "
                        "from thread {}."
                        .format(threading.current_thread().getName()))


def print_failed_task(task_status):
    """Print information about failed tasks.

    Args:
        task_status (Dict): A dictionary containing the name, operationid, and
            error message for a failed task.
    """
    print("""
      Error: Task failed
        Function Name: {}
        Task ID: {}
        Error Message: \n{}
    """.format(task_status["function_name"], task_status["operationid"],
               task_status["error_message"]))


def error_applies_to_driver(error_key, worker=global_worker):
    """Return True if the error is for this driver and false otherwise."""
    # TODO(rkn): Should probably check that this is only called on a driver.
    # Check that the error key is formatted as in push_error_to_driver.
    assert len(error_key) == (len(ERROR_KEY_PREFIX) + DRIVER_ID_LENGTH + 1 +
                              ERROR_ID_LENGTH), error_key
    # If the driver ID in the error message is a sequence of all zeros, then
    # the message is intended for all drivers.
    generic_driver_id = DRIVER_ID_LENGTH * b"\x00"
    driver_id = error_key[len(ERROR_KEY_PREFIX):(
        len(ERROR_KEY_PREFIX) + DRIVER_ID_LENGTH)]
    return (driver_id == worker.task_driver_id.id()
            or driver_id == generic_driver_id)


def error_info(worker=global_worker):
    """Return information about failed tasks."""
    worker.check_connected()
    check_main_thread()
    error_keys = worker.redis_client.lrange("ErrorKeys", 0, -1)
    errors = []
    for error_key in error_keys:
        if error_applies_to_driver(error_key, worker=worker):
            error_contents = worker.redis_client.hgetall(error_key)
            errors.append(error_contents)

    return errors


def _initialize_serialization(worker=global_worker):
    """Initialize the serialization library.

    This defines a custom serializer for object IDs and also tells ray to
    serialize several exception classes that we define for error handling.
    """
    worker.serialization_context = pyarrow.default_serialization_context()
    # Tell the serialization context to use the cloudpickle version that we
    # ship with Ray.
    worker.serialization_context.set_pickle(pickle.dumps, pickle.loads)
    pyarrow.register_torch_serialization_handlers(worker.serialization_context)

    # Define a custom serializer and deserializer for handling Object IDs.
    def object_id_custom_serializer(obj):
        return obj.id()

    def object_id_custom_deserializer(serialized_obj):
        return ray.ObjectID(serialized_obj)

    # We register this serializer on each worker instead of calling
    # register_custom_serializer from the driver so that isinstance still
    # works.
    worker.serialization_context.register_type(
        ray.ObjectID,
        "ray.ObjectID",
        pickle=False,
        custom_serializer=object_id_custom_serializer,
        custom_deserializer=object_id_custom_deserializer)

    def actor_handle_serializer(obj):
        return obj._serialization_helper(True)

    def actor_handle_deserializer(serialized_obj):
        new_handle = ray.actor.ActorHandle.__new__(ray.actor.ActorHandle)
        new_handle._deserialization_helper(serialized_obj, True)
        return new_handle

    # We register this serializer on each worker instead of calling
    # register_custom_serializer from the driver so that isinstance still
    # works.
    worker.serialization_context.register_type(
        ray.actor.ActorHandle,
        "ray.ActorHandle",
        pickle=False,
        custom_serializer=actor_handle_serializer,
        custom_deserializer=actor_handle_deserializer)

    if worker.mode in [SCRIPT_MODE, SILENT_MODE]:
        # These should only be called on the driver because
        # register_custom_serializer will export the class to all of the
        # workers.
        register_custom_serializer(RayTaskError, use_dict=True)
        register_custom_serializer(RayGetError, use_dict=True)
        register_custom_serializer(RayGetArgumentError, use_dict=True)
        # Tell Ray to serialize lambdas with pickle.
        register_custom_serializer(type(lambda: 0), use_pickle=True)
        # Tell Ray to serialize types with pickle.
        register_custom_serializer(type(int), use_pickle=True)
        # Tell Ray to serialize FunctionSignatures as dictionaries. This is
        # used when passing around actor handles.
        register_custom_serializer(
            ray.signature.FunctionSignature, use_dict=True)


def get_address_info_from_redis_helper(redis_address,
                                       node_ip_address,
                                       use_raylet=False):
    redis_ip_address, redis_port = redis_address.split(":")
    # For this command to work, some other client (on the same machine as
    # Redis) must have run "CONFIG SET protected-mode no".
    redis_client = redis.StrictRedis(
        host=redis_ip_address, port=int(redis_port))

    if not use_raylet:
        # The client table prefix must be kept in sync with the file
        # "src/common/redis_module/ray_redis_module.cc" where it is defined.
        REDIS_CLIENT_TABLE_PREFIX = "CL:"
        client_keys = redis_client.keys(
            "{}*".format(REDIS_CLIENT_TABLE_PREFIX))
        # Filter to live clients on the same node and do some basic checking.
        plasma_managers = []
        local_schedulers = []
        for key in client_keys:
            info = redis_client.hgetall(key)

            # Ignore clients that were deleted.
            deleted = info[b"deleted"]
            deleted = bool(int(deleted))
            if deleted:
                continue

            assert b"ray_client_id" in info
            assert b"node_ip_address" in info
            assert b"client_type" in info
            client_node_ip_address = info[b"node_ip_address"].decode("ascii")
            if (client_node_ip_address == node_ip_address or
                (client_node_ip_address == "127.0.0.1"
                 and redis_ip_address == ray.services.get_node_ip_address())):
                if info[b"client_type"].decode("ascii") == "plasma_manager":
                    plasma_managers.append(info)
                elif info[b"client_type"].decode("ascii") == "local_scheduler":
                    local_schedulers.append(info)
        # Make sure that we got at least one plasma manager and local
        # scheduler.
        assert len(plasma_managers) >= 1
        assert len(local_schedulers) >= 1
        # Build the address information.
        object_store_addresses = []
        for manager in plasma_managers:
            address = manager[b"manager_address"].decode("ascii")
            port = services.get_port(address)
            object_store_addresses.append(
                services.ObjectStoreAddress(
                    name=manager[b"store_socket_name"].decode("ascii"),
                    manager_name=manager[b"manager_socket_name"].decode(
                        "ascii"),
                    manager_port=port))
        scheduler_names = [
            scheduler[b"local_scheduler_socket_name"].decode("ascii")
            for scheduler in local_schedulers
        ]
        client_info = {
            "node_ip_address": node_ip_address,
            "redis_address": redis_address,
            "object_store_addresses": object_store_addresses,
            "local_scheduler_socket_names": scheduler_names,
            # Web UI should be running.
            "webui_url": _webui_url_helper(redis_client)
        }
        return client_info

    # Handle the raylet case.
    else:
        # In the raylet code path, all client data is stored in a zset at the
        # key for the nil client.
        client_key = b"CLIENT:" + NIL_CLIENT_ID
        clients = redis_client.zrange(client_key, 0, -1)
        raylets = []
        for client_message in clients:
            client = ClientTableData.GetRootAsClientTableData(
                client_message, 0)
            client_node_ip_address = client.NodeManagerAddress().decode(
                "ascii")
            if (client_node_ip_address == node_ip_address or
                (client_node_ip_address == "127.0.0.1"
                 and redis_ip_address == ray.services.get_node_ip_address())):
                raylets.append(client)

        object_store_addresses = [
            services.ObjectStoreAddress(
                name=raylet.ObjectStoreSocketName().decode("ascii"),
                manager_name=None,
                manager_port=None) for raylet in raylets
        ]
        raylet_socket_names = [
            raylet.RayletSocketName().decode("ascii") for raylet in raylets
        ]
        return {
            "node_ip_address": node_ip_address,
            "redis_address": redis_address,
            "object_store_addresses": object_store_addresses,
            "raylet_socket_names": raylet_socket_names,
            # Web UI should be running.
            "webui_url": _webui_url_helper(redis_client)
        }


def get_address_info_from_redis(redis_address,
                                node_ip_address,
                                num_retries=5,
                                use_raylet=False):
    counter = 0
    while True:
        try:
            return get_address_info_from_redis_helper(
                redis_address, node_ip_address, use_raylet=use_raylet)
        except Exception as e:
            if counter == num_retries:
                raise
            # Some of the information may not be in Redis yet, so wait a little
            # bit.
            print("Some processes that the driver needs to connect to have "
                  "not registered with Redis, so retrying. Have you run "
                  "'ray start' on this node?")
            time.sleep(1)
        counter += 1


def _normalize_resource_arguments(num_cpus, num_gpus, resources,
                                  num_local_schedulers):
    """Stick the CPU and GPU arguments into the resources dictionary.

    This also checks that the arguments are well-formed.

    Args:
        num_cpus: Either a number of CPUs or a list of numbers of CPUs.
        num_gpus: Either a number of CPUs or a list of numbers of CPUs.
        resources: Either a dictionary of resource mappings or a list of
            dictionaries of resource mappings.
        num_local_schedulers: The number of local schedulers.

    Returns:
        A list of dictionaries of resources of length num_local_schedulers.
    """
    if resources is None:
        resources = {}
    if not isinstance(num_cpus, list):
        num_cpus = num_local_schedulers * [num_cpus]
    if not isinstance(num_gpus, list):
        num_gpus = num_local_schedulers * [num_gpus]
    if not isinstance(resources, list):
        resources = num_local_schedulers * [resources]

    new_resources = [r.copy() for r in resources]

    for i in range(num_local_schedulers):
        assert "CPU" not in new_resources[i], "Use the 'num_cpus' argument."
        assert "GPU" not in new_resources[i], "Use the 'num_gpus' argument."
        if num_cpus[i] is not None:
            new_resources[i]["CPU"] = num_cpus[i]
        if num_gpus[i] is not None:
            new_resources[i]["GPU"] = num_gpus[i]

    return new_resources


def _init(address_info=None,
          start_ray_local=False,
          object_id_seed=None,
          num_workers=None,
          num_local_schedulers=None,
          object_store_memory=None,
          driver_mode=SCRIPT_MODE,
          redirect_worker_output=False,
          redirect_output=True,
          start_workers_from_local_scheduler=True,
          num_cpus=None,
          num_gpus=None,
          resources=None,
          num_redis_shards=None,
          redis_max_clients=None,
          plasma_directory=None,
          huge_pages=False,
          include_webui=True,
          use_raylet=False):
    """Helper method to connect to an existing Ray cluster or start a new one.

    This method handles two cases. Either a Ray cluster already exists and we
    just attach this driver to it, or we start all of the processes associated
    with a Ray cluster and attach to the newly started cluster.

    Args:
        address_info (dict): A dictionary with address information for
            processes in a partially-started Ray cluster. If
            start_ray_local=True, any processes not in this dictionary will be
            started. If provided, an updated address_info dictionary will be
            returned to include processes that are newly started.
        start_ray_local (bool): If True then this will start any processes not
            already in address_info, including Redis, a global scheduler, local
            scheduler(s), object store(s), and worker(s). It will also kill
            these processes when Python exits. If False, this will attach to an
            existing Ray cluster.
        object_id_seed (int): Used to seed the deterministic generation of
            object IDs. The same value can be used across multiple runs of the
            same job in order to generate the object IDs in a consistent
            manner. However, the same ID should not be used for different jobs.
        num_workers (int): The number of workers to start. This is only
            provided if start_ray_local is True.
        num_local_schedulers (int): The number of local schedulers to start.
            This is only provided if start_ray_local is True.
        object_store_memory: The amount of memory (in bytes) to start the
            object store with.
        driver_mode (bool): The mode in which to start the driver. This should
            be one of ray.SCRIPT_MODE, ray.PYTHON_MODE, and ray.SILENT_MODE.
        redirect_worker_output: True if the stdout and stderr of worker
            processes should be redirected to files.
        redirect_output (bool): True if stdout and stderr for non-worker
            processes should be redirected to files and false otherwise.
        start_workers_from_local_scheduler (bool): If this flag is True, then
            start the initial workers from the local scheduler. Else, start
            them from Python. The latter case is for debugging purposes only.
        num_cpus (int): Number of cpus the user wishes all local schedulers to
            be configured with.
        num_gpus (int): Number of gpus the user wishes all local schedulers to
            be configured with. If unspecified, Ray will attempt to autodetect
            the number of GPUs available on the node (note that autodetection
            currently only works for Nvidia GPUs).
        resources: A dictionary mapping resource names to the quantity of that
            resource available.
        num_redis_shards: The number of Redis shards to start in addition to
            the primary Redis shard.
        redis_max_clients: If provided, attempt to configure Redis with this
            maxclients number.
        plasma_directory: A directory where the Plasma memory mapped files will
            be created.
        huge_pages: Boolean flag indicating whether to start the Object
            Store with hugetlbfs support. Requires plasma_directory.
        include_webui: Boolean flag indicating whether to start the web
            UI, which is a Jupyter notebook.
        use_raylet: True if the new raylet code path should be used. This is
            not supported yet.

    Returns:
        Address information about the started processes.

    Raises:
        Exception: An exception is raised if an inappropriate combination of
            arguments is passed in.
    """
    check_main_thread()
    if driver_mode not in [SCRIPT_MODE, PYTHON_MODE, SILENT_MODE]:
        raise Exception("Driver_mode must be in [ray.SCRIPT_MODE, "
                        "ray.PYTHON_MODE, ray.SILENT_MODE].")

    if use_raylet is None and os.environ.get("RAY_USE_XRAY") == "1":
        # This environment variable is used in our testing setup.
        print("Detected environment variable 'RAY_USE_XRAY'.")
        use_raylet = True

    # Get addresses of existing services.
    if address_info is None:
        address_info = {}
    else:
        assert isinstance(address_info, dict)
    node_ip_address = address_info.get("node_ip_address")
    redis_address = address_info.get("redis_address")

    # Start any services that do not yet exist.
    if driver_mode == PYTHON_MODE:
        # If starting Ray in PYTHON_MODE, don't start any other processes.
        pass
    elif start_ray_local:
        # In this case, we launch a scheduler, a new object store, and some
        # workers, and we connect to them. We do not launch any processes that
        # are already registered in address_info.
        if node_ip_address is None:
            node_ip_address = ray.services.get_node_ip_address()
        # Use 1 local scheduler if num_local_schedulers is not provided. If
        # existing local schedulers are provided, use that count as
        # num_local_schedulers.
        local_schedulers = address_info.get("local_scheduler_socket_names", [])
        if num_local_schedulers is None:
            if len(local_schedulers) > 0:
                num_local_schedulers = len(local_schedulers)
            else:
                num_local_schedulers = 1
        # Use 1 additional redis shard if num_redis_shards is not provided.
        num_redis_shards = 1 if num_redis_shards is None else num_redis_shards

        # Stick the CPU and GPU resources into the resource dictionary.
        resources = _normalize_resource_arguments(
            num_cpus, num_gpus, resources, num_local_schedulers)

        # Start the scheduler, object store, and some workers. These will be
        # killed by the call to cleanup(), which happens when the Python script
        # exits.
        address_info = services.start_ray_head(
            address_info=address_info,
            node_ip_address=node_ip_address,
            num_workers=num_workers,
            num_local_schedulers=num_local_schedulers,
            object_store_memory=object_store_memory,
            redirect_worker_output=redirect_worker_output,
            redirect_output=redirect_output,
            start_workers_from_local_scheduler=(
                start_workers_from_local_scheduler),
            resources=resources,
            num_redis_shards=num_redis_shards,
            redis_max_clients=redis_max_clients,
            plasma_directory=plasma_directory,
            huge_pages=huge_pages,
            include_webui=include_webui,
            use_raylet=use_raylet)
    else:
        if redis_address is None:
            raise Exception("When connecting to an existing cluster, "
                            "redis_address must be provided.")
        if num_workers is not None:
            raise Exception("When connecting to an existing cluster, "
                            "num_workers must not be provided.")
        if num_local_schedulers is not None:
            raise Exception("When connecting to an existing cluster, "
                            "num_local_schedulers must not be provided.")
        if num_cpus is not None or num_gpus is not None:
            raise Exception("When connecting to an existing cluster, num_cpus "
                            "and num_gpus must not be provided.")
        if resources is not None:
            raise Exception("When connecting to an existing cluster, "
                            "resources must not be provided.")
        if num_redis_shards is not None:
            raise Exception("When connecting to an existing cluster, "
                            "num_redis_shards must not be provided.")
        if redis_max_clients is not None:
            raise Exception("When connecting to an existing cluster, "
                            "redis_max_clients must not be provided.")
        if object_store_memory is not None:
            raise Exception("When connecting to an existing cluster, "
                            "object_store_memory must not be provided.")
        if plasma_directory is not None:
            raise Exception("When connecting to an existing cluster, "
                            "plasma_directory must not be provided.")
        if huge_pages:
            raise Exception("When connecting to an existing cluster, "
                            "huge_pages must not be provided.")
        # Get the node IP address if one is not provided.
        if node_ip_address is None:
            node_ip_address = services.get_node_ip_address(redis_address)
        # Get the address info of the processes to connect to from Redis.
        address_info = get_address_info_from_redis(
            redis_address, node_ip_address, use_raylet=use_raylet)

    # Connect this driver to Redis, the object store, and the local scheduler.
    # Choose the first object store and local scheduler if there are multiple.
    # The corresponding call to disconnect will happen in the call to cleanup()
    # when the Python script exits.
    if driver_mode == PYTHON_MODE:
        driver_address_info = {}
    else:
        driver_address_info = {
            "node_ip_address": node_ip_address,
            "redis_address": address_info["redis_address"],
            "store_socket_name": (
                address_info["object_store_addresses"][0].name),
            "webui_url": address_info["webui_url"]
        }
        if not use_raylet:
            driver_address_info["manager_socket_name"] = (
                address_info["object_store_addresses"][0].manager_name)
            driver_address_info["local_scheduler_socket_name"] = (
                address_info["local_scheduler_socket_names"][0])
        else:
            driver_address_info["raylet_socket_name"] = (
                address_info["raylet_socket_names"][0])
    connect(
        driver_address_info,
        object_id_seed=object_id_seed,
        mode=driver_mode,
        worker=global_worker,
        use_raylet=use_raylet)
    return address_info


def init(redis_address=None,
         node_ip_address=None,
         object_id_seed=None,
         num_workers=None,
         driver_mode=SCRIPT_MODE,
         redirect_worker_output=False,
         redirect_output=True,
         num_cpus=None,
         num_gpus=None,
         resources=None,
         num_custom_resource=None,
         num_redis_shards=None,
         redis_max_clients=None,
         plasma_directory=None,
         huge_pages=False,
         include_webui=True,
         object_store_memory=None,
         use_raylet=None):
    """Connect to an existing Ray cluster or start one and connect to it.

    This method handles two cases. Either a Ray cluster already exists and we
    just attach this driver to it, or we start all of the processes associated
    with a Ray cluster and attach to the newly started cluster.

    Args:
        node_ip_address (str): The IP address of the node that we are on.
        redis_address (str): The address of the Redis server to connect to. If
            this address is not provided, then this command will start Redis, a
            global scheduler, a local scheduler, a plasma store, a plasma
            manager, and some workers. It will also kill these processes when
            Python exits.
        object_id_seed (int): Used to seed the deterministic generation of
            object IDs. The same value can be used across multiple runs of the
            same job in order to generate the object IDs in a consistent
            manner. However, the same ID should not be used for different jobs.
        num_workers (int): The number of workers to start. This is only
            provided if redis_address is not provided.
        driver_mode (bool): The mode in which to start the driver. This should
            be one of ray.SCRIPT_MODE, ray.PYTHON_MODE, and ray.SILENT_MODE.
        redirect_worker_output: True if the stdout and stderr of worker
            processes should be redirected to files.
        redirect_output (bool): True if stdout and stderr for non-worker
            processes should be redirected to files and false otherwise.
        num_cpus (int): Number of cpus the user wishes all local schedulers to
            be configured with.
        num_gpus (int): Number of gpus the user wishes all local schedulers to
            be configured with.
        resources: A dictionary mapping the name of a resource to the quantity
            of that resource available.
        num_redis_shards: The number of Redis shards to start in addition to
            the primary Redis shard.
        redis_max_clients: If provided, attempt to configure Redis with this
            maxclients number.
        plasma_directory: A directory where the Plasma memory mapped files will
            be created.
        huge_pages: Boolean flag indicating whether to start the Object
            Store with hugetlbfs support. Requires plasma_directory.
        include_webui: Boolean flag indicating whether to start the web
            UI, which is a Jupyter notebook.
        object_store_memory: The amount of memory (in bytes) to start the
            object store with.
        use_raylet: True if the new raylet code path should be used. This is
            not supported yet.


    Returns:
        Address information about the started processes.

    Raises:
        Exception: An exception is raised if an inappropriate combination of
            arguments is passed in.
    """
    if use_raylet is None and os.environ.get("RAY_USE_XRAY") == "1":
        # This environment variable is used in our testing setup.
        print("Detected environment variable 'RAY_USE_XRAY'.")
        use_raylet = True

    # Convert hostnames to numerical IP address.
    if node_ip_address is not None:
        node_ip_address = services.address_to_ip(node_ip_address)
    if redis_address is not None:
        redis_address = services.address_to_ip(redis_address)

    info = {"node_ip_address": node_ip_address, "redis_address": redis_address}
    return _init(
        address_info=info,
        start_ray_local=(redis_address is None),
        num_workers=num_workers,
        driver_mode=driver_mode,
        redirect_worker_output=redirect_worker_output,
        redirect_output=redirect_output,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        resources=resources,
        num_redis_shards=num_redis_shards,
        redis_max_clients=redis_max_clients,
        plasma_directory=plasma_directory,
        huge_pages=huge_pages,
        include_webui=include_webui,
        object_store_memory=object_store_memory,
        use_raylet=use_raylet)


def cleanup(worker=global_worker):
    """Disconnect the worker, and terminate any processes started in init.

    This will automatically run at the end when a Python process that uses Ray
    exits. It is ok to run this twice in a row. Note that we manually call
    services.cleanup() in the tests because we need to start and stop many
    clusters in the tests, but the import and exit only happen once.
    """
    disconnect(worker)
    if hasattr(worker, "local_scheduler_client"):
        del worker.local_scheduler_client
    if hasattr(worker, "plasma_client"):
        worker.plasma_client.disconnect()

    if worker.mode in [SCRIPT_MODE, SILENT_MODE]:
        # If this is a driver, push the finish time to Redis and clean up any
        # other services that were started with the driver.
        worker.redis_client.hmset(b"Drivers:" + worker.worker_id,
                                  {"end_time": time.time()})
        services.cleanup()
    else:
        # If this is not a driver, make sure there are no orphan processes,
        # besides possibly the worker itself.
        for process_type, processes in services.all_processes.items():
            if process_type == services.PROCESS_TYPE_WORKER:
                assert (len(processes)) <= 1
            else:
                assert (len(processes) == 0)

    worker.set_mode(None)


atexit.register(cleanup)

# Define a custom excepthook so that if the driver exits with an exception, we
# can push that exception to Redis.
normal_excepthook = sys.excepthook


def custom_excepthook(type, value, tb):
    # If this is a driver, push the exception to redis.
    if global_worker.mode in [SCRIPT_MODE, SILENT_MODE]:
        error_message = "".join(traceback.format_tb(tb))
        global_worker.redis_client.hmset(b"Drivers:" + global_worker.worker_id,
                                         {"exception": error_message})
    # Call the normal excepthook.
    normal_excepthook(type, value, tb)


sys.excepthook = custom_excepthook


def print_error_messages(worker):
    """Print error messages in the background on the driver.

    This runs in a separate thread on the driver and prints error messages in
    the background.
    """
    # TODO(rkn): All error messages should have a "component" field indicating
    # which process the error came from (e.g., a worker or a plasma store).
    # Currently all error messages come from workers.

    worker.error_message_pubsub_client = worker.redis_client.pubsub()
    # Exports that are published after the call to
    # error_message_pubsub_client.subscribe and before the call to
    # error_message_pubsub_client.listen will still be processed in the loop.
    worker.error_message_pubsub_client.subscribe("__keyspace@0__:ErrorKeys")
    num_errors_received = 0

    # Keep a set of all the error messages that we've seen so far in order to
    # avoid printing the same error message repeatedly. This is especially
    # important when running a script inside of a tool like screen where
    # scrolling is difficult.
    old_error_messages = set()

    # Get the exports that occurred before the call to subscribe.
    with worker.lock:
        error_keys = worker.redis_client.lrange("ErrorKeys", 0, -1)
        for error_key in error_keys:
            if error_applies_to_driver(error_key, worker=worker):
                error_message = worker.redis_client.hget(
                    error_key, "message").decode("ascii")
                if error_message not in old_error_messages:
                    print(error_message)
                    old_error_messages.add(error_message)
                else:
                    print("Suppressing duplicate error message.")
            num_errors_received += 1

    try:
        for msg in worker.error_message_pubsub_client.listen():
            with worker.lock:
                for error_key in worker.redis_client.lrange(
                        "ErrorKeys", num_errors_received, -1):
                    if error_applies_to_driver(error_key, worker=worker):
                        error_message = worker.redis_client.hget(
                            error_key, "message").decode("ascii")
                        if error_message not in old_error_messages:
                            print(error_message)
                            old_error_messages.add(error_message)
                        else:
                            print("Suppressing duplicate error message.")
                    num_errors_received += 1
    except redis.ConnectionError:
        # When Redis terminates the listen call will throw a ConnectionError,
        # which we catch here.
        pass


def fetch_and_register_remote_function(key, worker=global_worker):
    """Import a remote function."""
    (driver_id, function_id_str, function_name, serialized_function,
     num_return_vals, module, resources,
     max_calls) = worker.redis_client.hmget(key, [
         "driver_id", "function_id", "name", "function", "num_return_vals",
         "module", "resources", "max_calls"
     ])
    function_id = ray.ObjectID(function_id_str)
    function_name = function_name.decode("ascii")
    max_calls = int(max_calls)
    module = module.decode("ascii")

    # This is a placeholder in case the function can't be unpickled. This will
    # be overwritten if the function is successfully registered.
    def f():
        raise Exception("This function was not imported properly.")

    worker.function_execution_info[driver_id][function_id.id()] = (
        FunctionExecutionInfo(
            function=f, function_name=function_name, max_calls=max_calls))
    worker.num_task_executions[driver_id][function_id.id()] = 0

    try:
        function = pickle.loads(serialized_function)
    except Exception:
        # If an exception was thrown when the remote function was imported, we
        # record the traceback and notify the scheduler of the failure.
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        # Log the error message.
        ray.utils.push_error_to_driver(
            worker.redis_client,
            "register_remote_function",
            traceback_str,
            driver_id=driver_id,
            data={
                "function_id": function_id.id(),
                "function_name": function_name
            })
    else:
        # TODO(rkn): Why is the below line necessary?
        function.__module__ = module
        worker.function_execution_info[driver_id][function_id.id()] = (
            FunctionExecutionInfo(
                function=function,
                function_name=function_name,
                max_calls=max_calls))
        # Add the function to the function table.
        worker.redis_client.rpush(b"FunctionTable:" + function_id.id(),
                                  worker.worker_id)


def fetch_and_execute_function_to_run(key, worker=global_worker):
    """Run on arbitrary function on the worker."""
    driver_id, serialized_function = worker.redis_client.hmget(
        key, ["driver_id", "function"])

    if (worker.mode in [SCRIPT_MODE, SILENT_MODE]
            and driver_id != worker.task_driver_id.id()):
        # This export was from a different driver and there's no need for this
        # driver to import it.
        return

    try:
        # Deserialize the function.
        function = pickle.loads(serialized_function)
        # Run the function.
        function({"worker": worker})
    except Exception:
        # If an exception was thrown when the function was run, we record the
        # traceback and notify the scheduler of the failure.
        traceback_str = traceback.format_exc()
        # Log the error message.
        name = function.__name__ if ("function" in locals()
                                     and hasattr(function, "__name__")) else ""
        ray.utils.push_error_to_driver(
            worker.redis_client,
            "function_to_run",
            traceback_str,
            driver_id=driver_id,
            data={"name": name})


def import_thread(worker, mode):
    worker.import_pubsub_client = worker.redis_client.pubsub()
    # Exports that are published after the call to
    # import_pubsub_client.subscribe and before the call to
    # import_pubsub_client.listen will still be processed in the loop.
    worker.import_pubsub_client.subscribe("__keyspace@0__:Exports")
    # Keep track of the number of imports that we've imported.
    num_imported = 0

    # Get the exports that occurred before the call to subscribe.
    with worker.lock:
        export_keys = worker.redis_client.lrange("Exports", 0, -1)
        for key in export_keys:
            num_imported += 1

            # Handle the driver case first.
            if mode != WORKER_MODE:
                if key.startswith(b"FunctionsToRun"):
                    fetch_and_execute_function_to_run(key, worker=worker)
                # Continue because FunctionsToRun are the only things that the
                # driver should import.
                continue

            if key.startswith(b"RemoteFunction"):
                fetch_and_register_remote_function(key, worker=worker)
            elif key.startswith(b"FunctionsToRun"):
                fetch_and_execute_function_to_run(key, worker=worker)
            elif key.startswith(b"ActorClass"):
                # Keep track of the fact that this actor class has been
                # exported so that we know it is safe to turn this worker into
                # an actor of that class.
                worker.imported_actor_classes.add(key)
            else:
                raise Exception("This code should be unreachable.")

    try:
        for msg in worker.import_pubsub_client.listen():
            with worker.lock:
                if msg["type"] == "subscribe":
                    continue
                assert msg["data"] == b"rpush"
                num_imports = worker.redis_client.llen("Exports")
                assert num_imports >= num_imported
                for i in range(num_imported, num_imports):
                    num_imported += 1
                    key = worker.redis_client.lindex("Exports", i)

                    # Handle the driver case first.
                    if mode != WORKER_MODE:
                        if key.startswith(b"FunctionsToRun"):
                            with log_span(
                                    "ray:import_function_to_run",
                                    worker=worker):
                                fetch_and_execute_function_to_run(
                                    key, worker=worker)
                        # Continue because FunctionsToRun are the only things
                        # that the driver should import.
                        continue

                    if key.startswith(b"RemoteFunction"):
                        with log_span(
                                "ray:import_remote_function", worker=worker):
                            fetch_and_register_remote_function(
                                key, worker=worker)
                    elif key.startswith(b"FunctionsToRun"):
                        with log_span(
                                "ray:import_function_to_run", worker=worker):
                            fetch_and_execute_function_to_run(
                                key, worker=worker)
                    elif key.startswith(b"ActorClass"):
                        # Keep track of the fact that this actor class has been
                        # exported so that we know it is safe to turn this
                        # worker into an actor of that class.
                        worker.imported_actor_classes.add(key)

                    # TODO(rkn): We may need to bring back the case of fetching
                    # actor classes here.
                    else:
                        raise Exception("This code should be unreachable.")
    except redis.ConnectionError:
        # When Redis terminates the listen call will throw a ConnectionError,
        # which we catch here.
        pass


def connect(info,
            object_id_seed=None,
            mode=WORKER_MODE,
            worker=global_worker,
            use_raylet=False):
    """Connect this worker to the local scheduler, to Plasma, and to Redis.

    Args:
        info (dict): A dictionary with address of the Redis server and the
            sockets of the plasma store, plasma manager, and local scheduler.
        object_id_seed: A seed to use to make the generation of object IDs
            deterministic.
        mode: The mode of the worker. One of SCRIPT_MODE, WORKER_MODE,
            PYTHON_MODE, and SILENT_MODE.
        use_raylet: True if the new raylet code path should be used. This is
            not supported yet.
    """
    check_main_thread()
    # Do some basic checking to make sure we didn't call ray.init twice.
    error_message = "Perhaps you called ray.init twice by accident?"
    assert not worker.connected, error_message
    assert worker.cached_functions_to_run is not None, error_message
    assert worker.cached_remote_functions_and_actors is not None, error_message
    # Initialize some fields.
    worker.worker_id = random_string()

    # When tasks are executed on remote workers in the context of multiple
    # drivers, the task driver ID is used to keep track of which driver is
    # responsible for the task so that error messages will be propagated to
    # the correct driver.
    if mode != WORKER_MODE:
        worker.task_driver_id = ray.ObjectID(worker.worker_id)

    # All workers start out as non-actors. A worker can be turned into an actor
    # after it is created.
    worker.actor_id = NIL_ACTOR_ID
    worker.connected = True
    worker.set_mode(mode)
    worker.use_raylet = use_raylet

    # The worker.events field is used to aggregate logging information and
    # display it in the web UI. Note that Python lists protected by the GIL,
    # which is important because we will append to this field from multiple
    # threads.
    worker.events = []
    # If running Ray in PYTHON_MODE, there is no need to create call
    # create_worker or to start the worker service.
    if mode == PYTHON_MODE:
        return
    # Set the node IP address.
    worker.node_ip_address = info["node_ip_address"]
    worker.redis_address = info["redis_address"]

    # Create a Redis client.
    redis_ip_address, redis_port = info["redis_address"].split(":")
    worker.redis_client = redis.StrictRedis(
        host=redis_ip_address, port=int(redis_port))

    # For driver's check that the version information matches the version
    # information that the Ray cluster was started with.
    try:
        ray.services.check_version_info(worker.redis_client)
    except Exception as e:
        if mode in [SCRIPT_MODE, SILENT_MODE]:
            raise e
        elif mode == WORKER_MODE:
            traceback_str = traceback.format_exc()
            ray.utils.push_error_to_driver(
                worker.redis_client,
                "version_mismatch",
                traceback_str,
                driver_id=None)

    worker.lock = threading.Lock()

    # Check the RedirectOutput key in Redis and based on its value redirect
    # worker output and error to their own files.
    if mode == WORKER_MODE:
        # This key is set in services.py when Redis is started.
        redirect_worker_output_val = worker.redis_client.get("RedirectOutput")
        if (redirect_worker_output_val is not None
                and int(redirect_worker_output_val) == 1):
            redirect_worker_output = 1
        else:
            redirect_worker_output = 0
        if redirect_worker_output:
            log_stdout_file, log_stderr_file = services.new_log_files(
                "worker", True)
            sys.stdout = log_stdout_file
            sys.stderr = log_stderr_file
            services.record_log_files_in_redis(
                info["redis_address"], info["node_ip_address"],
                [log_stdout_file, log_stderr_file])

    # Create an object for interfacing with the global state.
    global_state._initialize_global_state(redis_ip_address, int(redis_port))

    # Register the worker with Redis.
    if mode in [SCRIPT_MODE, SILENT_MODE]:
        # The concept of a driver is the same as the concept of a "job".
        # Register the driver/job with Redis here.
        import __main__ as main
        driver_info = {
            "node_ip_address": worker.node_ip_address,
            "driver_id": worker.worker_id,
            "start_time": time.time(),
            "plasma_store_socket": info["store_socket_name"],
            "plasma_manager_socket": info.get("manager_socket_name"),
            "local_scheduler_socket": info.get("local_scheduler_socket_name"),
            "raylet_socket": info.get("raylet_socket_name")
        }
        driver_info["name"] = (main.__file__ if hasattr(main, "__file__") else
                               "INTERACTIVE MODE")
        worker.redis_client.hmset(b"Drivers:" + worker.worker_id, driver_info)
        if not worker.redis_client.exists("webui"):
            worker.redis_client.hmset("webui", {"url": info["webui_url"]})
        is_worker = False
    elif mode == WORKER_MODE:
        # Register the worker with Redis.
        worker_dict = {
            "node_ip_address": worker.node_ip_address,
            "plasma_store_socket": info["store_socket_name"],
            "plasma_manager_socket": info["manager_socket_name"],
            "local_scheduler_socket": info["local_scheduler_socket_name"]
        }
        if redirect_worker_output:
            worker_dict["stdout_file"] = os.path.abspath(log_stdout_file.name)
            worker_dict["stderr_file"] = os.path.abspath(log_stderr_file.name)
        worker.redis_client.hmset(b"Workers:" + worker.worker_id, worker_dict)
        is_worker = True
    else:
        raise Exception("This code should be unreachable.")

    # Create an object store client.
    if not worker.use_raylet:
        worker.plasma_client = plasma.connect(info["store_socket_name"],
                                              info["manager_socket_name"], 64)
    else:
        worker.plasma_client = plasma.connect(info["store_socket_name"], "",
                                              64)

    if not worker.use_raylet:
        local_scheduler_socket = info["local_scheduler_socket_name"]
    else:
        local_scheduler_socket = info["raylet_socket_name"]

    worker.local_scheduler_client = ray.local_scheduler.LocalSchedulerClient(
        local_scheduler_socket, worker.worker_id, is_worker)

    # If this is a driver, set the current task ID, the task driver ID, and set
    # the task index to 0.
    if mode in [SCRIPT_MODE, SILENT_MODE]:
        # If the user provided an object_id_seed, then set the current task ID
        # deterministically based on that seed (without altering the state of
        # the user's random number generator). Otherwise, set the current task
        # ID randomly to avoid object ID collisions.
        numpy_state = np.random.get_state()
        if object_id_seed is not None:
            np.random.seed(object_id_seed)
        else:
            # Try to use true randomness.
            np.random.seed(None)
        worker.current_task_id = ray.ObjectID(np.random.bytes(20))
        # Reset the state of the numpy random number generator.
        np.random.set_state(numpy_state)
        # Set other fields needed for computing task IDs.
        worker.task_index = 0
        worker.put_index = 1

        # Create an entry for the driver task in the task table. This task is
        # added immediately with status RUNNING. This allows us to push errors
        # related to this driver task back to the driver.  For example, if the
        # driver creates an object that is later evicted, we should notify the
        # user that we're unable to reconstruct the object, since we cannot
        # rerun the driver.
        nil_actor_counter = 0

        driver_task = ray.local_scheduler.Task(
            worker.task_driver_id, ray.ObjectID(NIL_FUNCTION_ID), [], 0,
            worker.current_task_id, worker.task_index,
            ray.ObjectID(NIL_ACTOR_ID), ray.ObjectID(NIL_ACTOR_ID),
            ray.ObjectID(NIL_ACTOR_ID), ray.ObjectID(NIL_ACTOR_ID),
            nil_actor_counter, False, [], {"CPU": 0}, worker.use_raylet)

        # Add the driver task to the task table.
        if not worker.use_raylet:
            global_state._execute_command(
                driver_task.task_id(), "RAY.TASK_TABLE_ADD",
                driver_task.task_id().id(), TASK_STATUS_RUNNING,
                NIL_LOCAL_SCHEDULER_ID,
                driver_task.execution_dependencies_string(), 0,
                ray.local_scheduler.task_to_string(driver_task))
        else:
            TablePubsub_RAYLET_TASK = 2

            # TODO(rkn): When we shard the GCS in xray, we will need to change
            # this to use _execute_command.
            global_state.redis_client.execute_command(
                "RAY.TABLE_ADD", state.TablePrefix_RAYLET_TASK,
                TablePubsub_RAYLET_TASK,
                driver_task.task_id().id(),
                driver_task._serialized_raylet_task())

        # Set the driver's current task ID to the task ID assigned to the
        # driver task.
        worker.current_task_id = driver_task.task_id()

    # Initialize the serialization library. This registers some classes, and so
    # it must be run before we export all of the cached remote functions.
    _initialize_serialization()

    # Start a thread to import exports from the driver or from other workers.
    # Note that the driver also has an import thread, which is used only to
    # import custom class definitions from calls to register_custom_serializer
    # that happen under the hood on workers.
    t = threading.Thread(target=import_thread, args=(worker, mode))
    # Making the thread a daemon causes it to exit when the main thread exits.
    t.daemon = True
    t.start()

    # If this is a driver running in SCRIPT_MODE, start a thread to print error
    # messages asynchronously in the background. Ideally the scheduler would
    # push messages to the driver's worker service, but we ran into bugs when
    # trying to properly shutdown the driver's worker service, so we are
    # temporarily using this implementation which constantly queries the
    # scheduler for new error messages.
    if mode == SCRIPT_MODE:
        t = threading.Thread(target=print_error_messages, args=(worker, ))
        # Making the thread a daemon causes it to exit when the main thread
        # exits.
        t.daemon = True
        t.start()

    if mode in [SCRIPT_MODE, SILENT_MODE]:
        # Add the directory containing the script that is running to the Python
        # paths of the workers. Also add the current directory. Note that this
        # assumes that the directory structures on the machines in the clusters
        # are the same.
        script_directory = os.path.abspath(os.path.dirname(sys.argv[0]))
        current_directory = os.path.abspath(os.path.curdir)
        worker.run_function_on_all_workers(
            lambda worker_info: sys.path.insert(1, script_directory))
        worker.run_function_on_all_workers(
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
        # Export cached functions_to_run.
        for function in worker.cached_functions_to_run:
            worker.run_function_on_all_workers(function)
        # Export cached remote functions to the workers.
        for cached_type, info in worker.cached_remote_functions_and_actors:
            if cached_type == "remote_function":
                info._export()
            elif cached_type == "actor":
                (key, actor_class_info) = info
                ray.actor.publish_actor_class_to_key(key, actor_class_info,
                                                     worker)
            else:
                assert False, "This code should be unreachable."
    worker.cached_functions_to_run = None
    worker.cached_remote_functions_and_actors = None


def disconnect(worker=global_worker):
    """Disconnect this worker from the scheduler and object store."""
    # Reset the list of cached remote functions and actors so that if more
    # remote functions or actors are defined and then connect is called again,
    # the remote functions will be exported. This is mostly relevant for the
    # tests.
    worker.connected = False
    worker.cached_functions_to_run = []
    worker.cached_remote_functions_and_actors = []
    worker.serialization_context = pyarrow.SerializationContext()


def _try_to_compute_deterministic_class_id(cls, depth=5):
    """Attempt to produce a deterministic class ID for a given class.

    The goal here is for the class ID to be the same when this is run on
    different worker processes. Pickling, loading, and pickling again seems to
    produce more consistent results than simply pickling. This is a bit crazy
    and could cause problems, in which case we should revert it and figure out
    something better.

    Args:
        cls: The class to produce an ID for.
        depth: The number of times to repeatedly try to load and dump the
            string while trying to reach a fixed point.

    Returns:
        A class ID for this class. We attempt to make the class ID the same
            when this function is run on different workers, but that is not
            guaranteed.

    Raises:
        Exception: This could raise an exception if cloudpickle raises an
            exception.
    """
    # Pickling, loading, and pickling again seems to produce more consistent
    # results than simply pickling. This is a bit
    class_id = pickle.dumps(cls)
    for _ in range(depth):
        new_class_id = pickle.dumps(pickle.loads(class_id))
        if new_class_id == class_id:
            # We appear to have reached a fix point, so use this as the ID.
            return hashlib.sha1(new_class_id).digest()
        class_id = new_class_id

    # We have not reached a fixed point, so we may end up with a different
    # class ID for this custom class on each worker, which could lead to the
    # same class definition being exported many many times.
    print(
        "WARNING: Could not produce a deterministic class ID for class "
        "{}".format(cls),
        file=sys.stderr)
    return hashlib.sha1(new_class_id).digest()


def register_custom_serializer(cls,
                               use_pickle=False,
                               use_dict=False,
                               serializer=None,
                               deserializer=None,
                               local=False,
                               worker=global_worker):
    """Enable serialization and deserialization for a particular class.

    This method runs the register_class function defined below on every worker,
    which will enable ray to properly serialize and deserialize objects of
    this class.

    Args:
        cls (type): The class that ray should serialize.
        use_pickle (bool): If true, then objects of this class will be
            serialized using pickle.
        use_dict: If true, then objects of this class be serialized turning
            their __dict__ fields into a dictionary. Must be False if
            use_pickle is true.
        serializer: The custom serializer to use. This should be provided if
            and only if use_pickle and use_dict are False.
        deserializer: The custom deserializer to use. This should be provided
            if and only if use_pickle and use_dict are False.
        local: True if the serializers should only be registered on the current
            worker. This should usually be False.

    Raises:
        Exception: An exception is raised if pickle=False and the class cannot
            be efficiently serialized by Ray. This can also raise an exception
            if use_dict is true and cls is not pickleable.
    """
    assert (serializer is None) == (deserializer is None), (
        "The serializer/deserializer arguments must both be provided or "
        "both not be provided.")
    use_custom_serializer = (serializer is not None)

    assert use_custom_serializer + use_pickle + use_dict == 1, (
        "Exactly one of use_pickle, use_dict, or serializer/deserializer must "
        "be specified.")

    if use_dict:
        # Raise an exception if cls cannot be serialized efficiently by Ray.
        serialization.check_serializable(cls)

    if not local:
        # In this case, the class ID will be used to deduplicate the class
        # across workers. Note that cloudpickle unfortunately does not produce
        # deterministic strings, so these IDs could be different on different
        # workers. We could use something weaker like cls.__name__, however
        # that would run the risk of having collisions. TODO(rkn): We should
        # improve this.
        try:
            # Attempt to produce a class ID that will be the same on each
            # worker. However, determinism is not guaranteed, and the result
            # may be different on different workers.
            class_id = _try_to_compute_deterministic_class_id(cls)
        except Exception as e:
            raise serialization.CloudPickleError("Failed to pickle class "
                                                 "'{}'".format(cls))
    else:
        # In this case, the class ID only needs to be meaningful on this worker
        # and not across workers.
        class_id = random_string()

    def register_class_for_serialization(worker_info):
        # TODO(rkn): We need to be more thoughtful about what to do if custom
        # serializers have already been registered for class_id. In some cases,
        # we may want to use the last user-defined serializers and ignore
        # subsequent calls to register_custom_serializer that were made by the
        # system.
        worker_info["worker"].serialization_context.register_type(
            cls,
            class_id,
            pickle=use_pickle,
            custom_serializer=serializer,
            custom_deserializer=deserializer)

    if not local:
        worker.run_function_on_all_workers(register_class_for_serialization)
    else:
        # Since we are pickling objects of this class, we don't actually need
        # to ship the class definition.
        register_class_for_serialization({"worker": worker})


class RayLogSpan(object):
    """An object used to enable logging a span of events with a with statement.

    Attributes:
        event_type (str): The type of the event being logged.
        contents: Additional information to log.
    """

    def __init__(self, event_type, contents=None, worker=global_worker):
        """Initialize a RayLogSpan object."""
        self.event_type = event_type
        self.contents = contents
        self.worker = worker

    def __enter__(self):
        """Log the beginning of a span event."""
        log(event_type=self.event_type,
            contents=self.contents,
            kind=LOG_SPAN_START,
            worker=self.worker)

    def __exit__(self, type, value, tb):
        """Log the end of a span event. Log any exception that occurred."""
        if type is None:
            log(event_type=self.event_type,
                kind=LOG_SPAN_END,
                worker=self.worker)
        else:
            log(event_type=self.event_type,
                contents={
                    "type": str(type),
                    "value": value,
                    "traceback": traceback.format_exc()
                },
                kind=LOG_SPAN_END,
                worker=self.worker)


def log_span(event_type, contents=None, worker=global_worker):
    return RayLogSpan(event_type, contents=contents, worker=worker)


def log_event(event_type, contents=None, worker=global_worker):
    log(event_type, kind=LOG_POINT, contents=contents, worker=worker)


def log(event_type, kind, contents=None, worker=global_worker):
    """Log an event to the global state store.

    This adds the event to a buffer of events locally. The buffer can be
    flushed and written to the global state store by calling flush_log().

    Args:
        event_type (str): The type of the event.
        contents: More general data to store with the event.
        kind (int): Either LOG_POINT, LOG_SPAN_START, or LOG_SPAN_END. This is
            LOG_POINT if the event being logged happens at a single point in
            time. It is LOG_SPAN_START if we are starting to log a span of
            time, and it is LOG_SPAN_END if we are finishing logging a span of
            time.
    """
    # TODO(rkn): This code currently takes around half a microsecond. Since we
    # call it tens of times per task, this adds up. We will need to redo the
    # logging code, perhaps in C.
    contents = {} if contents is None else contents
    assert isinstance(contents, dict)
    # Make sure all of the keys and values in the dictionary are strings.
    contents = {str(k): str(v) for k, v in contents.items()}
    # Log the event if this is a worker and not a driver, since the driver's
    # event log never gets flushed.
    if worker.mode == WORKER_MODE:
        worker.events.append((time.time(), event_type, kind, contents))


def flush_log(worker=global_worker):
    """Send the logged worker events to the global state store."""
    event_log_key = b"event_log:" + worker.worker_id
    event_log_value = json.dumps(worker.events)
    if not worker.use_raylet:
        worker.local_scheduler_client.log_event(event_log_key, event_log_value,
                                                time.time())
    worker.events = []


def get(object_ids, worker=global_worker):
    """Get a remote object or a list of remote objects from the object store.

    This method blocks until the object corresponding to the object ID is
    available in the local object store. If this object is not in the local
    object store, it will be shipped from an object store that has it (once the
    object has been created). If object_ids is a list, then the objects
    corresponding to each object in the list will be returned.

    Args:
        object_ids: Object ID of the object to get or a list of object IDs to
            get.

    Returns:
        A Python object or a list of Python objects.
    """
    worker.check_connected()
    with log_span("ray:get", worker=worker):
        check_main_thread()

        if worker.mode == PYTHON_MODE:
            # In PYTHON_MODE, ray.get is the identity operation (the input will
            # actually be a value not an objectid).
            return object_ids
        if isinstance(object_ids, list):
            values = worker.get_object(object_ids)
            for i, value in enumerate(values):
                if isinstance(value, RayTaskError):
                    raise RayGetError(object_ids[i], value)
            return values
        else:
            value = worker.get_object([object_ids])[0]
            if isinstance(value, RayTaskError):
                # If the result is a RayTaskError, then the task that created
                # this object failed, and we should propagate the error message
                # here.
                raise RayGetError(object_ids, value)
            return value


def put(value, worker=global_worker):
    """Store an object in the object store.

    Args:
        value: The Python object to be stored.

    Returns:
        The object ID assigned to this value.
    """
    worker.check_connected()
    with log_span("ray:put", worker=worker):
        check_main_thread()

        if worker.mode == PYTHON_MODE:
            # In PYTHON_MODE, ray.put is the identity operation.
            return value
        object_id = worker.local_scheduler_client.compute_put_id(
            worker.current_task_id, worker.put_index, worker.use_raylet)
        worker.put_object(object_id, value)
        worker.put_index += 1
        return object_id


def wait(object_ids, num_returns=1, timeout=None, worker=global_worker):
    """Return a list of IDs that are ready and a list of IDs that are not.

    If timeout is set, the function returns either when the requested number of
    IDs are ready or when the timeout is reached, whichever occurs first. If it
    is not set, the function simply waits until that number of objects is ready
    and returns that exact number of object_ids.

    This method returns two lists. The first list consists of object IDs that
    correspond to objects that are stored in the object store. The second list
    corresponds to the rest of the object IDs (which may or may not be ready).

    Ordering of the input list of object IDs is preserved: if A precedes B in
    the input list, and both are in the ready list, then A will precede B in
    the ready list. This also holds true if A and B are both in the remaining
    list.

    Args:
        object_ids (List[ObjectID]): List of object IDs for objects that may or
            may not be ready. Note that these IDs must be unique.
        num_returns (int): The number of object IDs that should be returned.
        timeout (int): The maximum amount of time in milliseconds to wait
            before returning.

    Returns:
        A list of object IDs that are ready and a list of the remaining object
            IDs.
    """

    if isinstance(object_ids, ray.ObjectID):
        raise TypeError(
            "wait() expected a list of ObjectID, got a single ObjectID")

    if not isinstance(object_ids, list):
        raise TypeError("wait() expected a list of ObjectID, got {}".format(
            type(object_ids)))

    if worker.mode != PYTHON_MODE:
        for object_id in object_ids:
            if not isinstance(object_id, ray.ObjectID):
                raise TypeError("wait() expected a list of ObjectID, "
                                "got list containing {}".format(
                                    type(object_id)))

    worker.check_connected()
    with log_span("ray:wait", worker=worker):
        check_main_thread()

        # When Ray is run in PYTHON_MODE, all functions are run immediately,
        # so all objects in object_id are ready.
        if worker.mode == PYTHON_MODE:
            return object_ids[:num_returns], object_ids[num_returns:]

        # TODO(rkn): This is a temporary workaround for
        # https://github.com/ray-project/ray/issues/997. However, it should be
        # fixed in Arrow instead of here.
        if len(object_ids) == 0:
            return [], []

        if len(object_ids) != len(set(object_ids)):
            raise Exception("Wait requires a list of unique object IDs.")
        if num_returns <= 0:
            raise Exception(
                "Invalid number of objects to return %d." % num_returns)
        if num_returns > len(object_ids):
            raise Exception("num_returns cannot be greater than the number "
                            "of objects provided to ray.wait.")
        timeout = timeout if timeout is not None else 2**30
        if worker.use_raylet:
            ready_ids, remaining_ids = worker.local_scheduler_client.wait(
                object_ids, num_returns, timeout, False)
        else:
            object_id_strs = [
                plasma.ObjectID(object_id.id()) for object_id in object_ids
            ]
            ready_ids, remaining_ids = worker.plasma_client.wait(
                object_id_strs, timeout, num_returns)
            ready_ids = [
                ray.ObjectID(object_id.binary()) for object_id in ready_ids
            ]
            remaining_ids = [
                ray.ObjectID(object_id.binary()) for object_id in remaining_ids
            ]
        return ready_ids, remaining_ids


def _mode(worker=global_worker):
    """This is a wrapper around worker.mode.

    We use this wrapper so that in the remote decorator, we can call _mode()
    instead of worker.mode. The difference is that when we attempt to serialize
    remote functions, we don't attempt to serialize the worker object, which
    cannot be serialized.
    """
    return worker.mode


def get_global_worker():
    return global_worker


def make_decorator(num_return_vals=None,
                   num_cpus=None,
                   num_gpus=None,
                   resources=None,
                   max_calls=None,
                   checkpoint_interval=None,
                   worker=None):
    def decorator(function_or_class):
        if (inspect.isfunction(function_or_class)
                or is_cython(function_or_class)):
            # Set the remote function default resources.
            if checkpoint_interval is not None:
                raise Exception("The keyword 'checkpoint_interval' is not "
                                "allowed for remote functions.")

            return ray.remote_function.RemoteFunction(
                function_or_class, num_cpus, num_gpus, resources,
                num_return_vals, max_calls)

        if inspect.isclass(function_or_class):
            if num_return_vals is not None:
                raise Exception("The keyword 'num_return_vals' is not allowed "
                                "for actors.")
            if max_calls is not None:
                raise Exception("The keyword 'max_calls' is not allowed for "
                                "actors.")

            # Set the actor default resources.
            if num_cpus is None and num_gpus is None and resources is None:
                # In the default case, actors acquire no resources for
                # their lifetime, and actor methods will require 1 CPU.
                cpus_to_use = DEFAULT_ACTOR_CREATION_CPUS_SIMPLE_CASE
                actor_method_cpus = DEFAULT_ACTOR_METHOD_CPUS_SIMPLE_CASE
            else:
                # If any resources are specified, then all resources are
                # acquired for the actor's lifetime and no resources are
                # associated with methods.
                cpus_to_use = (DEFAULT_ACTOR_CREATION_CPUS_SPECIFIED_CASE
                               if num_cpus is None else num_cpus)
                actor_method_cpus = DEFAULT_ACTOR_METHOD_CPUS_SPECIFIED_CASE

            return worker.make_actor(function_or_class, cpus_to_use, num_gpus,
                                     resources, actor_method_cpus,
                                     checkpoint_interval)

        raise Exception("The @ray.remote decorator must be applied to "
                        "either a function or to a class.")

    return decorator


def remote(*args, **kwargs):
    worker = get_global_worker()

    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # This is the case where the decorator is just @ray.remote.
        return make_decorator(worker=worker)(args[0])

    # Parse the keyword arguments from the decorator.
    error_string = ("The @ray.remote decorator must be applied either "
                    "with no arguments and no parentheses, for example "
                    "'@ray.remote', or it must be applied using some of "
                    "the arguments 'num_return_vals', 'num_cpus', 'num_gpus', "
                    "'resources', 'max_calls', or 'checkpoint_interval', like "
                    "'@ray.remote(num_return_vals=2, "
                    "resources={\"CustomResource\": 1})'.")
    assert len(args) == 0 and len(kwargs) > 0, error_string
    for key in kwargs:
        assert key in [
            "num_return_vals", "num_cpus", "num_gpus", "resources",
            "max_calls", "checkpoint_interval"
        ], error_string

    num_cpus = kwargs["num_cpus"] if "num_cpus" in kwargs else None
    num_gpus = kwargs["num_gpus"] if "num_gpus" in kwargs else None
    resources = kwargs.get("resources")
    if not isinstance(resources, dict) and resources is not None:
        raise Exception("The 'resources' keyword argument must be a "
                        "dictionary, but received type {}.".format(
                            type(resources)))
    if resources is not None:
        assert "CPU" not in resources, "Use the 'num_cpus' argument."
        assert "GPU" not in resources, "Use the 'num_gpus' argument."

    # Handle other arguments.
    num_return_vals = kwargs.get("num_return_vals")
    max_calls = kwargs.get("max_calls")
    checkpoint_interval = kwargs.get("checkpoint_interval")

    return make_decorator(
        num_return_vals=num_return_vals,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        resources=resources,
        max_calls=max_calls,
        checkpoint_interval=checkpoint_interval,
        worker=worker)
