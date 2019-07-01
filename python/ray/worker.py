from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from contextlib import contextmanager
import colorama
import atexit
import faulthandler
import hashlib
import inspect
import json
import logging
import numpy as np
import os
import signal
from six.moves import queue
import sys
import threading
import time
import traceback

# Ray modules
import pyarrow
import pyarrow.plasma as plasma
import ray.cloudpickle as pickle
import ray.experimental.signal as ray_signal
import ray.experimental.no_return
import ray.gcs_utils
import ray.memory_monitor as memory_monitor
import ray.node
import ray.parameter
import ray.ray_constants as ray_constants
import ray.remote_function
import ray.serialization as serialization
import ray.services as services
import ray.signature
import ray.state

from ray import (
    ActorHandleID,
    ActorID,
    ClientID,
    WorkerID,
    JobID,
    ObjectID,
    TaskID,
)
from ray import import_thread
from ray import profiling

from ray.gcs_utils import ErrorType
from ray.exceptions import (
    RayActorError,
    RayError,
    RayTaskError,
    RayWorkerError,
    UnreconstructableError,
    RAY_EXCEPTION_TYPES,
)
from ray.function_manager import (
    FunctionActorManager,
    FunctionDescriptor,
)
from ray.utils import (
    _random_string,
    check_oversized_pickle,
    is_cython,
    setup_logger,
    thread_safe_client,
)
from ray.local_mode_manager import LocalModeManager

SCRIPT_MODE = 0
WORKER_MODE = 1
LOCAL_MODE = 2

ERROR_KEY_PREFIX = b"Error:"

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)

try:
    import setproctitle
except ImportError:
    setproctitle = None


class ActorCheckpointInfo(object):
    """Information used to maintain actor checkpoints."""

    __slots__ = [
        # Number of tasks executed since last checkpoint.
        "num_tasks_since_last_checkpoint",
        # Timestamp of the last checkpoint, in milliseconds.
        "last_checkpoint_timestamp",
        # IDs of the previous checkpoints.
        "checkpoint_ids",
    ]

    def __init__(self, num_tasks_since_last_checkpoint,
                 last_checkpoint_timestamp, checkpoint_ids):
        self.num_tasks_since_last_checkpoint = num_tasks_since_last_checkpoint
        self.last_checkpoint_timestamp = last_checkpoint_timestamp
        self.checkpoint_ids = checkpoint_ids


class Worker(object):
    """A class used to define the control flow of a worker process.

    Note:
        The methods in this class are considered unexposed to the user. The
        functions outside of this class are considered exposed.

    Attributes:
        connected (bool): True if Ray has been started and False otherwise.
        node (ray.node.Node): The node this worker is attached to.
        mode: The mode of the worker. One of SCRIPT_MODE, LOCAL_MODE, and
            WORKER_MODE.
        cached_functions_to_run (List): A list of functions to run on all of
            the workers that should be exported as soon as connect is called.
        profiler: the profiler used to aggregate profiling information.
    """

    def __init__(self):
        """Initialize a Worker object."""
        self.node = None
        self.mode = None
        self.cached_functions_to_run = []
        self.actor_init_error = None
        self.make_actor = None
        self.actors = {}
        # Information used to maintain actor checkpoints.
        self.actor_checkpoint_info = {}
        self.actor_task_counter = 0
        # The number of threads Plasma should use when putting an object in the
        # object store.
        self.memcopy_threads = 12
        # When the worker is constructed. Record the original value of the
        # CUDA_VISIBLE_DEVICES environment variable.
        self.original_gpu_ids = ray.utils.get_cuda_visible_devices()
        self.profiler = None
        self.memory_monitor = memory_monitor.MemoryMonitor()
        # A dictionary that maps from driver id to SerializationContext
        # TODO: clean up the SerializationContext once the job finished.
        self.serialization_context_map = {}
        self.function_actor_manager = FunctionActorManager(self)
        # Identity of the job that this worker is processing.
        # It is a JobID.
        self.current_job_id = JobID.nil()
        self._task_context = threading.local()
        # This event is checked regularly by all of the threads so that they
        # know when to exit.
        self.threads_stopped = threading.Event()
        # Index of the current session. This number will
        # increment every time when `ray.shutdown` is called.
        self._session_index = 0
        self._current_task = None
        # Functions to run to process the values returned by ray.get. Each
        # postprocessor must take two arguments ("object_ids", and "values").
        self._post_get_hooks = []

    @property
    def connected(self):
        return self.node is not None

    @property
    def node_ip_address(self):
        self.check_connected()
        return self.node.node_ip_address

    @property
    def load_code_from_local(self):
        self.check_connected()
        return self.node.load_code_from_local

    @property
    def task_context(self):
        """A thread-local that contains the following attributes.

        current_task_id: For the main thread, this field is the ID of this
            worker's current running task; for other threads, this field is a
            fake random ID.
        task_index: The number of tasks that have been submitted from the
            current task.
        put_index: The number of objects that have been put from the current
            task.
        """
        if not hasattr(self._task_context, "initialized"):
            # Initialize task_context for the current thread.
            if ray.utils.is_main_thread():
                # If this is running on the main thread, initialize it to
                # NIL. The actual value will set when the worker receives
                # a task from raylet backend.
                self._task_context.current_task_id = TaskID.nil()
            else:
                # If this is running on a separate thread, then the mapping
                # to the current task ID may not be correct. Generate a
                # random task ID so that the backend can differentiate
                # between different threads.
                self._task_context.current_task_id = TaskID.from_random()
                if getattr(self, "_multithreading_warned", False) is not True:
                    logger.warning(
                        "Calling ray.get or ray.wait in a separate thread "
                        "may lead to deadlock if the main thread blocks on "
                        "this thread and there are not enough resources to "
                        "execute more tasks")
                    self._multithreading_warned = True

            self._task_context.task_index = 0
            self._task_context.put_index = 1
            self._task_context.initialized = True
        return self._task_context

    @property
    def current_task_id(self):
        return self.task_context.current_task_id

    def mark_actor_init_failed(self, error):
        """Called to mark this actor as failed during initialization."""

        self.actor_init_error = error

    def reraise_actor_init_error(self):
        """Raises any previous actor initialization error."""

        if self.actor_init_error is not None:
            raise self.actor_init_error

    def get_serialization_context(self, job_id):
        """Get the SerializationContext of the job that this worker is processing.

        Args:
            job_id: The ID of the job that indicates which job to get
                the serialization context for.

        Returns:
            The serialization context of the given job.
        """
        # This function needs to be proctected by a lock, because it will be
        # called by`register_class_for_serialization`, as well as the import
        # thread, from different threads. Also, this function will recursively
        # call itself, so we use RLock here.
        with self.lock:
            if job_id not in self.serialization_context_map:
                _initialize_serialization(job_id)
            return self.serialization_context_map[job_id]

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

        The mode LOCAL_MODE should be used if this Worker is a driver and if
        you want to run the driver in a manner equivalent to serial Python for
        debugging purposes. It will not send remote function calls to the
        scheduler and will instead execute them in a blocking fashion.

        Args:
            mode: One of SCRIPT_MODE, WORKER_MODE, and LOCAL_MODE.
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
                if isinstance(value, bytes):
                    # If the object is a byte array, skip serializing it and
                    # use a special metadata to indicate it's raw binary. So
                    # that this object can also be read by Java.
                    self.plasma_client.put_raw_buffer(
                        value,
                        object_id=pyarrow.plasma.ObjectID(object_id.binary()),
                        metadata=ray_constants.RAW_BUFFER_METADATA,
                        memcopy_threads=self.memcopy_threads)
                else:
                    self.plasma_client.put(
                        value,
                        object_id=pyarrow.plasma.ObjectID(object_id.binary()),
                        memcopy_threads=self.memcopy_threads,
                        serialization_context=self.get_serialization_context(
                            self.current_job_id))
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
                    logger.debug(warning_message)
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
                        logger.warning(warning_message)
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
                        logger.warning(warning_message)

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
        if isinstance(value, ObjectID):
            raise TypeError(
                "Calling 'put' on an ray.ObjectID is not allowed "
                "(similarly, returning an ray.ObjectID from a remote "
                "function is not allowed). If you really want to "
                "do this, you can wrap the ray.ObjectID in a list and "
                "call 'put' on it (or return it).")

        # Serialize and put the object in the object store.
        try:
            self.store_and_register(object_id, value)
        except pyarrow.PlasmaObjectExists:
            # The object already exists in the object store, so there is no
            # need to add it again. TODO(rkn): We need to compare the hashes
            # and make sure that the objects are in fact the same. We also
            # should return an error code to the caller instead of printing a
            # message.
            logger.info(
                "The object with ID {} already exists in the object store.".
                format(object_id))
        except TypeError:
            # This error can happen because one of the members of the object
            # may not be serializable for cloudpickle. So we need these extra
            # fallbacks here to start from the beginning. Hopefully the object
            # could have a `__reduce__` method.
            register_custom_serializer(type(value), use_pickle=True)
            warning_message = (
                "WARNING: Serializing the class {} failed, "
                "so are are falling back to cloudpickle.".format(type(value)))
            logger.warning(warning_message)
            self.store_and_register(object_id, value)

    def retrieve_and_deserialize(self, object_ids, timeout, error_timeout=10):
        start_time = time.time()
        # Only send the warning once.
        warning_sent = False
        serialization_context = self.get_serialization_context(
            self.current_job_id)
        while True:
            try:
                # We divide very large get requests into smaller get requests
                # so that a single get request doesn't block the store for a
                # long time, if the store is blocked, it can block the manager
                # as well as a consequence.
                results = []
                batch_size = ray._config.worker_fetch_request_size()
                for i in range(0, len(object_ids), batch_size):
                    metadata_data_pairs = self.plasma_client.get_buffers(
                        object_ids[i:i + batch_size],
                        timeout,
                        with_meta=True,
                    )
                    for j in range(len(metadata_data_pairs)):
                        metadata, data = metadata_data_pairs[j]
                        results.append(
                            self._deserialize_object_from_arrow(
                                data,
                                metadata,
                                object_ids[i + j],
                                serialization_context,
                            ))
                return results
            except pyarrow.DeserializationCallbackError:
                # Wait a little bit for the import thread to import the class.
                # If we currently have the worker lock, we need to release it
                # so that the import thread can acquire it.
                time.sleep(0.01)

                if time.time() - start_time > error_timeout:
                    warning_message = ("This worker or driver is waiting to "
                                       "receive a class definition so that it "
                                       "can deserialize an object from the "
                                       "object store. This may be fine, or it "
                                       "may be a bug.")
                    if not warning_sent:
                        ray.utils.push_error_to_driver(
                            self,
                            ray_constants.WAIT_FOR_CLASS_PUSH_ERROR,
                            warning_message,
                            job_id=self.current_job_id)
                    warning_sent = True

    def _deserialize_object_from_arrow(self, data, metadata, object_id,
                                       serialization_context):
        if metadata:
            # Check if the object should be returned as raw bytes.
            if metadata == ray_constants.RAW_BUFFER_METADATA:
                return data.to_pybytes()
            # Otherwise, return an exception object based on
            # the error type.
            error_type = int(metadata)
            if error_type == ErrorType.Value("WORKER_DIED"):
                return RayWorkerError()
            elif error_type == ErrorType.Value("ACTOR_DIED"):
                return RayActorError()
            elif error_type == ErrorType.Value("OBJECT_UNRECONSTRUCTABLE"):
                return UnreconstructableError(ray.ObjectID(object_id.binary()))
            else:
                assert False, "Unrecognized error type " + str(error_type)
        elif data:
            # If data is not empty, deserialize the object.
            # Note, the lock is needed because `serialization_context` isn't
            # thread-safe.
            with self.plasma_client.lock:
                return pyarrow.deserialize(data, serialization_context)
        else:
            # Object isn't available in plasma.
            return plasma.ObjectNotAvailable

    def get_object(self, object_ids):
        """Get the value or values in the object store associated with the IDs.

        Return the values from the local object store for object_ids. This will
        block until all the values for object_ids have been written to the
        local object store.

        Args:
            object_ids (List[object_id.ObjectID]): A list of the object IDs
                whose values should be retrieved.

        Raises:
            Exception if running in LOCAL_MODE and any of the object IDs do not
            exist in the emulated object store.
        """
        # Make sure that the values are object IDs.
        for object_id in object_ids:
            if not isinstance(object_id, ObjectID):
                raise TypeError(
                    "Attempting to call `get` on the value {}, "
                    "which is not an ray.ObjectID.".format(object_id))

        if self.mode == LOCAL_MODE:
            return self.local_mode_manager.get_object(object_ids)

        # Do an initial fetch for remote objects. We divide the fetch into
        # smaller fetches so as to not block the manager for a prolonged period
        # of time in a single call.
        plain_object_ids = [
            plasma.ObjectID(object_id.binary()) for object_id in object_ids
        ]
        for i in range(0, len(object_ids),
                       ray._config.worker_fetch_request_size()):
            self.raylet_client.fetch_or_reconstruct(
                object_ids[i:(i + ray._config.worker_fetch_request_size())],
                True)

        # Get the objects. We initially try to get the objects immediately.
        final_results = self.retrieve_and_deserialize(plain_object_ids, 0)
        # Construct a dictionary mapping object IDs that we haven't gotten yet
        # to their original index in the object_ids argument.
        unready_ids = {
            plain_object_ids[i].binary(): i
            for (i, val) in enumerate(final_results)
            if val is plasma.ObjectNotAvailable
        }

        if len(unready_ids) > 0:
            # Try reconstructing any objects we haven't gotten yet. Try to
            # get them until at least get_timeout_milliseconds
            # milliseconds passes, then repeat.
            while len(unready_ids) > 0:
                object_ids_to_fetch = [
                    plasma.ObjectID(unready_id)
                    for unready_id in unready_ids.keys()
                ]
                ray_object_ids_to_fetch = [
                    ObjectID(unready_id) for unready_id in unready_ids.keys()
                ]
                fetch_request_size = ray._config.worker_fetch_request_size()
                for i in range(0, len(object_ids_to_fetch),
                               fetch_request_size):
                    self.raylet_client.fetch_or_reconstruct(
                        ray_object_ids_to_fetch[i:(i + fetch_request_size)],
                        False,
                        self.current_task_id,
                    )
                results = self.retrieve_and_deserialize(
                    object_ids_to_fetch,
                    max([
                        ray._config.get_timeout_milliseconds(),
                        int(0.01 * len(unready_ids)),
                    ]),
                )
                # Remove any entries for objects we received during this
                # iteration so we don't retrieve the same object twice.
                for i, val in enumerate(results):
                    if val is not plasma.ObjectNotAvailable:
                        object_id = object_ids_to_fetch[i].binary()
                        index = unready_ids[object_id]
                        final_results[index] = val
                        unready_ids.pop(object_id)

            # If there were objects that we weren't able to get locally,
            # let the raylet know that we're now unblocked.
            self.raylet_client.notify_unblocked(self.current_task_id)

        assert len(final_results) == len(object_ids)
        return final_results

    def submit_task(self,
                    function_descriptor,
                    args,
                    actor_id=None,
                    actor_handle_id=None,
                    actor_counter=0,
                    actor_creation_id=None,
                    actor_creation_dummy_object_id=None,
                    max_actor_reconstructions=0,
                    execution_dependencies=None,
                    new_actor_handles=None,
                    num_return_vals=None,
                    resources=None,
                    placement_resources=None,
                    job_id=None):
        """Submit a remote task to the scheduler.

        Tell the scheduler to schedule the execution of the function with
        function_descriptor with arguments args. Retrieve object IDs for the
        outputs of the function from the scheduler and immediately return them.

        Args:
            function_descriptor: The function descriptor to execute.
            args: The arguments to pass into the function. Arguments can be
                object IDs or they can be values. If they are values, they must
                be serializable objects.
            actor_id: The ID of the actor that this task is for.
            actor_counter: The counter of the actor task.
            actor_creation_id: The ID of the actor to create, if this is an
                actor creation task.
            actor_creation_dummy_object_id: If this task is an actor method,
                then this argument is the dummy object ID associated with the
                actor creation task for the corresponding actor.
            execution_dependencies: The execution dependencies for this task.
            num_return_vals: The number of return values this function should
                have.
            resources: The resource requirements for this task.
            placement_resources: The resources required for placing the task.
                If this is not provided or if it is an empty dictionary, then
                the placement resources will be equal to resources.
            job_id: The ID of the relevant job. This is almost always the
                job ID of the job that is currently running. However, in
                the exceptional case that an actor task is being dispatched to
                an actor created by a different job, this should be the
                job ID of the job that created the actor.

        Returns:
            The return object IDs for this task.
        """
        with profiling.profile("submit_task"):
            if actor_id is None:
                assert actor_handle_id is None
                actor_id = ActorID.nil()
                actor_handle_id = ActorHandleID.nil()
            else:
                assert actor_handle_id is not None

            if actor_creation_id is None:
                actor_creation_id = ActorID.nil()

            if actor_creation_dummy_object_id is None:
                actor_creation_dummy_object_id = ObjectID.nil()

            # Put large or complex arguments that are passed by value in the
            # object store first.
            args_for_raylet = []
            for arg in args:
                if isinstance(arg, ObjectID):
                    args_for_raylet.append(arg)
                elif ray._raylet.check_simple_value(arg):
                    args_for_raylet.append(arg)
                else:
                    args_for_raylet.append(put(arg))

            # By default, there are no execution dependencies.
            if execution_dependencies is None:
                execution_dependencies = []

            if new_actor_handles is None:
                new_actor_handles = []

            if job_id is None:
                job_id = self.current_job_id

            if resources is None:
                raise ValueError("The resources dictionary is required.")
            for value in resources.values():
                assert (isinstance(value, int) or isinstance(value, float))
                if value < 0:
                    raise ValueError(
                        "Resource quantities must be nonnegative.")
                if (value >= 1 and isinstance(value, float)
                        and not value.is_integer()):
                    raise ValueError(
                        "Resource quantities must all be whole numbers.")

            # Remove any resources with zero quantity requirements
            resources = {
                resource_label: resource_quantity
                for resource_label, resource_quantity in resources.items()
                if resource_quantity > 0
            }

            if placement_resources is None:
                placement_resources = {}

            # Increment the worker's task index to track how many tasks
            # have been submitted by the current task so far.
            self.task_context.task_index += 1
            # The parent task must be set for the submitted task.
            assert not self.current_task_id.is_nil()
            # Current driver id must not be nil when submitting a task.
            # Because every task must belong to a driver.
            assert not self.current_job_id.is_nil()
            # Submit the task to raylet.
            function_descriptor_list = (
                function_descriptor.get_function_descriptor_list())
            assert isinstance(job_id, JobID)
            task = ray._raylet.Task(
                job_id,
                function_descriptor_list,
                args_for_raylet,
                num_return_vals,
                self.current_task_id,
                self.task_context.task_index,
                actor_creation_id,
                actor_creation_dummy_object_id,
                max_actor_reconstructions,
                actor_id,
                actor_handle_id,
                actor_counter,
                new_actor_handles,
                execution_dependencies,
                resources,
                placement_resources,
            )
            self.raylet_client.submit_task(task)

            return task.returns()

    def run_function_on_all_workers(self, function,
                                    run_on_other_drivers=False):
        """Run arbitrary code on all of the workers.

        This function will first be run on the driver, and then it will be
        exported to all of the workers to be run. It will also be run on any
        new workers that register later. If ray.init has not been called yet,
        then cache the function and export it later.

        Args:
            function (Callable): The function to run on all of the workers. It
                takes only one argument, a worker info dict. If it returns
                anything, its return values will not be used.
            run_on_other_drivers: The boolean that indicates whether we want to
                run this function on other drivers. One case is we may need to
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

            check_oversized_pickle(pickled_function, function.__name__,
                                   "function", self)

            # Run the function on all workers.
            self.redis_client.hmset(
                key, {
                    "job_id": self.current_job_id.binary(),
                    "function_id": function_to_run_id,
                    "function": pickled_function,
                    "run_on_other_drivers": str(run_on_other_drivers)
                })
            self.redis_client.rpush("Exports", key)
            # TODO(rkn): If the worker fails after it calls setnx and before it
            # successfully completes the hmset and rpush, then the program will
            # most likely hang. This could be fixed by making these three
            # operations into a transaction (or by implementing a custom
            # command that does all three things).

    def _get_arguments_for_execution(self, function_name, serialized_args):
        """Retrieve the arguments for the remote function.

        This retrieves the values for the arguments to the remote function that
        were passed in as object IDs. Arguments that were passed by value are
        not changed. This is called by the worker that is executing the remote
        function.

        Args:
            function_name (str): The name of the remote function whose
                arguments are being retrieved.
            serialized_args (List): The arguments to the function. These are
                either strings representing serialized objects passed by value
                or they are ray.ObjectIDs.

        Returns:
            The retrieved arguments in addition to the arguments that were
                passed by value.

        Raises:
            RayError: This exception is raised if a task that
                created one of the arguments failed.
        """
        arguments = [None] * len(serialized_args)
        object_ids = []
        object_indices = []

        for (i, arg) in enumerate(serialized_args):
            if isinstance(arg, ObjectID):
                object_ids.append(arg)
                object_indices.append(i)
            else:
                # pass the argument by value
                arguments[i] = arg

        # Get the objects from the local object store.
        if len(object_ids) > 0:
            values = self.get_object(object_ids)
            for i, value in enumerate(values):
                if isinstance(value, RayError):
                    raise value
                else:
                    arguments[object_indices[i]] = value

        return arguments

    def _store_outputs_in_object_store(self, object_ids, outputs):
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
            if outputs[i] is ray.experimental.no_return.NoReturn:
                if not self.plasma_client.contains(
                        pyarrow.plasma.ObjectID(object_ids[i].binary())):
                    raise RuntimeError(
                        "Attempting to return 'ray.experimental.NoReturn' "
                        "from a remote function, but the corresponding "
                        "ObjectID does not exist in the local object store.")
            else:
                self.put_object(object_ids[i], outputs[i])

    def _process_task(self, task, function_execution_info):
        """Execute a task assigned to this worker.

        This method deserializes a task from the scheduler, and attempts to
        execute the task. If the task succeeds, the outputs are stored in the
        local object store. If the task throws an exception, RayTaskError
        objects are stored in the object store to represent the failed task
        (these will be retrieved by calls to get or by subsequent tasks that
        use the outputs of this task).
        """
        assert self.current_task_id.is_nil()
        assert self.task_context.task_index == 0
        assert self.task_context.put_index == 1
        if task.actor_id().is_nil():
            # If this worker is not an actor, check that `current_job_id`
            # was reset when the worker finished the previous task.
            assert self.current_job_id.is_nil()
            # Set the driver ID of the current running task. This is
            # needed so that if the task throws an exception, we propagate
            # the error message to the correct driver.
            self.current_job_id = task.job_id()
        else:
            # If this worker is an actor, current_job_id wasn't reset.
            # Check that current task's driver ID equals the previous one.
            assert self.current_job_id == task.job_id()

        self.task_context.current_task_id = task.task_id()

        function_descriptor = FunctionDescriptor.from_bytes_list(
            task.function_descriptor_list())
        args = task.arguments()
        return_object_ids = task.returns()
        if (not task.actor_id().is_nil()
                or not task.actor_creation_id().is_nil()):
            dummy_return_id = return_object_ids.pop()
        function_executor = function_execution_info.function
        function_name = function_execution_info.function_name

        # Get task arguments from the object store.
        try:
            if function_name != "__ray_terminate__":
                self.reraise_actor_init_error()
            self.memory_monitor.raise_if_low_memory()
            with profiling.profile("task:deserialize_arguments"):
                arguments = self._get_arguments_for_execution(
                    function_name, args)
        except Exception as e:
            self._handle_process_task_failure(
                function_descriptor, return_object_ids, e,
                ray.utils.format_error_message(traceback.format_exc()))
            return

        # Execute the task.
        try:
            self._current_task = task
            with profiling.profile("task:execute"):
                if (task.actor_id().is_nil()
                        and task.actor_creation_id().is_nil()):
                    outputs = function_executor(*arguments)
                else:
                    if not task.actor_id().is_nil():
                        key = task.actor_id()
                    else:
                        key = task.actor_creation_id()
                    outputs = function_executor(dummy_return_id,
                                                self.actors[key], *arguments)
        except Exception as e:
            # Determine whether the exception occured during a task, not an
            # actor method.
            task_exception = task.actor_id().is_nil()
            traceback_str = ray.utils.format_error_message(
                traceback.format_exc(), task_exception=task_exception)
            self._handle_process_task_failure(
                function_descriptor, return_object_ids, e, traceback_str)
            return
        finally:
            self._current_task = None

        # Store the outputs in the local object store.
        try:
            with profiling.profile("task:store_outputs"):
                # If this is an actor task, then the last object ID returned by
                # the task is a dummy output, not returned by the function
                # itself. Decrement to get the correct number of return values.
                num_returns = len(return_object_ids)
                if num_returns == 1:
                    outputs = (outputs, )
                self._store_outputs_in_object_store(return_object_ids, outputs)
        except Exception as e:
            self._handle_process_task_failure(
                function_descriptor, return_object_ids, e,
                ray.utils.format_error_message(traceback.format_exc()))

    def _handle_process_task_failure(self, function_descriptor,
                                     return_object_ids, error, backtrace):
        function_name = function_descriptor.function_name
        failure_object = RayTaskError(function_name, backtrace)
        failure_objects = [
            failure_object for _ in range(len(return_object_ids))
        ]
        self._store_outputs_in_object_store(return_object_ids, failure_objects)
        # Log the error message.
        ray.utils.push_error_to_driver(
            self,
            ray_constants.TASK_PUSH_ERROR,
            str(failure_object),
            job_id=self.current_job_id)
        # Mark the actor init as failed
        if not self.actor_id.is_nil() and function_name == "__init__":
            self.mark_actor_init_failed(error)
        # Send signal with the error.
        ray_signal.send(ray_signal.ErrorSignal(str(failure_object)))

    def _wait_for_and_process_task(self, task):
        """Wait for a task to be ready and process the task.

        Args:
            task: The task to execute.
        """
        function_descriptor = FunctionDescriptor.from_bytes_list(
            task.function_descriptor_list())
        job_id = task.job_id()

        # TODO(rkn): It would be preferable for actor creation tasks to share
        # more of the code path with regular task execution.
        if not task.actor_creation_id().is_nil():
            assert self.actor_id.is_nil()
            self.actor_id = task.actor_creation_id()
            self.actor_creation_task_id = task.task_id()
            actor_class = self.function_actor_manager.load_actor_class(
                job_id, function_descriptor)
            self.actors[self.actor_id] = actor_class.__new__(actor_class)
            self.actor_checkpoint_info[self.actor_id] = ActorCheckpointInfo(
                num_tasks_since_last_checkpoint=0,
                last_checkpoint_timestamp=int(1000 * time.time()),
                checkpoint_ids=[],
            )

        execution_info = self.function_actor_manager.get_execution_info(
            job_id, function_descriptor)

        # Execute the task.
        function_name = execution_info.function_name
        extra_data = {"name": function_name, "task_id": task.task_id().hex()}
        if task.actor_id().is_nil():
            if task.actor_creation_id().is_nil():
                title = "ray_worker:{}()".format(function_name)
                next_title = "ray_worker"
            else:
                actor = self.actors[task.actor_creation_id()]
                title = "ray_{}:{}()".format(actor.__class__.__name__,
                                             function_name)
                next_title = "ray_{}".format(actor.__class__.__name__)
        else:
            actor = self.actors[task.actor_id()]
            title = "ray_{}:{}()".format(actor.__class__.__name__,
                                         function_name)
            next_title = "ray_{}".format(actor.__class__.__name__)
        with profiling.profile("task", extra_data=extra_data):
            with _changeproctitle(title, next_title):
                self._process_task(task, execution_info)
            # Reset the state fields so the next task can run.
            self.task_context.current_task_id = TaskID.nil()
            self.task_context.task_index = 0
            self.task_context.put_index = 1
            if self.actor_id.is_nil():
                # Don't need to reset `current_job_id` if the worker is an
                # actor. Because the following tasks should all have the
                # same driver id.
                self.current_job_id = WorkerID.nil()
                # Reset signal counters so that the next task can get
                # all past signals.
                ray_signal.reset()

        # Increase the task execution counter.
        self.function_actor_manager.increase_task_counter(
            job_id, function_descriptor)

        reached_max_executions = (self.function_actor_manager.get_task_counter(
            job_id, function_descriptor) == execution_info.max_calls)
        if reached_max_executions:
            self.raylet_client.disconnect()
            sys.exit(0)

    def _get_next_task_from_raylet(self):
        """Get the next task from the raylet.

        Returns:
            A task from the raylet.
        """
        with profiling.profile("worker_idle"):
            task = self.raylet_client.get_task()

        # Automatically restrict the GPUs available to this task.
        ray.utils.set_cuda_visible_devices(ray.get_gpu_ids())

        return task

    def main_loop(self):
        """The main loop a worker runs to receive and execute tasks."""

        def exit(signum, frame):
            shutdown()
            sys.exit(0)

        signal.signal(signal.SIGTERM, exit)

        while True:
            task = self._get_next_task_from_raylet()
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
    if _mode() == LOCAL_MODE:
        raise Exception("ray.get_gpu_ids() currently does not work in LOCAL "
                        "MODE.")

    all_resource_ids = global_worker.raylet_client.resource_ids()
    assigned_ids = [
        resource_id for resource_id, _ in all_resource_ids.get("GPU", [])
    ]
    # If the user had already set CUDA_VISIBLE_DEVICES, then respect that (in
    # the sense that only GPU IDs that appear in CUDA_VISIBLE_DEVICES should be
    # returned).
    if global_worker.original_gpu_ids is not None:
        assigned_ids = [
            global_worker.original_gpu_ids[gpu_id] for gpu_id in assigned_ids
        ]

    return assigned_ids


def get_resource_ids():
    """Get the IDs of the resources that are available to the worker.

    Returns:
        A dictionary mapping the name of a resource to a list of pairs, where
        each pair consists of the ID of a resource and the fraction of that
        resource reserved for this worker.
    """
    if _mode() == LOCAL_MODE:
        raise Exception(
            "ray.get_resource_ids() currently does not work in LOCAL "
            "MODE.")

    return global_worker.raylet_client.resource_ids()


def get_webui_url():
    """Get the URL to access the web UI.

    Note that the URL does not specify which node the web UI is on.

    Returns:
        The URL of the web UI as a string.
    """
    if _global_node is None:
        raise Exception("Ray has not been initialized/connected.")
    return _global_node.webui_url


global_worker = Worker()
"""Worker: The global Worker object for this worker process.

We use a global Worker object to ensure that there is a single worker object
per worker process.
"""

_global_node = None
"""ray.node.Node: The global node object that is created by ray.init()."""


class RayConnectionError(Exception):
    pass


def print_failed_task(task_status):
    """Print information about failed tasks.

    Args:
        task_status (Dict): A dictionary containing the name, operationid, and
            error message for a failed task.
    """
    logger.error("""
      Error: Task failed
        Function Name: {}
        Task ID: {}
        Error Message: \n{}
    """.format(task_status["function_name"], task_status["operationid"],
               task_status["error_message"]))


def _initialize_serialization(job_id, worker=global_worker):
    """Initialize the serialization library.

    This defines a custom serializer for object IDs and also tells ray to
    serialize several exception classes that we define for error handling.
    """
    serialization_context = pyarrow.default_serialization_context()
    # Tell the serialization context to use the cloudpickle version that we
    # ship with Ray.
    serialization_context.set_pickle(pickle.dumps, pickle.loads)
    pyarrow.register_torch_serialization_handlers(serialization_context)

    for id_type in ray._raylet._ID_TYPES:
        serialization_context.register_type(
            id_type,
            "{}.{}".format(id_type.__module__, id_type.__name__),
            pickle=True)

    def actor_handle_serializer(obj):
        return obj._serialization_helper(True)

    def actor_handle_deserializer(serialized_obj):
        new_handle = ray.actor.ActorHandle.__new__(ray.actor.ActorHandle)
        new_handle._deserialization_helper(serialized_obj, True)
        return new_handle

    # We register this serializer on each worker instead of calling
    # register_custom_serializer from the driver so that isinstance still
    # works.
    serialization_context.register_type(
        ray.actor.ActorHandle,
        "ray.ActorHandle",
        pickle=False,
        custom_serializer=actor_handle_serializer,
        custom_deserializer=actor_handle_deserializer)

    worker.serialization_context_map[job_id] = serialization_context

    # Register exception types.
    for error_cls in RAY_EXCEPTION_TYPES:
        register_custom_serializer(
            error_cls,
            use_dict=True,
            local=True,
            job_id=job_id,
            class_id=error_cls.__module__ + ". " + error_cls.__name__,
        )
    # Tell Ray to serialize lambdas with pickle.
    register_custom_serializer(
        type(lambda: 0),
        use_pickle=True,
        local=True,
        job_id=job_id,
        class_id="lambda")
    # Tell Ray to serialize types with pickle.
    register_custom_serializer(
        type(int), use_pickle=True, local=True, job_id=job_id, class_id="type")
    # Tell Ray to serialize FunctionSignatures as dictionaries. This is
    # used when passing around actor handles.
    register_custom_serializer(
        ray.signature.FunctionSignature,
        use_dict=True,
        local=True,
        job_id=job_id,
        class_id="ray.signature.FunctionSignature")


def init(redis_address=None,
         num_cpus=None,
         num_gpus=None,
         resources=None,
         object_store_memory=None,
         redis_max_memory=None,
         log_to_driver=True,
         node_ip_address=None,
         object_id_seed=None,
         local_mode=False,
         redirect_worker_output=None,
         redirect_output=None,
         ignore_reinit_error=False,
         num_redis_shards=None,
         redis_max_clients=None,
         redis_password=None,
         plasma_directory=None,
         huge_pages=False,
         include_webui=False,
         job_id=None,
         configure_logging=True,
         logging_level=logging.INFO,
         logging_format=ray_constants.LOGGER_FORMAT,
         plasma_store_socket_name=None,
         raylet_socket_name=None,
         temp_dir=None,
         load_code_from_local=False,
         _internal_config=None):
    """Connect to an existing Ray cluster or start one and connect to it.

    This method handles two cases. Either a Ray cluster already exists and we
    just attach this driver to it, or we start all of the processes associated
    with a Ray cluster and attach to the newly started cluster.

    To start Ray and all of the relevant processes, use this as follows:

    .. code-block:: python

        ray.init()

    To connect to an existing Ray cluster, use this as follows (substituting
    in the appropriate address):

    .. code-block:: python

        ray.init(redis_address="123.45.67.89:6379")

    Args:
        redis_address (str): The address of the Redis server to connect to. If
            this address is not provided, then this command will start Redis, a
            raylet, a plasma store, a plasma manager, and some workers.
            It will also kill these processes when Python exits.
        num_cpus (int): Number of cpus the user wishes all raylets to
            be configured with.
        num_gpus (int): Number of gpus the user wishes all raylets to
            be configured with.
        resources: A dictionary mapping the name of a resource to the quantity
            of that resource available.
        object_store_memory: The amount of memory (in bytes) to start the
            object store with. By default, this is capped at 20GB but can be
            set higher.
        redis_max_memory: The max amount of memory (in bytes) to allow each
            redis shard to use. Once the limit is exceeded, redis will start
            LRU eviction of entries. This only applies to the sharded redis
            tables (task, object, and profile tables). By default, this is
            capped at 10GB but can be set higher.
        log_to_driver (bool): If true, then output from all of the worker
            processes on all nodes will be directed to the driver.
        node_ip_address (str): The IP address of the node that we are on.
        object_id_seed (int): Used to seed the deterministic generation of
            object IDs. The same value can be used across multiple runs of the
            same driver in order to generate the object IDs in a consistent
            manner. However, the same ID should not be used for different
            drivers.
        local_mode (bool): True if the code should be executed serially
            without Ray. This is useful for debugging.
        ignore_reinit_error: True if we should suppress errors from calling
            ray.init() a second time.
        num_redis_shards: The number of Redis shards to start in addition to
            the primary Redis shard.
        redis_max_clients: If provided, attempt to configure Redis with this
            maxclients number.
        redis_password (str): Prevents external clients without the password
            from connecting to Redis if provided.
        plasma_directory: A directory where the Plasma memory mapped files will
            be created.
        huge_pages: Boolean flag indicating whether to start the Object
            Store with hugetlbfs support. Requires plasma_directory.
        include_webui: Boolean flag indicating whether to start the web
            UI, which displays the status of the Ray cluster.
        job_id: The ID of this job.
        configure_logging: True if allow the logging cofiguration here.
            Otherwise, the users may want to configure it by their own.
        logging_level: Logging level, default will be logging.INFO.
        logging_format: Logging format, default contains a timestamp,
            filename, line number, and message. See ray_constants.py.
        plasma_store_socket_name (str): If provided, it will specify the socket
            name used by the plasma store.
        raylet_socket_name (str): If provided, it will specify the socket path
            used by the raylet process.
        temp_dir (str): If provided, it will specify the root temporary
            directory for the Ray process.
        load_code_from_local: Whether code should be loaded from a local module
            or from the GCS.
        _internal_config (str): JSON configuration for overriding
            RayConfig defaults. For testing purposes ONLY.

    Returns:
        Address information about the started processes.

    Raises:
        Exception: An exception is raised if an inappropriate combination of
            arguments is passed in.
    """

    if configure_logging:
        setup_logger(logging_level, logging_format)

    if local_mode:
        driver_mode = LOCAL_MODE
    else:
        driver_mode = SCRIPT_MODE

    if setproctitle is None:
        logger.warning(
            "WARNING: Not updating worker name since `setproctitle` is not "
            "installed. Install this with `pip install setproctitle` "
            "(or ray[debug]) to enable monitoring of worker processes.")

    if global_worker.connected:
        if ignore_reinit_error:
            logger.error("Calling ray.init() again after it has already been "
                         "called.")
            return
        else:
            raise Exception("Perhaps you called ray.init twice by accident? "
                            "This error can be suppressed by passing in "
                            "'ignore_reinit_error=True' or by calling "
                            "'ray.shutdown()' prior to 'ray.init()'.")

    # Convert hostnames to numerical IP address.
    if node_ip_address is not None:
        node_ip_address = services.address_to_ip(node_ip_address)
    if redis_address is not None:
        redis_address = services.address_to_ip(redis_address)

    global _global_node
    if driver_mode == LOCAL_MODE:
        # If starting Ray in LOCAL_MODE, don't start any other processes.
        _global_node = ray.node.LocalNode()
    elif redis_address is None:
        # In this case, we need to start a new cluster.
        ray_params = ray.parameter.RayParams(
            redis_address=redis_address,
            node_ip_address=node_ip_address,
            object_id_seed=object_id_seed,
            local_mode=local_mode,
            driver_mode=driver_mode,
            redirect_worker_output=redirect_worker_output,
            redirect_output=redirect_output,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            resources=resources,
            num_redis_shards=num_redis_shards,
            redis_max_clients=redis_max_clients,
            redis_password=redis_password,
            plasma_directory=plasma_directory,
            huge_pages=huge_pages,
            include_webui=include_webui,
            object_store_memory=object_store_memory,
            redis_max_memory=redis_max_memory,
            plasma_store_socket_name=plasma_store_socket_name,
            raylet_socket_name=raylet_socket_name,
            temp_dir=temp_dir,
            load_code_from_local=load_code_from_local,
            _internal_config=_internal_config,
        )
        # Start the Ray processes. We set shutdown_at_exit=False because we
        # shutdown the node in the ray.shutdown call that happens in the atexit
        # handler.
        _global_node = ray.node.Node(
            head=True, shutdown_at_exit=False, ray_params=ray_params)
    else:
        # In this case, we are connecting to an existing cluster.
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
        if redis_max_memory is not None:
            raise Exception("When connecting to an existing cluster, "
                            "redis_max_memory must not be provided.")
        if plasma_directory is not None:
            raise Exception("When connecting to an existing cluster, "
                            "plasma_directory must not be provided.")
        if huge_pages:
            raise Exception("When connecting to an existing cluster, "
                            "huge_pages must not be provided.")
        if temp_dir is not None:
            raise Exception("When connecting to an existing cluster, "
                            "temp_dir must not be provided.")
        if plasma_store_socket_name is not None:
            raise Exception("When connecting to an existing cluster, "
                            "plasma_store_socket_name must not be provided.")
        if raylet_socket_name is not None:
            raise Exception("When connecting to an existing cluster, "
                            "raylet_socket_name must not be provided.")
        if _internal_config is not None:
            raise Exception("When connecting to an existing cluster, "
                            "_internal_config must not be provided.")

        # In this case, we only need to connect the node.
        ray_params = ray.parameter.RayParams(
            node_ip_address=node_ip_address,
            redis_address=redis_address,
            redis_password=redis_password,
            object_id_seed=object_id_seed,
            temp_dir=temp_dir,
            load_code_from_local=load_code_from_local)
        _global_node = ray.node.Node(
            ray_params, head=False, shutdown_at_exit=False, connect_only=True)

    connect(
        _global_node,
        mode=driver_mode,
        log_to_driver=log_to_driver,
        worker=global_worker,
        job_id=job_id)

    for hook in _post_init_hooks:
        hook()

    return _global_node.address_info


# Functions to run as callback after a successful ray init.
_post_init_hooks = []


def shutdown(exiting_interpreter=False):
    """Disconnect the worker, and terminate processes started by ray.init().

    This will automatically run at the end when a Python process that uses Ray
    exits. It is ok to run this twice in a row. The primary use case for this
    function is to cleanup state between tests.

    Note that this will clear any remote function definitions, actor
    definitions, and existing actors, so if you wish to use any previously
    defined remote functions or actors after calling ray.shutdown(), then you
    need to redefine them. If they were defined in an imported module, then you
    will need to reload the module.

    Args:
        exiting_interpreter (bool): True if this is called by the atexit hook
            and false otherwise. If we are exiting the interpreter, we will
            wait a little while to print any extra error messages.
    """
    if exiting_interpreter and global_worker.mode == SCRIPT_MODE:
        # This is a duration to sleep before shutting down everything in order
        # to make sure that log messages finish printing.
        time.sleep(0.5)

    disconnect()

    # Disconnect global state from GCS.
    ray.state.state.disconnect()

    # Shut down the Ray processes.
    global _global_node
    if _global_node is not None:
        _global_node.kill_all_processes(check_alive=False, allow_graceful=True)
        _global_node = None

    # TODO(rkn): Instead of manually reseting some of the worker fields, we
    # should simply set "global_worker" to equal "None" or something like that.
    global_worker.set_mode(None)
    global_worker._post_get_hooks = []


atexit.register(shutdown, True)

# Define a custom excepthook so that if the driver exits with an exception, we
# can push that exception to Redis.
normal_excepthook = sys.excepthook


def custom_excepthook(type, value, tb):
    # If this is a driver, push the exception to redis.
    if global_worker.mode == SCRIPT_MODE:
        error_message = "".join(traceback.format_tb(tb))
        global_worker.redis_client.hmset(b"Drivers:" + global_worker.worker_id,
                                         {"exception": error_message})
    # Call the normal excepthook.
    normal_excepthook(type, value, tb)


sys.excepthook = custom_excepthook

# The last time we raised a TaskError in this process. We use this value to
# suppress redundant error messages pushed from the workers.
last_task_error_raise_time = 0

# The max amount of seconds to wait before printing out an uncaught error.
UNCAUGHT_ERROR_GRACE_PERIOD = 5


def print_logs(redis_client, threads_stopped):
    """Prints log messages from workers on all of the nodes.

    Args:
        redis_client: A client to the primary Redis shard.
        threads_stopped (threading.Event): A threading event used to signal to
            the thread that it should exit.
    """
    pubsub_client = redis_client.pubsub(ignore_subscribe_messages=True)
    pubsub_client.subscribe(ray.gcs_utils.LOG_FILE_CHANNEL)
    localhost = services.get_node_ip_address()
    try:
        # Keep track of the number of consecutive log messages that have been
        # received with no break in between. If this number grows continually,
        # then the worker is probably not able to process the log messages as
        # rapidly as they are coming in.
        num_consecutive_messages_received = 0
        while True:
            # Exit if we received a signal that we should stop.
            if threads_stopped.is_set():
                return

            msg = pubsub_client.get_message()
            if msg is None:
                num_consecutive_messages_received = 0
                threads_stopped.wait(timeout=0.01)
                continue
            num_consecutive_messages_received += 1

            data = json.loads(ray.utils.decode(msg["data"]))
            if data["ip"] == localhost:
                for line in data["lines"]:
                    print("{}{}(pid={}){} {}".format(
                        colorama.Style.DIM, colorama.Fore.CYAN, data["pid"],
                        colorama.Style.RESET_ALL, line))
            else:
                for line in data["lines"]:
                    print("{}{}(pid={}, ip={}){} {}".format(
                        colorama.Style.DIM, colorama.Fore.CYAN, data["pid"],
                        data["ip"], colorama.Style.RESET_ALL, line))

            if (num_consecutive_messages_received % 100 == 0
                    and num_consecutive_messages_received > 0):
                logger.warning(
                    "The driver may not be able to keep up with the "
                    "stdout/stderr of the workers. To avoid forwarding logs "
                    "to the driver, use 'ray.init(log_to_driver=False)'.")
    finally:
        # Close the pubsub client to avoid leaking file descriptors.
        pubsub_client.close()


def print_error_messages_raylet(task_error_queue, threads_stopped):
    """Prints message received in the given output queue.

    This checks periodically if any un-raised errors occured in the background.

    Args:
        task_error_queue (queue.Queue): A queue used to receive errors from the
            thread that listens to Redis.
        threads_stopped (threading.Event): A threading event used to signal to
            the thread that it should exit.
    """

    while True:
        # Exit if we received a signal that we should stop.
        if threads_stopped.is_set():
            return

        try:
            error, t = task_error_queue.get(block=False)
        except queue.Empty:
            threads_stopped.wait(timeout=0.01)
            continue
        # Delay errors a little bit of time to attempt to suppress redundant
        # messages originating from the worker.
        while t + UNCAUGHT_ERROR_GRACE_PERIOD > time.time():
            threads_stopped.wait(timeout=1)
            if threads_stopped.is_set():
                break
        if t < last_task_error_raise_time + UNCAUGHT_ERROR_GRACE_PERIOD:
            logger.debug("Suppressing error from worker: {}".format(error))
        else:
            logger.error(
                "Possible unhandled error from worker: {}".format(error))


def listen_error_messages_raylet(worker, task_error_queue, threads_stopped):
    """Listen to error messages in the background on the driver.

    This runs in a separate thread on the driver and pushes (error, time)
    tuples to the output queue.

    Args:
        worker: The worker class that this thread belongs to.
        task_error_queue (queue.Queue): A queue used to communicate with the
            thread that prints the errors found by this thread.
        threads_stopped (threading.Event): A threading event used to signal to
            the thread that it should exit.
    """
    worker.error_message_pubsub_client = worker.redis_client.pubsub(
        ignore_subscribe_messages=True)
    # Exports that are published after the call to
    # error_message_pubsub_client.subscribe and before the call to
    # error_message_pubsub_client.listen will still be processed in the loop.

    # Really we should just subscribe to the errors for this specific job.
    # However, currently all errors seem to be published on the same channel.
    error_pubsub_channel = str(
        ray.gcs_utils.TablePubsub.Value("ERROR_INFO_PUBSUB")).encode("ascii")
    worker.error_message_pubsub_client.subscribe(error_pubsub_channel)
    # worker.error_message_pubsub_client.psubscribe("*")

    try:
        # Get the exports that occurred before the call to subscribe.
        error_messages = ray.errors(include_cluster_errors=False)
        for error_message in error_messages:
            logger.error(error_message)

        while True:
            # Exit if we received a signal that we should stop.
            if threads_stopped.is_set():
                return

            msg = worker.error_message_pubsub_client.get_message()
            if msg is None:
                threads_stopped.wait(timeout=0.01)
                continue
            gcs_entry = ray.gcs_utils.GcsEntry.FromString(msg["data"])
            assert len(gcs_entry.entries) == 1
            error_data = ray.gcs_utils.ErrorTableData.FromString(
                gcs_entry.entries[0])
            job_id = error_data.job_id
            if job_id not in [
                    worker.current_job_id.binary(),
                    JobID.nil().binary()
            ]:
                continue

            error_message = error_data.error_message
            if (error_data.type == ray_constants.TASK_PUSH_ERROR):
                # Delay it a bit to see if we can suppress it
                task_error_queue.put((error_message, time.time()))
            else:
                logger.error(error_message)
    finally:
        # Close the pubsub client to avoid leaking file descriptors.
        worker.error_message_pubsub_client.close()


def is_initialized():
    """Check if ray.init has been called yet.

    Returns:
        True if ray.init has already been called and false otherwise.
    """
    return ray.worker.global_worker.connected


def connect(node,
            mode=WORKER_MODE,
            log_to_driver=False,
            worker=global_worker,
            job_id=None):
    """Connect this worker to the raylet, to Plasma, and to Redis.

    Args:
        node (ray.node.Node): The node to connect.
        mode: The mode of the worker. One of SCRIPT_MODE, WORKER_MODE, and
            LOCAL_MODE.
        log_to_driver (bool): If true, then output from all of the worker
            processes on all nodes will be directed to the driver.
        worker: The ray.Worker instance.
        job_id: The ID of job. If it's None, then we will generate one.
    """
    # Do some basic checking to make sure we didn't call ray.init twice.
    error_message = "Perhaps you called ray.init twice by accident?"
    assert not worker.connected, error_message
    assert worker.cached_functions_to_run is not None, error_message

    # Enable nice stack traces on SIGSEGV etc.
    if not faulthandler.is_enabled():
        faulthandler.enable(all_threads=False)

    worker.profiler = profiling.Profiler(worker, worker.threads_stopped)

    # Initialize some fields.
    if mode is WORKER_MODE:
        worker.worker_id = _random_string()
        if setproctitle:
            setproctitle.setproctitle("ray_worker")
    else:
        # This is the code path of driver mode.
        if job_id is None:
            job_id = JobID.from_random()

        if not isinstance(job_id, JobID):
            raise TypeError("The type of given job id must be JobID.")

        worker.worker_id = job_id.binary()

    # When tasks are executed on remote workers in the context of multiple
    # drivers, the current job ID is used to keep track of which driver is
    # responsible for the task so that error messages will be propagated to
    # the correct driver.
    if mode != WORKER_MODE:
        worker.current_job_id = JobID(worker.worker_id)

    # All workers start out as non-actors. A worker can be turned into an actor
    # after it is created.
    worker.actor_id = ActorID.nil()
    worker.node = node
    worker.set_mode(mode)

    # If running Ray in LOCAL_MODE, there is no need to create call
    # create_worker or to start the worker service.
    if mode == LOCAL_MODE:
        worker.local_mode_manager = LocalModeManager()
        return

    # Create a Redis client.
    # The Redis client can safely be shared between threads. However, that is
    # not true of Redis pubsub clients. See the documentation at
    # https://github.com/andymccurdy/redis-py#thread-safety.
    worker.redis_client = node.create_redis_client()

    # For driver's check that the version information matches the version
    # information that the Ray cluster was started with.
    try:
        ray.services.check_version_info(worker.redis_client)
    except Exception as e:
        if mode == SCRIPT_MODE:
            raise e
        elif mode == WORKER_MODE:
            traceback_str = traceback.format_exc()
            ray.utils.push_error_to_driver_through_redis(
                worker.redis_client,
                ray_constants.VERSION_MISMATCH_PUSH_ERROR,
                traceback_str,
                job_id=None)

    worker.lock = threading.RLock()

    # Create an object for interfacing with the global state.
    ray.state.state._initialize_global_state(
        node.redis_address, redis_password=node.redis_password)

    # Register the worker with Redis.
    if mode == SCRIPT_MODE:
        # The concept of a driver is the same as the concept of a "job".
        # Register the driver/job with Redis here.
        import __main__ as main
        driver_info = {
            "node_ip_address": node.node_ip_address,
            "driver_id": worker.worker_id,
            "start_time": time.time(),
            "plasma_store_socket": node.plasma_store_socket_name,
            "raylet_socket": node.raylet_socket_name,
            "name": (main.__file__
                     if hasattr(main, "__file__") else "INTERACTIVE MODE")
        }
        worker.redis_client.hmset(b"Drivers:" + worker.worker_id, driver_info)
    elif mode == WORKER_MODE:
        # Register the worker with Redis.
        worker_dict = {
            "node_ip_address": node.node_ip_address,
            "plasma_store_socket": node.plasma_store_socket_name,
        }
        # Check the RedirectOutput key in Redis and based on its value redirect
        # worker output and error to their own files.
        # This key is set in services.py when Redis is started.
        redirect_worker_output_val = worker.redis_client.get("RedirectOutput")
        if (redirect_worker_output_val is not None
                and int(redirect_worker_output_val) == 1):
            log_stdout_file, log_stderr_file = (
                node.new_worker_redirected_log_file(worker.worker_id))
            # Redirect stdout/stderr at the file descriptor level. If we simply
            # set sys.stdout and sys.stderr, then logging from C++ can fail to
            # be redirected.
            os.dup2(log_stdout_file.fileno(), sys.stdout.fileno())
            os.dup2(log_stderr_file.fileno(), sys.stderr.fileno())
            # We also manually set sys.stdout and sys.stderr because that seems
            # to have an affect on the output buffering. Without doing this,
            # stdout and stderr are heavily buffered resulting in seemingly
            # lost logging statements.
            sys.stdout = log_stdout_file
            sys.stderr = log_stderr_file
            # This should always be the first message to appear in the worker's
            # stdout and stderr log files. The string "Ray worker pid:" is
            # parsed in the log monitor process.
            print("Ray worker pid: {}".format(os.getpid()))
            print("Ray worker pid: {}".format(os.getpid()), file=sys.stderr)
            sys.stdout.flush()
            sys.stderr.flush()

            worker_dict["stdout_file"] = os.path.abspath(log_stdout_file.name)
            worker_dict["stderr_file"] = os.path.abspath(log_stderr_file.name)
        worker.redis_client.hmset(b"Workers:" + worker.worker_id, worker_dict)
    else:
        raise Exception("This code should be unreachable.")

    # Create an object store client.
    worker.plasma_client = thread_safe_client(
        plasma.connect(node.plasma_store_socket_name, None, 0, 300))
    job_id_str = _random_string()

    # If this is a driver, set the current task ID, the task driver ID, and set
    # the task index to 0.
    if mode == SCRIPT_MODE:
        # If the user provided an object_id_seed, then set the current task ID
        # deterministically based on that seed (without altering the state of
        # the user's random number generator). Otherwise, set the current task
        # ID randomly to avoid object ID collisions.
        numpy_state = np.random.get_state()
        if node.object_id_seed is not None:
            np.random.seed(node.object_id_seed)
        else:
            # Try to use true randomness.
            np.random.seed(None)
        # Reset the state of the numpy random number generator.
        np.random.set_state(numpy_state)

        # Create an entry for the driver task in the task table. This task is
        # added immediately with status RUNNING. This allows us to push errors
        # related to this driver task back to the driver.  For example, if the
        # driver creates an object that is later evicted, we should notify the
        # user that we're unable to reconstruct the object, since we cannot
        # rerun the driver.
        nil_actor_counter = 0

        function_descriptor = FunctionDescriptor.for_driver_task()
        driver_task = ray._raylet.Task(
            worker.current_job_id,
            function_descriptor.get_function_descriptor_list(),
            [],  # arguments.
            0,  # num_returns.
            TaskID(job_id_str[:TaskID.size()]),  # parent_task_id.
            0,  # parent_counter.
            ActorID.nil(),  # actor_creation_id.
            ObjectID.nil(),  # actor_creation_dummy_object_id.
            0,  # max_actor_reconstructions.
            ActorID.nil(),  # actor_id.
            ActorHandleID.nil(),  # actor_handle_id.
            nil_actor_counter,  # actor_counter.
            [],  # new_actor_handles.
            [],  # execution_dependencies.
            {},  # resource_map.
            {},  # placement_resource_map.
        )
        task_table_data = ray.gcs_utils.TaskTableData()
        task_table_data.task = driver_task._serialized_raylet_task()

        # Add the driver task to the task table.
        ray.state.state._execute_command(
            driver_task.task_id(), "RAY.TABLE_ADD",
            ray.gcs_utils.TablePrefix.Value("RAYLET_TASK"),
            ray.gcs_utils.TablePubsub.Value("RAYLET_TASK_PUBSUB"),
            driver_task.task_id().binary(),
            task_table_data.SerializeToString())

        # Set the driver's current task ID to the task ID assigned to the
        # driver task.
        worker.task_context.current_task_id = driver_task.task_id()

    worker.raylet_client = ray._raylet.RayletClient(
        node.raylet_socket_name,
        ClientID(worker.worker_id),
        (mode == WORKER_MODE),
        JobID(job_id_str),
    )

    # Start the import thread
    worker.import_thread = import_thread.ImportThread(worker, mode,
                                                      worker.threads_stopped)
    worker.import_thread.start()

    # If this is a driver running in SCRIPT_MODE, start a thread to print error
    # messages asynchronously in the background. Ideally the scheduler would
    # push messages to the driver's worker service, but we ran into bugs when
    # trying to properly shutdown the driver's worker service, so we are
    # temporarily using this implementation which constantly queries the
    # scheduler for new error messages.
    if mode == SCRIPT_MODE:
        q = queue.Queue()
        worker.listener_thread = threading.Thread(
            target=listen_error_messages_raylet,
            name="ray_listen_error_messages",
            args=(worker, q, worker.threads_stopped))
        worker.printer_thread = threading.Thread(
            target=print_error_messages_raylet,
            name="ray_print_error_messages",
            args=(q, worker.threads_stopped))
        worker.listener_thread.daemon = True
        worker.listener_thread.start()
        worker.printer_thread.daemon = True
        worker.printer_thread.start()
        if log_to_driver:
            worker.logger_thread = threading.Thread(
                target=print_logs,
                name="ray_print_logs",
                args=(worker.redis_client, worker.threads_stopped))
            worker.logger_thread.daemon = True
            worker.logger_thread.start()

    # If we are using the raylet code path and we are not in local mode, start
    # a background thread to periodically flush profiling data to the GCS.
    if mode != LOCAL_MODE:
        worker.profiler.start_flush_thread()

    if mode == SCRIPT_MODE:
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
        # Export cached remote functions and actors to the workers.
        worker.function_actor_manager.export_cached()
    worker.cached_functions_to_run = None


def disconnect():
    """Disconnect this worker from the raylet and object store."""
    # Reset the list of cached remote functions and actors so that if more
    # remote functions or actors are defined and then connect is called again,
    # the remote functions will be exported. This is mostly relevant for the
    # tests.
    worker = global_worker
    if worker.connected:
        # Shutdown all of the threads that we've started. TODO(rkn): This
        # should be handled cleanly in the worker object's destructor and not
        # in this disconnect method.
        worker.threads_stopped.set()
        if hasattr(worker, "import_thread"):
            worker.import_thread.join_import_thread()
        if hasattr(worker, "profiler") and hasattr(worker.profiler, "t"):
            worker.profiler.join_flush_thread()
        if hasattr(worker, "listener_thread"):
            worker.listener_thread.join()
        if hasattr(worker, "printer_thread"):
            worker.printer_thread.join()
        if hasattr(worker, "logger_thread"):
            worker.logger_thread.join()
        worker.threads_stopped.clear()
        worker._session_index += 1

    worker.node = None  # Disconnect the worker from the node.
    worker.cached_functions_to_run = []
    worker.function_actor_manager.reset_cache()
    worker.serialization_context_map.clear()

    if hasattr(worker, "raylet_client"):
        del worker.raylet_client
    if hasattr(worker, "plasma_client"):
        worker.plasma_client.disconnect()


@contextmanager
def _changeproctitle(title, next_title):
    if setproctitle:
        setproctitle.setproctitle(title)
    yield
    if setproctitle:
        setproctitle.setproctitle(next_title)


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
    logger.warning(
        "WARNING: Could not produce a deterministic class ID for class "
        "{}".format(cls))
    return hashlib.sha1(new_class_id).digest()


def register_custom_serializer(cls,
                               use_pickle=False,
                               use_dict=False,
                               serializer=None,
                               deserializer=None,
                               local=False,
                               job_id=None,
                               class_id=None):
    """Enable serialization and deserialization for a particular class.

    This method runs the register_class function defined below on every worker,
    which will enable ray to properly serialize and deserialize objects of
    this class.

    Args:
        cls (type): The class that ray should use this custom serializer for.
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
        job_id: ID of the job that we want to register the class for.
        class_id: ID of the class that we are registering. If this is not
            specified, we will calculate a new one inside the function.

    Raises:
        Exception: An exception is raised if pickle=False and the class cannot
            be efficiently serialized by Ray. This can also raise an exception
            if use_dict is true and cls is not pickleable.
    """
    worker = global_worker
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

    if class_id is None:
        if not local:
            # In this case, the class ID will be used to deduplicate the class
            # across workers. Note that cloudpickle unfortunately does not
            # produce deterministic strings, so these IDs could be different
            # on different workers. We could use something weaker like
            # cls.__name__, however that would run the risk of having
            # collisions.
            # TODO(rkn): We should improve this.
            try:
                # Attempt to produce a class ID that will be the same on each
                # worker. However, determinism is not guaranteed, and the
                # result may be different on different workers.
                class_id = _try_to_compute_deterministic_class_id(cls)
            except Exception:
                raise serialization.CloudPickleError("Failed to pickle class "
                                                     "'{}'".format(cls))
        else:
            # In this case, the class ID only needs to be meaningful on this
            # worker and not across workers.
            class_id = _random_string()

        # Make sure class_id is a string.
        class_id = ray.utils.binary_to_hex(class_id)

    if job_id is None:
        job_id = worker.current_job_id
    assert isinstance(job_id, JobID)

    def register_class_for_serialization(worker_info):
        # TODO(rkn): We need to be more thoughtful about what to do if custom
        # serializers have already been registered for class_id. In some cases,
        # we may want to use the last user-defined serializers and ignore
        # subsequent calls to register_custom_serializer that were made by the
        # system.

        serialization_context = worker_info[
            "worker"].get_serialization_context(job_id)
        serialization_context.register_type(
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


def get(object_ids):
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

    Raises:
        Exception: An exception is raised if the task that created the object
            or that created one of the objects raised an exception.
    """
    worker = global_worker
    worker.check_connected()
    with profiling.profile("ray.get"):
        is_individual_id = isinstance(object_ids, ray.ObjectID)
        if is_individual_id:
            object_ids = [object_ids]

        if not isinstance(object_ids, list):
            raise ValueError("'object_ids' must either be an object ID "
                             "or a list of object IDs.")

        global last_task_error_raise_time
        values = worker.get_object(object_ids)
        for i, value in enumerate(values):
            if isinstance(value, RayError):
                last_task_error_raise_time = time.time()
                raise value

        # Run post processors.
        for post_processor in worker._post_get_hooks:
            values = post_processor(object_ids, values)

        if is_individual_id:
            values = values[0]
        return values


def put(value):
    """Store an object in the object store.

    Args:
        value: The Python object to be stored.

    Returns:
        The object ID assigned to this value.
    """
    worker = global_worker
    worker.check_connected()
    with profiling.profile("ray.put"):
        if worker.mode == LOCAL_MODE:
            object_id = worker.local_mode_manager.put_object(value)
        else:
            object_id = ray._raylet.compute_put_id(
                worker.current_task_id,
                worker.task_context.put_index,
            )
            worker.put_object(object_id, value)
        worker.task_context.put_index += 1
        return object_id


def wait(object_ids, num_returns=1, timeout=None):
    """Return a list of IDs that are ready and a list of IDs that are not.

    .. warning::

        The **timeout** argument used to be in **milliseconds** (up through
        ``ray==0.6.1``) and now it is in **seconds**.

    If timeout is set, the function returns either when the requested number of
    IDs are ready or when the timeout is reached, whichever occurs first. If it
    is not set, the function simply waits until that number of objects is ready
    and returns that exact number of object IDs.

    This method returns two lists. The first list consists of object IDs that
    correspond to objects that are available in the object store. The second
    list corresponds to the rest of the object IDs (which may or may not be
    ready).

    Ordering of the input list of object IDs is preserved. That is, if A
    precedes B in the input list, and both are in the ready list, then A will
    precede B in the ready list. This also holds true if A and B are both in
    the remaining list.

    Args:
        object_ids (List[ObjectID]): List of object IDs for objects that may or
            may not be ready. Note that these IDs must be unique.
        num_returns (int): The number of object IDs that should be returned.
        timeout (float): The maximum amount of time in seconds to wait before
            returning.

    Returns:
        A list of object IDs that are ready and a list of the remaining object
        IDs.
    """
    worker = global_worker

    if isinstance(object_ids, ObjectID):
        raise TypeError(
            "wait() expected a list of ray.ObjectID, got a single ray.ObjectID"
        )

    if not isinstance(object_ids, list):
        raise TypeError(
            "wait() expected a list of ray.ObjectID, got {}".format(
                type(object_ids)))

    if isinstance(timeout, int) and timeout != 0:
        logger.warning("The 'timeout' argument now requires seconds instead "
                       "of milliseconds. This message can be suppressed by "
                       "passing in a float.")

    if timeout is not None and timeout < 0:
        raise ValueError("The 'timeout' argument must be nonnegative. "
                         "Received {}".format(timeout))

    for object_id in object_ids:
        if not isinstance(object_id, ObjectID):
            raise TypeError("wait() expected a list of ray.ObjectID, "
                            "got list containing {}".format(type(object_id)))

    worker.check_connected()
    # TODO(swang): Check main thread.
    with profiling.profile("ray.wait"):
        # When Ray is run in LOCAL_MODE, all functions are run immediately,
        # so all objects in object_id are ready.
        if worker.mode == LOCAL_MODE:
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

        timeout = timeout if timeout is not None else 10**6
        timeout_milliseconds = int(timeout * 1000)
        ready_ids, remaining_ids = worker.raylet_client.wait(
            object_ids,
            num_returns,
            timeout_milliseconds,
            False,
            worker.current_task_id,
        )
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
                   max_reconstructions=None,
                   worker=None):
    def decorator(function_or_class):
        if (inspect.isfunction(function_or_class)
                or is_cython(function_or_class)):
            # Set the remote function default resources.
            if max_reconstructions is not None:
                raise Exception("The keyword 'max_reconstructions' is not "
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

            return worker.make_actor(function_or_class, num_cpus, num_gpus,
                                     resources, max_reconstructions)

        raise Exception("The @ray.remote decorator must be applied to "
                        "either a function or to a class.")

    return decorator


def remote(*args, **kwargs):
    """Define a remote function or an actor class.

    This can be used with no arguments to define a remote function or actor as
    follows:

    .. code-block:: python

        @ray.remote
        def f():
            return 1

        @ray.remote
        class Foo(object):
            def method(self):
                return 1

    It can also be used with specific keyword arguments:

    * **num_return_vals:** This is only for *remote functions*. It specifies
      the number of object IDs returned by the remote function invocation.
    * **num_cpus:** The quantity of CPU cores to reserve for this task or for
      the lifetime of the actor.
    * **num_gpus:** The quantity of GPUs to reserve for this task or for the
      lifetime of the actor.
    * **resources:** The quantity of various custom resources to reserve for
      this task or for the lifetime of the actor. This is a dictionary mapping
      strings (resource names) to numbers.
    * **max_calls:** Only for *remote functions*. This specifies the maximum
      number of times that a given worker can execute the given remote function
      before it must exit (this can be used to address memory leaks in
      third-party libraries or to reclaim resources that cannot easily be
      released, e.g., GPU memory that was acquired by TensorFlow). By
      default this is infinite.
    * **max_reconstructions**: Only for *actors*. This specifies the maximum
      number of times that the actor should be reconstructed when it dies
      unexpectedly. The minimum valid value is 0 (default), which indicates
      that the actor doesn't need to be reconstructed. And the maximum valid
      value is ray.ray_constants.INFINITE_RECONSTRUCTIONS.

    This can be done as follows:

    .. code-block:: python

        @ray.remote(num_gpus=1, max_calls=1, num_return_vals=2)
        def f():
            return 1, 2

        @ray.remote(num_cpus=2, resources={"CustomResource": 1})
        class Foo(object):
            def method(self):
                return 1
    """
    worker = get_global_worker()

    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # This is the case where the decorator is just @ray.remote.
        return make_decorator(worker=worker)(args[0])

    # Parse the keyword arguments from the decorator.
    error_string = ("The @ray.remote decorator must be applied either "
                    "with no arguments and no parentheses, for example "
                    "'@ray.remote', or it must be applied using some of "
                    "the arguments 'num_return_vals', 'num_cpus', 'num_gpus', "
                    "'resources', 'max_calls', "
                    "or 'max_reconstructions', like "
                    "'@ray.remote(num_return_vals=2, "
                    "resources={\"CustomResource\": 1})'.")
    assert len(args) == 0 and len(kwargs) > 0, error_string
    for key in kwargs:
        assert key in [
            "num_return_vals", "num_cpus", "num_gpus", "resources",
            "max_calls", "max_reconstructions"
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
    max_reconstructions = kwargs.get("max_reconstructions")

    return make_decorator(
        num_return_vals=num_return_vals,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        resources=resources,
        max_calls=max_calls,
        max_reconstructions=max_reconstructions,
        worker=worker)
