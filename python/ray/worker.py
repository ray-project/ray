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
import os
import redis
import signal
from six.moves import queue
import sys
import threading
import time
import traceback
import random

# Ray modules
import ray.cloudpickle as pickle
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
    ActorID,
    JobID,
    ObjectID,
)
from ray import import_thread
from ray import profiling

from ray.exceptions import (
    RayError,
    RayTaskError,
    ObjectStoreFullError,
)
from ray.function_manager import FunctionActorManager
from ray.utils import (
    _random_string,
    check_oversized_pickle,
    is_cython,
    setup_logger,
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
    import aiohttp
except ImportError:
    aiohttp = None

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
        # When the worker is constructed. Record the original value of the
        # CUDA_VISIBLE_DEVICES environment variable.
        self.original_gpu_ids = ray.utils.get_cuda_visible_devices()
        self.memory_monitor = memory_monitor.MemoryMonitor()
        # A dictionary that maps from driver id to SerializationContext
        # TODO: clean up the SerializationContext once the job finished.
        self.serialization_context_map = {}
        self.function_actor_manager = FunctionActorManager(self)
        # This event is checked regularly by all of the threads so that they
        # know when to exit.
        self.threads_stopped = threading.Event()
        # Index of the current session. This number will
        # increment every time when `ray.shutdown` is called.
        self._session_index = 0
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
    def use_pickle(self):
        self.check_connected()
        return self.node.use_pickle

    @property
    def current_job_id(self):
        if hasattr(self, "core_worker"):
            return self.core_worker.get_current_job_id()
        return JobID.nil()

    @property
    def actor_id(self):
        if hasattr(self, "core_worker"):
            return self.core_worker.get_actor_id()
        return ActorID.nil()

    @property
    def current_task_id(self):
        return self.core_worker.get_current_task_id()

    @property
    def current_session_and_job(self):
        """Get the current session index and job id as pair."""
        assert isinstance(self._session_index, int)
        assert isinstance(self.current_job_id, ray.JobID)
        return self._session_index, self.current_job_id

    def mark_actor_init_failed(self, error):
        """Called to mark this actor as failed during initialization."""

        self.actor_init_error = error

    def reraise_actor_init_error(self):
        """Raises any previous actor initialization error."""

        if self.actor_init_error is not None:
            raise self.actor_init_error

    def get_serialization_context(self, job_id=None):
        """Get the SerializationContext of the job that this worker is processing.

        Args:
            job_id: The ID of the job that indicates which job to get
                the serialization context for.

        Returns:
            The serialization context of the given job.
        """
        # This function needs to be protected by a lock, because it will be
        # called by`register_class_for_serialization`, as well as the import
        # thread, from different threads. Also, this function will recursively
        # call itself, so we use RLock here.
        if job_id is None:
            job_id = self.current_job_id
        with self.lock:
            if job_id not in self.serialization_context_map:
                self.serialization_context_map[
                    job_id] = serialization.SerializationContext(self)
                self.serialization_context_map[job_id].initialize()
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

    def put_object(self, value, object_id=None):
        """Put value in the local object store with object id `objectid`.

        This assumes that the value for `objectid` has not yet been placed in
        the local object store. If the plasma store is full, the worker will
        automatically retry up to DEFAULT_PUT_OBJECT_RETRIES times. Each
        retry will delay for an exponentially doubling amount of time,
        starting with DEFAULT_PUT_OBJECT_DELAY. After this, exception
        will be raised.

        Args:
            value: The value to put in the object store.
            object_id (object_id.ObjectID): The object ID of the value to be
                put. If None, one will be generated.

        Returns:
            object_id.ObjectID: The object ID the object was put under.

        Raises:
            ray.exceptions.ObjectStoreFullError: This is raised if the attempt
                to store the object fails because the object store is full even
                after multiple retries.
        """
        # Make sure that the value is not an object ID.
        if isinstance(value, ObjectID):
            raise TypeError(
                "Calling 'put' on an ray.ObjectID is not allowed "
                "(similarly, returning an ray.ObjectID from a remote "
                "function is not allowed). If you really want to "
                "do this, you can wrap the ray.ObjectID in a list and "
                "call 'put' on it (or return it).")

        serialized_value = self.get_serialization_context().serialize(value)
        return self.core_worker.put_serialized_object(
            serialized_value, object_id=object_id)

    def deserialize_objects(self,
                            data_metadata_pairs,
                            object_ids,
                            error_timeout=10):
        context = self.get_serialization_context()
        return context.deserialize_objects(data_metadata_pairs, object_ids,
                                           error_timeout)

    def get_objects(self, object_ids):
        """Get the values in the object store associated with the IDs.

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
            return self.local_mode_manager.get_objects(object_ids)

        data_metadata_pairs = self.core_worker.get_objects(
            object_ids, self.current_task_id)
        return self.deserialize_objects(data_metadata_pairs, object_ids)

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
            values = self.get_objects(object_ids)
            for i, value in enumerate(values):
                if isinstance(value, RayError):
                    raise value
                else:
                    arguments[object_indices[i]] = value

        return ray.signature.recover_args(arguments)

    def main_loop(self):
        """The main loop a worker runs to receive and execute tasks."""

        def sigterm_handler(signum, frame):
            shutdown(True)
            sys.exit(1)

        signal.signal(signal.SIGTERM, sigterm_handler)
        self.core_worker.run_task_loop()


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

    all_resource_ids = global_worker.core_worker.resource_ids()
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

    return global_worker.core_worker.resource_ids()


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


def init(address=None,
         redis_address=None,
         num_cpus=None,
         num_gpus=None,
         memory=None,
         object_store_memory=None,
         resources=None,
         driver_object_store_memory=None,
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
         include_webui=aiohttp is not None,
         webui_host="localhost",
         job_id=None,
         configure_logging=True,
         logging_level=logging.INFO,
         logging_format=ray_constants.LOGGER_FORMAT,
         plasma_store_socket_name=None,
         raylet_socket_name=None,
         temp_dir=None,
         load_code_from_local=False,
         use_pickle=ray.cloudpickle.FAST_CLOUDPICKLE_USED,
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

        ray.init(address="123.45.67.89:6379")

    Args:
        address (str): The address of the Ray cluster to connect to. If
            this address is not provided, then this command will start Redis, a
            raylet, a plasma store, a plasma manager, and some workers.
            It will also kill these processes when Python exits.
        redis_address (str): Deprecated; same as address.
        num_cpus (int): Number of cpus the user wishes all raylets to
            be configured with.
        num_gpus (int): Number of gpus the user wishes all raylets to
            be configured with.
        resources: A dictionary mapping the name of a resource to the quantity
            of that resource available.
        memory: The amount of memory (in bytes) that is available for use by
            workers requesting memory resources. By default, this is autoset
            based on available system memory.
        object_store_memory: The amount of memory (in bytes) to start the
            object store with. By default, this is autoset based on available
            system memory, subject to a 20GB cap.
        redis_max_memory: The max amount of memory (in bytes) to allow each
            redis shard to use. Once the limit is exceeded, redis will start
            LRU eviction of entries. This only applies to the sharded redis
            tables (task, object, and profile tables).  By default, this is
            autoset based on available system memory, subject to a 10GB cap.
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
        driver_object_store_memory (int): Limit the amount of memory the driver
            can use in the object store for creating objects. By default, this
            is autoset based on available system memory, subject to a 20GB cap.
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
        webui_host: The host to bind the web UI server to. Can either be
            localhost (127.0.0.1) or 0.0.0.0 (available from all interfaces).
            By default, this is set to localhost to prevent access from
            external machines.
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
        use_pickle: Whether data objects should be serialized with cloudpickle.
        _internal_config (str): JSON configuration for overriding
            RayConfig defaults. For testing purposes ONLY.

    Returns:
        Address information about the started processes.

    Raises:
        Exception: An exception is raised if an inappropriate combination of
            arguments is passed in.
    """

    if redis_address is not None or address is not None:
        redis_address, _, _ = services.validate_redis_address(
            address, redis_address)

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
            webui_host=webui_host,
            memory=memory,
            object_store_memory=object_store_memory,
            redis_max_memory=redis_max_memory,
            plasma_store_socket_name=plasma_store_socket_name,
            raylet_socket_name=raylet_socket_name,
            temp_dir=temp_dir,
            load_code_from_local=load_code_from_local,
            use_pickle=use_pickle,
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
        if memory is not None:
            raise Exception("When connecting to an existing cluster, "
                            "memory must not be provided.")
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
            load_code_from_local=load_code_from_local,
            use_pickle=use_pickle)
        _global_node = ray.node.Node(
            ray_params, head=False, shutdown_at_exit=False, connect_only=True)

    connect(
        _global_node,
        mode=driver_mode,
        log_to_driver=log_to_driver,
        worker=global_worker,
        driver_object_store_memory=driver_object_store_memory,
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

    disconnect(exiting_interpreter)

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


def sigterm_handler(signum, frame):
    sys.exit(signal.SIGTERM)


signal.signal(signal.SIGTERM, sigterm_handler)

# Define a custom excepthook so that if the driver exits with an exception, we
# can push that exception to Redis.
normal_excepthook = sys.excepthook


def custom_excepthook(type, value, tb):
    # If this is a driver, push the exception to redis.
    if global_worker.mode == SCRIPT_MODE:
        error_message = "".join(traceback.format_tb(tb))
        try:
            global_worker.redis_client.hmset(
                b"Drivers:" + global_worker.worker_id,
                {"exception": error_message})
        except (ConnectionRefusedError, redis.exceptions.ConnectionError):
            logger.warning("Could not push exception to redis.")
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

            def color_for(data):
                if data["pid"] == "raylet":
                    return colorama.Fore.YELLOW
                else:
                    return colorama.Fore.CYAN

            if data["ip"] == localhost:
                for line in data["lines"]:
                    print("{}{}(pid={}){} {}".format(
                        colorama.Style.DIM, color_for(data), data["pid"],
                        colorama.Style.RESET_ALL, line))
            else:
                for line in data["lines"]:
                    print("{}{}(pid={}, ip={}){} {}".format(
                        colorama.Style.DIM, color_for(data), data["pid"],
                        data["ip"], colorama.Style.RESET_ALL, line))

            if (num_consecutive_messages_received % 100 == 0
                    and num_consecutive_messages_received > 0):
                logger.warning(
                    "The driver may not be able to keep up with the "
                    "stdout/stderr of the workers. To avoid forwarding logs "
                    "to the driver, use 'ray.init(log_to_driver=False)'.")
    except (OSError, redis.exceptions.ConnectionError) as e:
        logger.error("print_logs: {}".format(e))
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
        # Get the errors that occurred before the call to subscribe.
        error_messages = ray.errors()
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
                logger.warning(error_message)
    except (OSError, redis.exceptions.ConnectionError) as e:
        logger.error("listen_error_messages_raylet: {}".format(e))
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
            driver_object_store_memory=None,
            job_id=None):
    """Connect this worker to the raylet, to Plasma, and to Redis.

    Args:
        node (ray.node.Node): The node to connect.
        mode: The mode of the worker. One of SCRIPT_MODE, WORKER_MODE, and
            LOCAL_MODE.
        log_to_driver (bool): If true, then output from all of the worker
            processes on all nodes will be directed to the driver.
        worker: The ray.Worker instance.
        driver_object_store_memory: Limit the amount of memory the driver can
            use in the object store when creating objects.
        job_id: The ID of job. If it's None, then we will generate one.
    """
    # Do some basic checking to make sure we didn't call ray.init twice.
    error_message = "Perhaps you called ray.init twice by accident?"
    assert not worker.connected, error_message
    assert worker.cached_functions_to_run is not None, error_message

    # Enable nice stack traces on SIGSEGV etc.
    if not faulthandler.is_enabled():
        faulthandler.enable(all_threads=False)

    if mode is not LOCAL_MODE:
        # Create a Redis client to primary.
        # The Redis client can safely be shared between threads. However,
        # that is not true of Redis pubsub clients. See the documentation at
        # https://github.com/andymccurdy/redis-py#thread-safety.
        worker.redis_client = node.create_redis_client()

    # Initialize some fields.
    if mode is WORKER_MODE:
        # We should not specify the job_id if it's `WORKER_MODE`.
        assert job_id is None
        job_id = JobID.nil()
        # TODO(qwang): Rename this to `worker_id_str` or type to `WorkerID`
        worker.worker_id = _random_string()
        if setproctitle:
            setproctitle.setproctitle("ray_worker")
    elif mode is LOCAL_MODE:
        # Code path of local mode
        if job_id is None:
            job_id = JobID.from_int(random.randint(1, 100000))
        worker.worker_id = ray.utils.compute_driver_id_from_job(
            job_id).binary()
    else:
        # This is the code path of driver mode.
        if job_id is None:
            # TODO(qwang): use `GcsClient::GenerateJobId()` here.
            job_id = JobID.from_int(
                int(worker.redis_client.incr("JobCounter")))
        # When tasks are executed on remote workers in the context of multiple
        # drivers, the current job ID is used to keep track of which job is
        # responsible for the task so that error messages will be propagated to
        # the correct driver.
        worker.worker_id = ray.utils.compute_driver_id_from_job(
            job_id).binary()

    if not isinstance(job_id, JobID):
        raise TypeError("The type of given job id must be JobID.")

    # All workers start out as non-actors. A worker can be turned into an actor
    # after it is created.
    worker.node = node
    worker.set_mode(mode)

    # If running Ray in LOCAL_MODE, there is no need to create call
    # create_worker or to start the worker service.
    if mode == LOCAL_MODE:
        worker.local_mode_manager = LocalModeManager()
        return

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
        raise ValueError("Invalid worker mode. Expected DRIVER or WORKER.")

    redis_address, redis_port = node.redis_address.split(":")
    gcs_options = ray._raylet.GcsClientOptions(
        redis_address,
        int(redis_port),
        node.redis_password,
    )
    worker.core_worker = ray._raylet.CoreWorker(
        (mode == SCRIPT_MODE),
        node.plasma_store_socket_name,
        node.raylet_socket_name,
        job_id,
        gcs_options,
        node.get_logs_dir_path(),
        node.node_ip_address,
    )
    worker.raylet_client = ray._raylet.RayletClient(worker.core_worker)

    if driver_object_store_memory is not None:
        worker.core_worker.set_object_store_client_options(
            "ray_driver_{}".format(os.getpid()), driver_object_store_memory)

    # Put something in the plasma store so that subsequent plasma store
    # accesses will be faster. Currently the first access is always slow, and
    # we don't want the user to experience this.
    temporary_object_id = ray.ObjectID.from_random()
    worker.put_object(1, object_id=temporary_object_id)
    ray.internal.free([temporary_object_id])

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
    worker.cached_functions_to_run = None


def disconnect(exiting_interpreter=False):
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
    worker.serialization_context_map.clear()

    # We need to destruct the core worker here because after this function,
    # we will tear down any processes spawned by ray.init() and the background
    # threads in the core worker don't currently handle that gracefully.
    if hasattr(worker, "core_worker"):
        del worker.core_worker


@contextmanager
def _changeproctitle(title, next_title):
    if setproctitle:
        setproctitle.setproctitle(title)
    yield
    if setproctitle:
        setproctitle.setproctitle(next_title)


def register_custom_serializer(cls,
                               serializer=None,
                               deserializer=None,
                               use_pickle=False,
                               use_dict=False,
                               local=None,
                               job_id=None,
                               class_id=None):
    """Registers custom functions for efficient object serialization.

    The serializer and deserializer are used when transferring objects of
    `cls` across processes and nodes. This can be significantly faster than
    the Ray default fallbacks. Wraps `register_custom_serializer` underneath.

    `use_pickle` tells Ray to automatically use cloudpickle for serialization,
    and `use_dict` automatically uses `cls.__dict__`.

    When calling this function, you can only provide one of the following:

        1. serializer and deserializer
        2. `use_pickle`
        3. `use_dict`

    Args:
        cls (type): The class that ray should use this custom serializer for.
        serializer: The custom serializer that takes in a cls instance and
            outputs a serialized representation. use_pickle and use_dict
            must be False if provided.
        deserializer: The custom deserializer that takes in a serialized
            representation of the cls and outputs a cls instance. use_pickle
            and use_dict must be False if provided.
        use_pickle (bool): If true, objects of this class will be
            serialized using pickle. Must be False if
            use_dict is true.
        use_dict (bool): If true, objects of this class be serialized turning
            their __dict__ fields into a dictionary. Must be False if
            use_pickle is true.
        local: Deprecated.
        job_id: Deprecated.
        class_id (str): Unique ID of the class. Autogenerated if None.
    """
    if job_id:
        raise DeprecationWarning(
            "`job_id` is no longer a valid parameter and will be removed in "
            "future versions of Ray. If this breaks your application, "
            "see `SerializationContext.register_custom_serializer`.")
    if local:
        raise DeprecationWarning(
            "`local` is no longer a valid parameter and will be removed in "
            "future versions of Ray. If this breaks your application, "
            "see `SerializationContext.register_custom_serializer`.")
    context = global_worker.get_serialization_context()
    context.register_custom_serializer(
        cls,
        use_pickle=use_pickle,
        use_dict=use_dict,
        serializer=serializer,
        deserializer=deserializer,
        class_id=class_id)


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
        values = worker.get_objects(object_ids)
        for i, value in enumerate(values):
            if isinstance(value, RayError):
                last_task_error_raise_time = time.time()
                if isinstance(value, ray.exceptions.UnreconstructableError):
                    worker.core_worker.dump_object_store_memory_usage()
                if isinstance(value, RayTaskError):
                    raise value.as_instanceof_cause()
                else:
                    raise value

        # Run post processors.
        for post_processor in worker._post_get_hooks:
            values = post_processor(object_ids, values)

        if is_individual_id:
            values = values[0]
        return values


def put(value, weakref=False):
    """Store an object in the object store.

    The object may not be evicted while a reference to the returned ID exists.
    Note that this pinning only applies to the particular object ID returned
    by put, not object IDs in general.

    Args:
        value: The Python object to be stored.
        weakref: If set, allows the object to be evicted while a reference
            to the returned ID exists. You might want to set this if putting
            a lot of objects that you might not need in the future.

    Returns:
        The object ID assigned to this value.
    """
    worker = global_worker
    worker.check_connected()
    with profiling.profile("ray.put"):
        if worker.mode == LOCAL_MODE:
            object_id = worker.local_mode_manager.put_object(value)
        else:
            try:
                object_id = worker.put_object(value)
            except ObjectStoreFullError:
                logger.info(
                    "Put failed since the value was either too large or the "
                    "store was full of pinned objects. If you are putting "
                    "and holding references to a lot of object ids, consider "
                    "ray.put(value, weakref=True) to allow object data to "
                    "be evicted early.")
                raise
        # Pin the object buffer with the returned id. This avoids put returns
        # from getting evicted out from under the id.
        # TODO(edoakes): we should be able to avoid this extra IPC by holding
        # a reference to the buffer created when putting the object, but the
        # buffer returned by the plasma store create method doesn't prevent
        # the object from being evicted.
        if not weakref and not worker.mode == LOCAL_MODE:
            object_id.set_buffer_ref(
                worker.core_worker.get_objects([object_id],
                                               worker.current_task_id))
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
        ready_ids, remaining_ids = worker.core_worker.wait(
            object_ids,
            num_returns,
            timeout_milliseconds,
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
                   memory=None,
                   object_store_memory=None,
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
                function_or_class, num_cpus, num_gpus, memory,
                object_store_memory, resources, num_return_vals, max_calls)

        if inspect.isclass(function_or_class):
            if num_return_vals is not None:
                raise Exception("The keyword 'num_return_vals' is not allowed "
                                "for actors.")
            if max_calls is not None:
                raise Exception("The keyword 'max_calls' is not allowed for "
                                "actors.")

            return worker.make_actor(function_or_class, num_cpus, num_gpus,
                                     memory, object_store_memory, resources,
                                     max_reconstructions)

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
                    "'memory', 'object_store_memory', 'resources', "
                    "'max_calls', or 'max_reconstructions', like "
                    "'@ray.remote(num_return_vals=2, "
                    "resources={\"CustomResource\": 1})'.")
    assert len(args) == 0 and len(kwargs) > 0, error_string
    for key in kwargs:
        assert key in [
            "num_return_vals",
            "num_cpus",
            "num_gpus",
            "memory",
            "object_store_memory",
            "resources",
            "max_calls",
            "max_reconstructions",
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
    memory = kwargs.get("memory")
    object_store_memory = kwargs.get("object_store_memory")

    return make_decorator(
        num_return_vals=num_return_vals,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        memory=memory,
        object_store_memory=object_store_memory,
        resources=resources,
        max_calls=max_calls,
        max_reconstructions=max_reconstructions,
        worker=worker)
