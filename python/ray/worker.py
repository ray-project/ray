from contextlib import contextmanager
import colorama
import atexit
import faulthandler
import hashlib
import inspect
import io
import json
import logging
import os
import redis
from six.moves import queue
import sys
import threading
import time
import traceback

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
import ray
import setproctitle
import ray.signature
import ray.state

from ray import (
    ActorID,
    JobID,
    ObjectRef,
    Language,
)
from ray import import_thread
from ray import profiling

from ray.exceptions import (
    RayConnectionError,
    RayError,
    RayTaskError,
    ObjectStoreFullError,
)
from ray.function_manager import FunctionActorManager
from ray.utils import (_random_string, check_oversized_pickle, is_cython,
                       setup_logger, create_and_init_new_worker_log, open_log)

SCRIPT_MODE = 0
WORKER_MODE = 1
LOCAL_MODE = 2

ERROR_KEY_PREFIX = b"Error:"

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


class ActorCheckpointInfo:
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


class Worker:
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
        # postprocessor must take two arguments ("object_refs", and "values").
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

    def put_object(self, value, object_ref=None, pin_object=True):
        """Put value in the local object store with object reference `object_ref`.

        This assumes that the value for `object_ref` has not yet been placed in
        the local object store. If the plasma store is full, the worker will
        automatically retry up to DEFAULT_PUT_OBJECT_RETRIES times. Each
        retry will delay for an exponentially doubling amount of time,
        starting with DEFAULT_PUT_OBJECT_DELAY. After this, exception
        will be raised.

        Args:
            value: The value to put in the object store.
            object_ref (ObjectRef): The object ref of the value to be
                put. If None, one will be generated.
            pin_object: If set, the object will be pinned at the raylet.

        Returns:
            ObjectRef: The object ref the object was put under.

        Raises:
            ray.exceptions.ObjectStoreFullError: This is raised if the attempt
                to store the object fails because the object store is full even
                after multiple retries.
        """
        # Make sure that the value is not an object ref.
        if isinstance(value, ObjectRef):
            raise TypeError(
                "Calling 'put' on an ray.ObjectRef is not allowed "
                "(similarly, returning an ray.ObjectRef from a remote "
                "function is not allowed). If you really want to "
                "do this, you can wrap the ray.ObjectRef in a list and "
                "call 'put' on it (or return it).")

        if self.mode == LOCAL_MODE:
            assert object_ref is None, ("Local Mode does not support "
                                        "inserting with an ObjectRef")

        serialized_value = self.get_serialization_context().serialize(value)
        # This *must* be the first place that we construct this python
        # ObjectRef because an entry with 0 local references is created when
        # the object is Put() in the core worker, expecting that this python
        # reference will be created. If another reference is created and
        # removed before this one, it will corrupt the state in the
        # reference counter.
        return ray.ObjectRef(
            self.core_worker.put_serialized_object(
                serialized_value, object_ref=object_ref,
                pin_object=pin_object))

    def deserialize_objects(self, data_metadata_pairs, object_refs):
        context = self.get_serialization_context()
        return context.deserialize_objects(data_metadata_pairs, object_refs)

    def get_objects(self, object_refs, timeout=None):
        """Get the values in the object store associated with the IDs.

        Return the values from the local object store for object_refs. This
        will block until all the values for object_refs have been written to
        the local object store.

        Args:
            object_refs (List[object_ref.ObjectRef]): A list of the object refs
                whose values should be retrieved.
            timeout (float): timeout (float): The maximum amount of time in
                seconds to wait before returning.
        """
        # Make sure that the values are object refs.
        for object_ref in object_refs:
            if not isinstance(object_ref, ObjectRef):
                raise TypeError(
                    "Attempting to call `get` on the value {}, "
                    "which is not an ray.ObjectRef.".format(object_ref))

        timeout_ms = int(timeout * 1000) if timeout else -1
        data_metadata_pairs = self.core_worker.get_objects(
            object_refs, self.current_task_id, timeout_ms)
        return self.deserialize_objects(data_metadata_pairs, object_refs)

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
                    "run_on_other_drivers": str(run_on_other_drivers),
                })
            self.redis_client.rpush("Exports", key)
            # TODO(rkn): If the worker fails after it calls setnx and before it
            # successfully completes the hmset and rpush, then the program will
            # most likely hang. This could be fixed by making these three
            # operations into a transaction (or by implementing a custom
            # command that does all three things).

    def main_loop(self):
        """The main loop a worker runs to receive and execute tasks."""

        def sigterm_handler(signum, frame):
            shutdown(True)
            sys.exit(1)

        ray.utils.set_sigterm_handler(sigterm_handler)
        self.core_worker.run_task_loop()
        sys.exit(0)


def get_gpu_ids():
    """Get the IDs of the GPUs that are available to the worker.

    If the CUDA_VISIBLE_DEVICES environment variable was set when the worker
    started up, then the IDs returned by this method will be a subset of the
    IDs in CUDA_VISIBLE_DEVICES. If not, the IDs will fall in the range
    [0, NUM_GPUS - 1], where NUM_GPUS is the number of GPUs that the node has.

    Returns:
        A list of GPU IDs.
    """

    # TODO(ilr) Handle inserting resources in local mode
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
        # Give all GPUs in local_mode.
        if global_worker.mode == LOCAL_MODE:
            max_gpus = global_worker.node.get_resource_spec().num_gpus
            return global_worker.original_gpu_ids[:max_gpus]

    return assigned_ids


def get_resource_ids():
    """Get the IDs of the resources that are available to the worker.

    Returns:
        A dictionary mapping the name of a resource to a list of pairs, where
        each pair consists of the ID of a resource and the fraction of that
        resource reserved for this worker.
    """
    if _mode() == LOCAL_MODE:
        raise RuntimeError("ray.get_resource_ids() currently does not work in "
                           "local_mode.")

    return global_worker.core_worker.resource_ids()


def get_webui_url():
    """Get the URL to access the web UI.

    Note that the URL does not specify which node the web UI is on.

    Returns:
        The URL of the web UI as a string.
    """
    if _global_node is None:
        raise RuntimeError("Ray has not been initialized/connected.")
    return _global_node.webui_url


global_worker = Worker()
"""Worker: The global Worker object for this worker process.

We use a global Worker object to ensure that there is a single worker object
per worker process.
"""

_global_node = None
"""ray.node.Node: The global node object that is created by ray.init()."""


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
         redis_port=None,
         num_cpus=None,
         num_gpus=None,
         memory=None,
         object_store_memory=None,
         resources=None,
         driver_object_store_memory=None,
         redis_max_memory=None,
         log_to_driver=True,
         node_ip_address=ray_constants.NODE_DEFAULT_IP,
         object_ref_seed=None,
         local_mode=False,
         redirect_worker_output=None,
         redirect_output=None,
         ignore_reinit_error=False,
         num_redis_shards=None,
         redis_max_clients=None,
         redis_password=ray_constants.REDIS_DEFAULT_PASSWORD,
         plasma_directory=None,
         huge_pages=False,
         include_java=False,
         include_dashboard=None,
         dashboard_host="localhost",
         dashboard_port=ray_constants.DEFAULT_DASHBOARD_PORT,
         job_id=None,
         configure_logging=True,
         logging_level=logging.INFO,
         logging_format=ray_constants.LOGGER_FORMAT,
         plasma_store_socket_name=None,
         raylet_socket_name=None,
         temp_dir=None,
         load_code_from_local=False,
         java_worker_options=None,
         use_pickle=True,
         _internal_config=None,
         lru_evict=False,
         enable_object_reconstruction=False):
    """
    Connect to an existing Ray cluster or start one and connect to it.

    This method handles two cases; either a Ray cluster already exists and we
    just attach this driver to it or we start all of the processes associated
    with a Ray cluster and attach to the newly started cluster.

    To start Ray and all of the relevant processes, use this as follows:

    .. code-block:: python

        ray.init()

    To connect to an existing Ray cluster, use this as follows (substituting
    in the appropriate address):

    .. code-block:: python

        ray.init(address="123.45.67.89:6379")

    You can also define an environment variable called `RAY_ADDRESS` in
    the same format as the `address` parameter to connect to an existing
    cluster with ray.init().

    Args:
        address (str): The address of the Ray cluster to connect to. If
            this address is not provided, then this command will start Redis,
            a raylet, a plasma store, a plasma manager, and some workers.
            It will also kill these processes when Python exits. If the driver
            is running on a node in a Ray cluster, using `auto` as the value
            tells the driver to detect the the cluster, removing the need to
            specify a specific node address.
        redis_address (str): Deprecated; same as address.
        redis_port (int): The port that the primary Redis shard should listen
            to. If None, then a random port will be chosen.
        num_cpus (int): Number of CPUs the user wishes to assign to each
            raylet.
        num_gpus (int): Number of GPUs the user wishes to assign to each
            raylet.
        resources: A dictionary mapping the names of custom resources to the
            quantities for them available.
        memory: The amount of memory (in bytes) that is available for use by
            workers requesting memory resources. By default, this is
            automatically set based on available system memory.
        object_store_memory: The amount of memory (in bytes) to start the
            object store with. By default, this is automatically set based on
            available system memory, subject to a 20GB cap.
        redis_max_memory: The max amount of memory (in bytes) to allow each
            redis shard to use. Once the limit is exceeded, redis will start
            LRU eviction of entries. This only applies to the sharded redis
            tables (task, object, and profile tables).  By default, this is
            autoset based on available system memory, subject to a 10GB cap.
        log_to_driver (bool): If true, the output from all of the worker
            processes on all nodes will be directed to the driver.
        node_ip_address (str): The IP address of the node that we are on.
        object_ref_seed (int): Used to seed the deterministic generation of
            object refs. The same value can be used across multiple runs of the
            same driver in order to generate the object refs in a consistent
            manner. However, the same ID should not be used for different
            drivers.
        local_mode (bool): If true, the code will be executed serially. This
            is useful for debugging.
        driver_object_store_memory (int): Limit the amount of memory the driver
            can use in the object store for creating objects. By default, this
            is autoset based on available system memory, subject to a 20GB cap.
        ignore_reinit_error: If true, Ray suppresses errors from calling
            ray.init() a second time. Ray won't be restarted.
        num_redis_shards: The number of Redis shards to start in addition to
            the primary Redis shard.
        redis_max_clients: If provided, attempt to configure Redis with this
            maxclients number.
        redis_password (str): Prevents external clients without the password
            from connecting to Redis if provided.
        plasma_directory: A directory where the Plasma memory mapped files
            will be created.
        huge_pages: Boolean flag indicating whether to start the Object
            Store with hugetlbfs support. Requires plasma_directory.
        include_java: Boolean flag indicating whether or not to enable java
            workers.
        include_dashboard: Boolean flag indicating whether or not to start the
            Ray dashboard, which displays the status of the Ray
            cluster. If this argument is None, then the UI will be started if
            the relevant dependencies are present.
        dashboard_host: The host to bind the dashboard server to. Can either be
            localhost (127.0.0.1) or 0.0.0.0 (available from all interfaces).
            By default, this is set to localhost to prevent access from
            external machines.
        dashboard_port: The port to bind the dashboard server to. Defaults to
            8265.
        job_id: The ID of this job.
        configure_logging: True (default) if configuration of logging is
            allowed here. Otherwise, the user may want to configure it
            separately.
        logging_level: Logging level, defaults to logging.INFO. Ignored unless
            "configure_logging" is true.
        logging_format: Logging format, defaults to string containing a
            timestamp, filename, line number, and message. See the source file
            ray_constants.py for details. Ignored unless "configure_logging"
            is true.
        plasma_store_socket_name (str): If provided, specifies the socket
            name used by the plasma store.
        raylet_socket_name (str): If provided, specifies the socket path
            used by the raylet process.
        temp_dir (str): If provided, specifies the root temporary
            directory for the Ray process. Defaults to an OS-specific
            conventional location, e.g., "/tmp/ray".
        load_code_from_local: Whether code should be loaded from a local
            module or from the GCS.
        java_worker_options: Overwrite the options to start Java workers.
        use_pickle: Deprecated.
        _internal_config (str): JSON configuration for overriding
            RayConfig defaults. For testing purposes ONLY.
        lru_evict (bool): If True, when an object store is full, it will evict
            objects in LRU order to make more space and when under memory
            pressure, ray.UnreconstructableError may be thrown. If False, then
            reference counting will be used to decide which objects are safe
            to evict and when under memory pressure, ray.ObjectStoreFullError
            may be thrown.
        enable_object_reconstruction (bool): If True, when an object stored in
            the distributed plasma store is lost due to node failure, Ray will
            attempt to reconstruct the object by re-executing the task that
            created the object. Arguments to the task will be recursively
            reconstructed. If False, then ray.UnreconstructableError will be
            thrown.

    Returns:
        Address information about the started processes.

    Raises:
        Exception: An exception is raised if an inappropriate combination of
            arguments is passed in.
    """

    if not use_pickle:
        raise DeprecationWarning("The use_pickle argument is deprecated.")

    if redis_address is not None:
        raise DeprecationWarning("The redis_address argument is deprecated. "
                                 "Please use address instead.")

    if "RAY_ADDRESS" in os.environ:
        if redis_address is None and (address is None or address == "auto"):
            address = os.environ["RAY_ADDRESS"]
        else:
            raise RuntimeError(
                "Cannot use both the RAY_ADDRESS environment variable and "
                "the address argument of ray.init simultaneously. If you "
                "use RAY_ADDRESS to connect to a specific Ray cluster, "
                "please call ray.init() or ray.init(address=\"auto\") on the "
                "driver.")

    if redis_address is not None or address is not None:
        redis_address, _, _ = services.validate_redis_address(
            address, redis_address)

    if configure_logging:
        setup_logger(logging_level, logging_format)

    if local_mode:
        driver_mode = LOCAL_MODE
    else:
        driver_mode = SCRIPT_MODE

    if global_worker.connected:
        if ignore_reinit_error:
            logger.error("Calling ray.init() again after it has already been "
                         "called.")
            return
        else:
            raise RuntimeError("Maybe you called ray.init twice by accident? "
                               "This error can be suppressed by passing in "
                               "'ignore_reinit_error=True' or by calling "
                               "'ray.shutdown()' prior to 'ray.init()'.")

    # Convert hostnames to numerical IP address.
    if node_ip_address is not None:
        node_ip_address = services.address_to_ip(node_ip_address)

    raylet_ip_address = node_ip_address

    _internal_config = (json.loads(_internal_config)
                        if _internal_config else {})

    global _global_node
    if redis_address is None:
        # In this case, we need to start a new cluster.
        ray_params = ray.parameter.RayParams(
            redis_address=redis_address,
            redis_port=redis_port,
            node_ip_address=node_ip_address,
            raylet_ip_address=raylet_ip_address,
            object_ref_seed=object_ref_seed,
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
            include_java=include_java,
            include_dashboard=include_dashboard,
            dashboard_host=dashboard_host,
            dashboard_port=dashboard_port,
            memory=memory,
            object_store_memory=object_store_memory,
            redis_max_memory=redis_max_memory,
            plasma_store_socket_name=plasma_store_socket_name,
            raylet_socket_name=raylet_socket_name,
            temp_dir=temp_dir,
            load_code_from_local=load_code_from_local,
            java_worker_options=java_worker_options,
            _internal_config=_internal_config,
            lru_evict=lru_evict,
            enable_object_reconstruction=enable_object_reconstruction)
        # Start the Ray processes. We set shutdown_at_exit=False because we
        # shutdown the node in the ray.shutdown call that happens in the atexit
        # handler. We still spawn a reaper process in case the atexit handler
        # isn't called.
        _global_node = ray.node.Node(
            head=True,
            shutdown_at_exit=False,
            spawn_reaper=True,
            ray_params=ray_params)
    else:
        # In this case, we are connecting to an existing cluster.
        if num_cpus is not None or num_gpus is not None:
            raise ValueError(
                "When connecting to an existing cluster, num_cpus "
                "and num_gpus must not be provided.")
        if resources is not None:
            raise ValueError("When connecting to an existing cluster, "
                             "resources must not be provided.")
        if num_redis_shards is not None:
            raise ValueError("When connecting to an existing cluster, "
                             "num_redis_shards must not be provided.")
        if redis_max_clients is not None:
            raise ValueError("When connecting to an existing cluster, "
                             "redis_max_clients must not be provided.")
        if memory is not None:
            raise ValueError("When connecting to an existing cluster, "
                             "memory must not be provided.")
        if object_store_memory is not None:
            raise ValueError("When connecting to an existing cluster, "
                             "object_store_memory must not be provided.")
        if redis_max_memory is not None:
            raise ValueError("When connecting to an existing cluster, "
                             "redis_max_memory must not be provided.")
        if plasma_directory is not None:
            raise ValueError("When connecting to an existing cluster, "
                             "plasma_directory must not be provided.")
        if huge_pages:
            raise ValueError("When connecting to an existing cluster, "
                             "huge_pages must not be provided.")
        if temp_dir is not None:
            raise ValueError("When connecting to an existing cluster, "
                             "temp_dir must not be provided.")
        if plasma_store_socket_name is not None:
            raise ValueError("When connecting to an existing cluster, "
                             "plasma_store_socket_name must not be provided.")
        if raylet_socket_name is not None:
            raise ValueError("When connecting to an existing cluster, "
                             "raylet_socket_name must not be provided.")
        if java_worker_options is not None:
            raise ValueError("When connecting to an existing cluster, "
                             "java_worker_options must not be provided.")
        if _internal_config is not None and len(_internal_config) != 0:
            raise ValueError("When connecting to an existing cluster, "
                             "_internal_config must not be provided.")
        if lru_evict:
            raise ValueError("When connecting to an existing cluster, "
                             "lru_evict must not be provided.")
        if enable_object_reconstruction:
            raise ValueError(
                "When connecting to an existing cluster, "
                "enable_object_reconstruction must not be provided.")

        # In this case, we only need to connect the node.
        ray_params = ray.parameter.RayParams(
            node_ip_address=node_ip_address,
            raylet_ip_address=raylet_ip_address,
            redis_address=redis_address,
            redis_password=redis_password,
            object_ref_seed=object_ref_seed,
            temp_dir=temp_dir,
            load_code_from_local=load_code_from_local,
            _internal_config=_internal_config,
            lru_evict=lru_evict,
            enable_object_reconstruction=enable_object_reconstruction)
        _global_node = ray.node.Node(
            ray_params,
            head=False,
            shutdown_at_exit=False,
            spawn_reaper=False,
            connect_only=True)

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

    # We need to destruct the core worker here because after this function,
    # we will tear down any processes spawned by ray.init() and the background
    # IO thread in the core worker doesn't currently handle that gracefully.
    if hasattr(global_worker, "core_worker"):
        del global_worker.core_worker

    # Disconnect global state from GCS.
    ray.state.state.disconnect()

    # Shut down the Ray processes.
    global _global_node
    if _global_node is not None:
        _global_node.kill_all_processes(check_alive=False, allow_graceful=True)
        _global_node = None

    # TODO(rkn): Instead of manually resetting some of the worker fields, we
    # should simply set "global_worker" to equal "None" or something like that.
    global_worker.set_mode(None)
    global_worker._post_get_hooks = []


atexit.register(shutdown, True)


# TODO(edoakes): this should only be set in the driver.
def sigterm_handler(signum, frame):
    sys.exit(signum)


try:
    ray.utils.set_sigterm_handler(sigterm_handler)
except ValueError:
    logger.warning("Failed to set SIGTERM handler, processes might"
                   "not be cleaned up properly on exit.")

# Define a custom excepthook so that if the driver exits with an exception, we
# can push that exception to Redis.
normal_excepthook = sys.excepthook


def custom_excepthook(type, value, tb):
    # If this is a driver, push the exception to GCS worker table.
    if global_worker.mode == SCRIPT_MODE:
        error_message = "".join(traceback.format_tb(tb))
        worker_id = global_worker.worker_id
        worker_type = ray.gcs_utils.DRIVER
        worker_info = {"exception": error_message}

        ray.state.state.add_worker(worker_id, worker_type, worker_info)
    # Call the normal excepthook.
    normal_excepthook(type, value, tb)


sys.excepthook = custom_excepthook

# The last time we raised a TaskError in this process. We use this value to
# suppress redundant error messages pushed from the workers.
last_task_error_raise_time = 0

# The max amount of seconds to wait before printing out an uncaught error.
UNCAUGHT_ERROR_GRACE_PERIOD = 5


def _set_log_file(file_name, worker_pid, old_obj, setter_func):
    # Line-buffer the output (mode 1).
    f = create_and_init_new_worker_log(file_name, worker_pid)

    # TODO (Alex): Python seems to always flush when writing. If that is no
    # longer true, then we need to manually flush the old buffer.
    # old_obj.flush()

    # TODO (Alex): Flush the c/c++ userspace buffers if necessary.
    # `fflush(stdout); cout.flush();`

    fileno = old_obj.fileno()

    # C++ logging requires redirecting the stdout file descriptor. Note that
    # dup2 will automatically close the old file descriptor before overriding
    # it.
    os.dup2(f.fileno(), fileno)

    # We also manually set sys.stdout and sys.stderr because that seems to
    # have an effect on the output buffering. Without doing this, stdout
    # and stderr are heavily buffered resulting in seemingly lost logging
    # statements. We never want to close the stdout file descriptor, dup2 will
    # close it when necessary and we don't want python's GC to close it.
    setter_func(open_log(fileno, closefd=False))

    return os.path.abspath(f.name)


def set_log_file(stdout_name, stderr_name):
    """Sets up logging for the current worker, creating the (fd backed) file and
    flushing buffers as is necessary.

    Args:
        stdout_name (str): The file name that stdout should be written to.
        stderr_name(str): The file name that stderr should be written to.

    Returns:
        (tuple) The absolute paths of the files that stdout and stderr will be
    written to.

    """
    stdout_path = ""
    stderr_path = ""
    worker_pid = os.getpid()

    # lambda cannot contain assignment
    def stdout_setter(x):
        sys.stdout = x

    def stderr_setter(x):
        sys.stderr = x

    if stdout_name:
        _set_log_file(stdout_name, worker_pid, sys.stdout, stdout_setter)

    # The stderr case should be analogous to the stdout case
    if stderr_name:
        _set_log_file(stderr_name, worker_pid, sys.stderr, stderr_setter)

    return stdout_path, stderr_path


def print_logs(redis_client, threads_stopped, job_id):
    """Prints log messages from workers on all of the nodes.

    Args:
        redis_client: A client to the primary Redis shard.
        threads_stopped (threading.Event): A threading event used to signal to
            the thread that it should exit.
        job_id (JobID): The id of the driver's job

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
            if (num_consecutive_messages_received % 100 == 0
                    and num_consecutive_messages_received > 0):
                logger.warning(
                    "The driver may not be able to keep up with the "
                    "stdout/stderr of the workers. To avoid forwarding logs "
                    "to the driver, use 'ray.init(log_to_driver=False)'.")

            data = json.loads(ray.utils.decode(msg["data"]))

            # Don't show logs from other drivers.
            if data["job"] and ray.utils.binary_to_hex(
                    job_id.binary()) != data["job"]:
                continue

            print_file = sys.stderr if data["is_err"] else sys.stdout

            def color_for(data):
                if data["pid"] == "raylet":
                    return colorama.Fore.YELLOW
                else:
                    return colorama.Fore.CYAN

            if data["ip"] == localhost:
                for line in data["lines"]:
                    print(
                        "{}{}(pid={}){} {}".format(
                            colorama.Style.DIM, color_for(data), data["pid"],
                            colorama.Style.RESET_ALL, line),
                        file=print_file)
            else:
                for line in data["lines"]:
                    print(
                        "{}{}(pid={}, ip={}){} {}".format(
                            colorama.Style.DIM, color_for(data), data["pid"],
                            data["ip"], colorama.Style.RESET_ALL, line),
                        file=print_file)

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
                    JobID.nil().binary(),
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
    try:
        if not faulthandler.is_enabled():
            faulthandler.enable(all_threads=False)
    except io.UnsupportedOperation:
        pass  # ignore

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

    if mode is not SCRIPT_MODE and setproctitle:
        setproctitle.setproctitle("ray::IDLE")

    if not isinstance(job_id, JobID):
        raise TypeError("The type of given job id must be JobID.")

    # All workers start out as non-actors. A worker can be turned into an actor
    # after it is created.
    worker.node = node
    worker.set_mode(mode)

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

    driver_name = ""
    log_stdout_file_path = ""
    log_stderr_file_path = ""
    if mode == SCRIPT_MODE:
        import __main__ as main
        driver_name = (main.__file__
                       if hasattr(main, "__file__") else "INTERACTIVE MODE")
    elif mode == WORKER_MODE:
        # Check the RedirectOutput key in Redis and based on its value redirect
        # worker output and error to their own files.
        # This key is set in services.py when Redis is started.
        redirect_worker_output_val = worker.redis_client.get("RedirectOutput")
        if (redirect_worker_output_val is not None
                and int(redirect_worker_output_val) == 1):
            log_stdout_file_name, log_stderr_file_name = (
                node.get_job_redirected_log_file(worker.worker_id))
            try:
                log_stdout_file_path, log_stderr_file_path = \
                    set_log_file(log_stdout_file_name, log_stderr_file_name)
            except IOError:
                raise IOError(
                    "Workers must be able to redirect their output at"
                    "the file descriptor level.")
    elif not LOCAL_MODE:
        raise ValueError(
            "Invalid worker mode. Expected DRIVER, WORKER or LOCAL.")

    # TODO (Alex): `current_logging_job` tracks the current job so that we know
    # when to switch log files. If all logging functionaility was moved to c++,
    # the functionaility in `_raylet.pyx::switch_worker_log_if_necessary` could
    # be moved to `CoreWorker::SetCurrentTaskId()`.
    worker.current_logging_job_id = None
    redis_address, redis_port = node.redis_address.split(":")
    gcs_options = ray._raylet.GcsClientOptions(
        redis_address,
        int(redis_port),
        node.redis_password,
    )

    worker.core_worker = ray._raylet.CoreWorker(
        (mode == SCRIPT_MODE or mode == LOCAL_MODE),
        node.plasma_store_socket_name,
        node.raylet_socket_name,
        job_id,
        gcs_options,
        node.get_logs_dir_path(),
        node.node_ip_address,
        node.node_manager_port,
        node.raylet_ip_address,
        (mode == LOCAL_MODE),
        driver_name,
        log_stdout_file_path,
        log_stderr_file_path,
    )

    # Create an object for interfacing with the global state.
    # Note, global state should be intialized after `CoreWorker`, because it
    # will use glog, which is intialized in `CoreWorker`.
    ray.state.state._initialize_global_state(
        node.redis_address, redis_password=node.redis_password)

    if driver_object_store_memory is not None:
        worker.core_worker.set_object_store_client_options(
            "ray_driver_{}".format(os.getpid()), driver_object_store_memory)

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
                args=(worker.redis_client, worker.threads_stopped, job_id))
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
    try:
        ray_actor = ray.actor
    except AttributeError:
        ray_actor = None  # This can occur during program termination
    if ray_actor is not None:
        ray_actor.ActorClassMethodMetadata.reset_cache()


@contextmanager
def _changeproctitle(title, next_title):
    setproctitle.setproctitle(title)
    try:
        yield
    finally:
        setproctitle.setproctitle(next_title)


def register_custom_serializer(cls,
                               serializer,
                               deserializer,
                               use_pickle=False,
                               use_dict=False,
                               class_id=None):
    """Registers custom functions for efficient object serialization.

    The serializer and deserializer are used when transferring objects of
    `cls` across processes and nodes. This can be significantly faster than
    the Ray default fallbacks. Wraps `register_custom_serializer` underneath.

    Args:
        cls (type): The class that ray should use this custom serializer for.
        serializer: The custom serializer that takes in a cls instance and
            outputs a serialized representation. use_pickle and use_dict
            must be False if provided.
        deserializer: The custom deserializer that takes in a serialized
            representation of the cls and outputs a cls instance. use_pickle
            and use_dict must be False if provided.
        use_pickle: Deprecated.
        use_dict: Deprecated.
        class_id (str): Unique ID of the class. Autogenerated if None.
    """
    worker = global_worker
    worker.check_connected()

    if use_pickle:
        raise DeprecationWarning(
            "`use_pickle` is no longer a valid parameter and will be removed "
            "in future versions of Ray. If this breaks your application, "
            "see `SerializationContext.register_custom_serializer`.")
    if use_dict:
        raise DeprecationWarning(
            "`use_pickle` is no longer a valid parameter and will be removed "
            "in future versions of Ray. If this breaks your application, "
            "see `SerializationContext.register_custom_serializer`.")
    assert serializer is not None and deserializer is not None
    context = global_worker.get_serialization_context()
    context.register_custom_serializer(
        cls, serializer, deserializer, class_id=class_id)


def show_in_webui(message, key="", dtype="text"):
    """Display message in dashboard.

    Display message for the current task or actor in the dashboard.
    For example, this can be used to display the status of a long-running
    computation.

    Args:
        message (str): Message to be displayed.
        key (str): The key name for the message. Multiple message under
            different keys will be displayed at the same time. Messages
            under the same key will be overriden.
        data_type (str): The type of message for rendering. One of the
            following: text, html.
    """
    worker = global_worker
    worker.check_connected()

    acceptable_dtypes = {"text", "html"}
    assert dtype in acceptable_dtypes, "dtype accepts only: {}".format(
        acceptable_dtypes)

    message_wrapped = {"message": message, "dtype": dtype}
    message_encoded = json.dumps(message_wrapped).encode()

    worker.core_worker.set_webui_display(key.encode(), message_encoded)


# Global varaible to make sure we only send out the warning once
blocking_get_inside_async_warned = False


def get(object_refs, timeout=None):
    """Get a remote object or a list of remote objects from the object store.

    This method blocks until the object corresponding to the object ref is
    available in the local object store. If this object is not in the local
    object store, it will be shipped from an object store that has it (once the
    object has been created). If object_refs is a list, then the objects
    corresponding to each object in the list will be returned.

    This method will issue a warning if it's running inside async context,
    you can use ``await object_ref`` instead of ``ray.get(object_ref)``. For
    a list of object refs, you can use ``await asyncio.gather(*object_refs)``.

    Args:
        object_refs: Object ref of the object to get or a list of object refs
            to get.
        timeout (Optional[float]): The maximum amount of time in seconds to
            wait before returning.

    Returns:
        A Python object or a list of Python objects.

    Raises:
        RayTimeoutError: A RayTimeoutError is raised if a timeout is set and
            the get takes longer than timeout to return.
        Exception: An exception is raised if the task that created the object
            or that created one of the objects raised an exception.
    """
    worker = global_worker
    worker.check_connected()

    if hasattr(
            worker,
            "core_worker") and worker.core_worker.current_actor_is_asyncio():
        global blocking_get_inside_async_warned
        if not blocking_get_inside_async_warned:
            logger.debug("Using blocking ray.get inside async actor. "
                         "This blocks the event loop. Please use `await` "
                         "on object ref with asyncio.gather if you want to "
                         "yield execution to the event loop instead.")
            blocking_get_inside_async_warned = True

    with profiling.profile("ray.get"):
        is_individual_id = isinstance(object_refs, ray.ObjectRef)
        if is_individual_id:
            object_refs = [object_refs]

        if not isinstance(object_refs, list):
            raise ValueError("'object_refs' must either be an object ref "
                             "or a list of object refs.")

        global last_task_error_raise_time
        # TODO(ujvl): Consider how to allow user to retrieve the ready objects.
        values = worker.get_objects(object_refs, timeout=timeout)
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
            values = post_processor(object_refs, values)

        if is_individual_id:
            values = values[0]
        return values


def put(value, weakref=False):
    """Store an object in the object store.

    The object may not be evicted while a reference to the returned ID exists.

    Args:
        value: The Python object to be stored.
        weakref: If set, allows the object to be evicted while a reference
            to the returned ID exists. You might want to set this if putting
            a lot of objects that you might not need in the future.
            It allows Ray to more aggressively reclaim memory.

    Returns:
        The object ref assigned to this value.
    """
    worker = global_worker
    worker.check_connected()
    with profiling.profile("ray.put"):
        try:
            object_ref = worker.put_object(value, pin_object=not weakref)
        except ObjectStoreFullError:
            logger.info(
                "Put failed since the value was either too large or the "
                "store was full of pinned objects.")
            raise
        return object_ref


# Global variable to make sure we only send out the warning once.
blocking_wait_inside_async_warned = False


def wait(object_refs, num_returns=1, timeout=None):
    """Return a list of IDs that are ready and a list of IDs that are not.

    If timeout is set, the function returns either when the requested number of
    IDs are ready or when the timeout is reached, whichever occurs first. If it
    is not set, the function simply waits until that number of objects is ready
    and returns that exact number of object refs.

    This method returns two lists. The first list consists of object refs that
    correspond to objects that are available in the object store. The second
    list corresponds to the rest of the object refs (which may or may not be
    ready).

    Ordering of the input list of object refs is preserved. That is, if A
    precedes B in the input list, and both are in the ready list, then A will
    precede B in the ready list. This also holds true if A and B are both in
    the remaining list.

    This method will issue a warning if it's running inside an async context.
    Instead of ``ray.wait(object_refs)``, you can use
    ``await asyncio.wait(object_refs)``.

    Args:
        object_refs (List[ObjectRef]): List of object refs for objects that may
            or may not be ready. Note that these IDs must be unique.
        num_returns (int): The number of object refs that should be returned.
        timeout (float): The maximum amount of time in seconds to wait before
            returning.

    Returns:
        A list of object refs that are ready and a list of the remaining object
        IDs.
    """
    worker = global_worker

    if hasattr(worker,
               "core_worker") and worker.core_worker.current_actor_is_asyncio(
               ) and timeout != 0:
        global blocking_wait_inside_async_warned
        if not blocking_wait_inside_async_warned:
            logger.debug("Using blocking ray.wait inside async method. "
                         "This blocks the event loop. Please use `await` "
                         "on object ref with asyncio.wait. ")
            blocking_wait_inside_async_warned = True

    if isinstance(object_refs, ObjectRef):
        raise TypeError(
            "wait() expected a list of ray.ObjectRef, got a single "
            "ray.ObjectRef")

    if not isinstance(object_refs, list):
        raise TypeError(
            "wait() expected a list of ray.ObjectRef, got {}".format(
                type(object_refs)))

    if timeout is not None and timeout < 0:
        raise ValueError("The 'timeout' argument must be nonnegative. "
                         "Received {}".format(timeout))

    for object_ref in object_refs:
        if not isinstance(object_ref, ObjectRef):
            raise TypeError("wait() expected a list of ray.ObjectRef, "
                            "got list containing {}".format(type(object_ref)))

    worker.check_connected()
    # TODO(swang): Check main thread.
    with profiling.profile("ray.wait"):

        # TODO(rkn): This is a temporary workaround for
        # https://github.com/ray-project/ray/issues/997. However, it should be
        # fixed in Arrow instead of here.
        if len(object_refs) == 0:
            return [], []

        if len(object_refs) != len(set(object_refs)):
            raise ValueError("Wait requires a list of unique object refs.")
        if num_returns <= 0:
            raise ValueError(
                "Invalid number of objects to return %d." % num_returns)
        if num_returns > len(object_refs):
            raise ValueError("num_returns cannot be greater than the number "
                             "of objects provided to ray.wait.")

        timeout = timeout if timeout is not None else 10**6
        timeout_milliseconds = int(timeout * 1000)
        ready_ids, remaining_ids = worker.core_worker.wait(
            object_refs,
            num_returns,
            timeout_milliseconds,
            worker.current_task_id,
        )
        return ready_ids, remaining_ids


def get_actor(name):
    """Get a handle to a detached actor.

    Gets a handle to a detached actor with the given name. The actor must
    have been created with Actor.options(name="name").remote().

    Returns:
        ActorHandle to the actor.

    Raises:
        ValueError if the named actor does not exist.
    """
    return ray.util.named_actors._get_actor(name)


def kill(actor, no_restart=True):
    """Kill an actor forcefully.

    This will interrupt any running tasks on the actor, causing them to fail
    immediately. Any atexit handlers installed in the actor will still be run.

    If you want to kill the actor but let pending tasks finish,
    you can call ``actor.__ray_terminate__.remote()`` instead to queue a
    termination task.

    If the actor is a detached actor, subsequent calls to get its handle via
    ray.get_actor will fail.

    Args:
        actor (ActorHandle): Handle to the actor to kill.
        no_restart (bool): Whether or not this actor should be restarted if
            it's a restartable actor.
    """
    if not isinstance(actor, ray.actor.ActorHandle):
        raise ValueError("ray.kill() only supported for actors. "
                         "Got: {}.".format(type(actor)))
    worker = ray.worker.global_worker
    worker.check_connected()
    worker.core_worker.kill_actor(actor._ray_actor_id, no_restart)


def cancel(object_ref, force=False):
    """Cancels a task according to the following conditions.

    If the specified task is pending execution, it will not be executed. If
    the task is currently executing, the behavior depends on the ``force``
    flag. When ``force=False``, a KeyboardInterrupt will be raised in Python
    and when ``force=True``, the executing the task will immediately exit. If
    the task is already finished, nothing will happen.

    Only non-actor tasks can be canceled. Canceled tasks will not be
    retried (max_retries will not be respected).

    Calling ray.get on a canceled task will raise a RayCancellationError.

    Args:
        object_ref (ObjectRef): ObjectRef returned by the task
            that should be canceled.
        force (boolean): Whether to force-kill a running task by killing
            the worker that is running the task.
    Raises:
        TypeError: This is also raised for actor tasks.
    """
    worker = ray.worker.global_worker
    worker.check_connected()

    if not isinstance(object_ref, ray.ObjectRef):
        raise TypeError(
            "ray.cancel() only supported for non-actor object refs. "
            "Got: {}.".format(type(object_ref)))
    return worker.core_worker.cancel_task(object_ref, force)


def _mode(worker=global_worker):
    """This is a wrapper around worker.mode.

    We use this wrapper so that in the remote decorator, we can call _mode()
    instead of worker.mode. The difference is that when we attempt to
    serialize remote functions, we don't attempt to serialize the worker
    object, which cannot be serialized.
    """
    return worker.mode


def make_decorator(num_return_vals=None,
                   num_cpus=None,
                   num_gpus=None,
                   memory=None,
                   object_store_memory=None,
                   resources=None,
                   max_calls=None,
                   max_retries=None,
                   max_restarts=None,
                   max_task_retries=None,
                   worker=None):
    def decorator(function_or_class):
        if (inspect.isfunction(function_or_class)
                or is_cython(function_or_class)):
            # Set the remote function default resources.
            if max_restarts is not None:
                raise ValueError("The keyword 'max_restarts' is not "
                                 "allowed for remote functions.")
            if max_task_retries is not None:
                raise ValueError("The keyword 'max_task_retries' is not "
                                 "allowed for remote functions.")

            return ray.remote_function.RemoteFunction(
                Language.PYTHON, function_or_class, None, num_cpus, num_gpus,
                memory, object_store_memory, resources, num_return_vals,
                max_calls, max_retries)

        if inspect.isclass(function_or_class):
            if num_return_vals is not None:
                raise TypeError("The keyword 'num_return_vals' is not "
                                "allowed for actors.")
            if max_calls is not None:
                raise TypeError("The keyword 'max_calls' is not "
                                "allowed for actors.")

            return ray.actor.make_actor(function_or_class, num_cpus, num_gpus,
                                        memory, object_store_memory, resources,
                                        max_restarts, max_task_retries)

        raise TypeError("The @ray.remote decorator must be applied to "
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
        class Foo:
            def method(self):
                return 1

    It can also be used with specific keyword arguments:

    * **num_return_vals:** This is only for *remote functions*. It specifies
      the number of object refs returned by the remote function invocation.
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
    * **max_restarts**: Only for *actors*. This specifies the maximum
      number of times that the actor should be restarted when it dies
      unexpectedly. The minimum valid value is 0 (default), which indicates
      that the actor doesn't need to be restarted. A value of -1
      indicates that an actor should be restarted indefinitely.
    * **max_task_retries**: Only for *actors*. How many times to retry an actor
      task if the task fails due to a system error, e.g., the actor has died.
      If set to -1, the system will retry the failed task until the task
      succeeds, or the actor has reached its max_restarts limit. If set to n >
      0, the system will retry the failed task up to n times, after which the
      task will throw a `RayActorError` exception upon `ray.get`. Note that
      Python exceptions are not considered system errors and will not trigger
      retries.
    * **max_retries**: Only for *remote functions*. This specifies the maximum
      number of times that the remote function should be rerun when the worker
      process executing it crashes unexpectedly. The minimum valid value is 0,
      the default is 4 (default), and a value of -1 indicates infinite retries.

    This can be done as follows:

    .. code-block:: python

        @ray.remote(num_gpus=1, max_calls=1, num_return_vals=2)
        def f():
            return 1, 2

        @ray.remote(num_cpus=2, resources={"CustomResource": 1})
        class Foo:
            def method(self):
                return 1

    Remote task and actor objects returned by @ray.remote can also be
    dynamically modified with the same arguments as above using
    ``.options()`` as follows:

    .. code-block:: python

        @ray.remote(num_gpus=1, max_calls=1, num_return_vals=2)
        def f():
            return 1, 2
        g = f.options(num_gpus=2, max_calls=None)

        @ray.remote(num_cpus=2, resources={"CustomResource": 1})
        class Foo:
            def method(self):
                return 1
        Bar = Foo.options(num_cpus=1, resources=None)

    Running remote actors will be terminated when the actor handle to them
    in Python is deleted, which will cause them to complete any outstanding
    work and then shut down. If you want to kill them immediately, you can
    also call ``ray.kill(actor)``.
    """
    worker = global_worker

    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # This is the case where the decorator is just @ray.remote.
        return make_decorator(worker=worker)(args[0])

    # Parse the keyword arguments from the decorator.
    error_string = ("The @ray.remote decorator must be applied either "
                    "with no arguments and no parentheses, for example "
                    "'@ray.remote', or it must be applied using some of "
                    "the arguments 'num_return_vals', 'num_cpus', 'num_gpus', "
                    "'memory', 'object_store_memory', 'resources', "
                    "'max_calls', or 'max_restarts', like "
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
            "max_restarts",
            "max_task_retries",
            "max_retries",
        ], error_string

    num_cpus = kwargs["num_cpus"] if "num_cpus" in kwargs else None
    num_gpus = kwargs["num_gpus"] if "num_gpus" in kwargs else None
    resources = kwargs.get("resources")
    if not isinstance(resources, dict) and resources is not None:
        raise TypeError("The 'resources' keyword argument must be a "
                        "dictionary, but received type {}.".format(
                            type(resources)))
    if resources is not None:
        assert "CPU" not in resources, "Use the 'num_cpus' argument."
        assert "GPU" not in resources, "Use the 'num_gpus' argument."

    # Handle other arguments.
    num_return_vals = kwargs.get("num_return_vals")
    max_calls = kwargs.get("max_calls")
    max_restarts = kwargs.get("max_restarts")
    max_task_retries = kwargs.get("max_task_retries")
    memory = kwargs.get("memory")
    object_store_memory = kwargs.get("object_store_memory")
    max_retries = kwargs.get("max_retries")

    return make_decorator(
        num_return_vals=num_return_vals,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        memory=memory,
        object_store_memory=object_store_memory,
        resources=resources,
        max_calls=max_calls,
        max_restarts=max_restarts,
        max_task_retries=max_task_retries,
        max_retries=max_retries,
        worker=worker)
