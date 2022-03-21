import atexit
import collections
import datetime
import errno
import json
import logging
import os
import random
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback

from typing import Optional, Dict
from collections import defaultdict
from filelock import FileLock

import ray
import ray.ray_constants as ray_constants
import ray._private.services
import ray._private.utils
from ray.internal import storage
from ray._private.gcs_utils import GcsClient
from ray._private.resource_spec import ResourceSpec
from ray._private.utils import try_to_create_directory, try_to_symlink, open_log
import ray._private.usage.usage_lib as ray_usage_lib

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray configures it by default automatically
# using logging.basicConfig in its entry/init points.
logger = logging.getLogger(__name__)

SESSION_LATEST = "session_latest"
NUM_PORT_RETRIES = 40
NUM_REDIS_GET_RETRIES = 20


class Node:
    """An encapsulation of the Ray processes on a single node.

    This class is responsible for starting Ray processes and killing them,
    and it also controls the temp file policy.

    Attributes:
        all_processes (dict): A mapping from process type (str) to a list of
            ProcessInfo objects. All lists have length one except for the Redis
            server list, which has multiple.
    """

    def __init__(
        self,
        ray_params,
        head=False,
        shutdown_at_exit=True,
        spawn_reaper=True,
        connect_only=False,
    ):
        """Start a node.

        Args:
            ray_params (ray.params.RayParams): The parameters to use to
                configure the node.
            head (bool): True if this is the head node, which means it will
                start additional processes like the Redis servers, monitor
                processes, and web UI.
            shutdown_at_exit (bool): If true, spawned processes will be cleaned
                up if this process exits normally.
            spawn_reaper (bool): If true, spawns a process that will clean up
                other spawned processes if this process dies unexpectedly.
            connect_only (bool): If true, connect to the node without starting
                new processes.
        """
        if shutdown_at_exit:
            if connect_only:
                raise ValueError(
                    "'shutdown_at_exit' and 'connect_only' cannot both be true."
                )
            self._register_shutdown_hooks()

        self.head = head
        self.kernel_fate_share = bool(
            spawn_reaper and ray._private.utils.detect_fate_sharing_support()
        )
        self.all_processes = {}
        self.removal_lock = threading.Lock()

        # Set up external Redis when `RAY_REDIS_ADDRESS` is specified.
        redis_address_env = os.environ.get("RAY_REDIS_ADDRESS")
        if ray_params.external_addresses is None and redis_address_env is not None:
            external_redis = redis_address_env.split(",")

            # Reuse primary Redis as Redis shard when there's only one
            # instance provided.
            if len(external_redis) == 1:
                external_redis.append(external_redis[0])
            [primary_redis_ip, port] = external_redis[0].split(":")
            ray._private.services.wait_for_redis_to_start(
                primary_redis_ip, port, password=ray_params.redis_password
            )

            ray_params.external_addresses = external_redis
            ray_params.num_redis_shards = len(external_redis) - 1

        # Try to get node IP address with the parameters.
        if ray_params.node_ip_address:
            node_ip_address = ray_params.node_ip_address
        elif ray_params.redis_address:
            node_ip_address = ray.util.get_node_ip_address(ray_params.redis_address)
        else:
            node_ip_address = ray.util.get_node_ip_address()
        self._node_ip_address = node_ip_address

        if ray_params.raylet_ip_address:
            raylet_ip_address = ray_params.raylet_ip_address
        else:
            raylet_ip_address = node_ip_address

        if raylet_ip_address != node_ip_address and (not connect_only or head):
            raise ValueError(
                "The raylet IP address should only be different than the node "
                "IP address when connecting to an existing raylet; i.e., when "
                "head=False and connect_only=True."
            )
        if (
            ray_params._system_config
            and len(ray_params._system_config) > 0
            and (not head and not connect_only)
        ):
            raise ValueError(
                "System config parameters can only be set on the head node."
            )

        self._raylet_ip_address = raylet_ip_address

        ray_params.update_if_absent(
            include_log_monitor=True,
            resources={},
            temp_dir=ray._private.utils.get_ray_temp_dir(),
            worker_path=os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "workers/default_worker.py"
            ),
            setup_worker_path=os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                f"workers/{ray_constants.SETUP_WORKER_FILENAME}",
            ),
        )

        self._resource_spec = None
        self._localhost = socket.gethostbyname("localhost")
        self._ray_params = ray_params
        self._config = ray_params._system_config or {}

        # Configure log rotation parameters.
        self.max_bytes = int(
            os.getenv("RAY_ROTATION_MAX_BYTES", ray_constants.LOGGING_ROTATE_BYTES)
        )
        self.backup_count = int(
            os.getenv(
                "RAY_ROTATION_BACKUP_COUNT", ray_constants.LOGGING_ROTATE_BACKUP_COUNT
            )
        )

        assert self.max_bytes >= 0
        assert self.backup_count >= 0

        self._redis_address = ray_params.redis_address
        if head:
            ray_params.update_if_absent(num_redis_shards=1)
        self._gcs_address = ray_params.gcs_address
        self._gcs_client = None

        if not self.head:
            self.validate_ip_port(self.address)
            self.get_gcs_client()

        # Register the temp dir.
        if head:
            # date including microsecond
            date_str = datetime.datetime.today().strftime("%Y-%m-%d_%H-%M-%S_%f")
            self.session_name = f"session_{date_str}_{os.getpid()}"
        else:
            session_name = ray._private.utils.internal_kv_get_with_retry(
                self.get_gcs_client(),
                "session_name",
                ray_constants.KV_NAMESPACE_SESSION,
                num_retries=NUM_REDIS_GET_RETRIES,
            )
            self.session_name = ray._private.utils.decode(session_name)
            # setup gcs client
            self.get_gcs_client()

        # Initialize webui url
        if head:
            self._webui_url = None
        else:
            self._webui_url = ray._private.services.get_webui_url_from_internal_kv()

        self._init_temp()

        # Validate and initialize the persistent storage API.
        storage._init_storage(ray_params.storage, is_head=head)

        # If it is a head node, try validating if
        # external storage is configurable.
        if head:
            self.validate_external_storage()

        if connect_only:
            # Get socket names from the configuration.
            self._plasma_store_socket_name = ray_params.plasma_store_socket_name
            self._raylet_socket_name = ray_params.raylet_socket_name

            # If user does not provide the socket name, get it from Redis.
            if (
                self._plasma_store_socket_name is None
                or self._raylet_socket_name is None
                or self._ray_params.node_manager_port is None
            ):
                # Get the address info of the processes to connect to
                # from Redis or GCS.
                node_info = ray._private.services.get_node_to_connect_for_driver(
                    self.redis_address,
                    self.gcs_address,
                    self._raylet_ip_address,
                    redis_password=self.redis_password,
                )
                self._plasma_store_socket_name = node_info.object_store_socket_name
                self._raylet_socket_name = node_info.raylet_socket_name
                self._ray_params.node_manager_port = node_info.node_manager_port
        else:
            # If the user specified a socket name, use it.
            self._plasma_store_socket_name = self._prepare_socket_file(
                self._ray_params.plasma_store_socket_name, default_prefix="plasma_store"
            )
            self._raylet_socket_name = self._prepare_socket_file(
                self._ray_params.raylet_socket_name, default_prefix="raylet"
            )

        self.metrics_agent_port = self._get_cached_port(
            "metrics_agent_port", default_port=ray_params.metrics_agent_port
        )
        self._metrics_export_port = self._get_cached_port(
            "metrics_export_port", default_port=ray_params.metrics_export_port
        )

        ray_params.update_if_absent(
            metrics_agent_port=self.metrics_agent_port,
            metrics_export_port=self._metrics_export_port,
        )

        # Pick a GCS server port.
        if head:
            gcs_server_port = os.getenv(ray_constants.GCS_PORT_ENVIRONMENT_VARIABLE)
            if gcs_server_port:
                ray_params.update_if_absent(gcs_server_port=gcs_server_port)
            if ray_params.gcs_server_port is None or ray_params.gcs_server_port == 0:
                ray_params.gcs_server_port = self._get_cached_port("gcs_server_port")

        if not connect_only and spawn_reaper and not self.kernel_fate_share:
            self.start_reaper_process()
        if not connect_only:
            self._ray_params.update_pre_selected_port()

        # Start processes.
        if head:
            self.start_head_processes()
            # Make sure GCS is up.
            self.get_gcs_client().internal_kv_put(
                b"session_name",
                self.session_name.encode(),
                True,
                ray_constants.KV_NAMESPACE_SESSION,
            )
            self.get_gcs_client().internal_kv_put(
                b"session_dir",
                self._session_dir.encode(),
                True,
                ray_constants.KV_NAMESPACE_SESSION,
            )
            self.get_gcs_client().internal_kv_put(
                b"temp_dir",
                self._temp_dir.encode(),
                True,
                ray_constants.KV_NAMESPACE_SESSION,
            )
            # Add tracing_startup_hook to redis / internal kv manually
            # since internal kv is not yet initialized.
            if ray_params.tracing_startup_hook:
                self.get_gcs_client().internal_kv_put(
                    b"tracing_startup_hook",
                    ray_params.tracing_startup_hook.encode(),
                    True,
                    ray_constants.KV_NAMESPACE_TRACING,
                )

        if not connect_only:
            self.start_ray_processes()
            # we should update the address info after the node has been started
            try:
                ray._private.services.wait_for_node(
                    self.redis_address,
                    self.gcs_address,
                    self._plasma_store_socket_name,
                    self.redis_password,
                )
            except TimeoutError:
                raise Exception(
                    "The current node has not been updated within 30 "
                    "seconds, this could happen because of some of "
                    "the Ray processes failed to startup."
                )
            node_info = ray._private.services.get_node_to_connect_for_driver(
                self.redis_address,
                self.gcs_address,
                self._raylet_ip_address,
                redis_password=self.redis_password,
            )
            self._ray_params.node_manager_port = node_info.node_manager_port

        # Makes sure the Node object has valid addresses after setup.
        self.validate_ip_port(self.address)
        self.validate_ip_port(self.gcs_address)

    @staticmethod
    def validate_ip_port(ip_port):
        """Validates the address is in the ip:port format"""
        _, _, port = ip_port.rpartition(":")
        _ = int(port)

    def check_version_info(self):
        """Check if various Python and Ray version of this process is correct.

        This will be used to detect if workers or drivers are started using
        different versions of Python, or Ray. If the version information
        is not present in KV store, then no check is done.
        Raises:
            Exception: An exception is raised if there is a version mismatch.
        """
        cluster_metadata = ray_usage_lib.get_cluster_metadata(
            self.get_gcs_client(), num_retries=NUM_REDIS_GET_RETRIES
        )
        if cluster_metadata is None:
            return
        true_version_info = (
            cluster_metadata["ray_version"],
            cluster_metadata["python_version"],
        )
        version_info = ray._private.utils.compute_version_info()
        if version_info != true_version_info:
            node_ip_address = ray._private.services.get_node_ip_address()
            error_message = (
                "Version mismatch: The cluster was started with:\n"
                "    Ray: " + true_version_info[0] + "\n"
                "    Python: " + true_version_info[1] + "\n"
                "This process on node " + node_ip_address + " was started with:" + "\n"
                "    Ray: " + version_info[0] + "\n"
                "    Python: " + version_info[1] + "\n"
            )
            if version_info[:2] != true_version_info[:2]:
                raise RuntimeError(error_message)
            else:
                logger.warning(error_message)

    def _register_shutdown_hooks(self):
        # Register the atexit handler. In this case, we shouldn't call sys.exit
        # as we're already in the exit procedure.
        def atexit_handler(*args):
            self.kill_all_processes(check_alive=False, allow_graceful=True)

        atexit.register(atexit_handler)

        # Register the handler to be called if we get a SIGTERM.
        # In this case, we want to exit with an error code (1) after
        # cleaning up child processes.
        def sigterm_handler(signum, frame):
            self.kill_all_processes(check_alive=False, allow_graceful=True)
            sys.exit(1)

        ray._private.utils.set_sigterm_handler(sigterm_handler)

    def _init_temp(self):
        # Create a dictionary to store temp file index.
        self._incremental_dict = collections.defaultdict(lambda: 0)

        if self.head:
            self._temp_dir = self._ray_params.temp_dir
        else:
            temp_dir = ray._private.utils.internal_kv_get_with_retry(
                self.get_gcs_client(),
                "temp_dir",
                ray_constants.KV_NAMESPACE_SESSION,
                num_retries=NUM_REDIS_GET_RETRIES,
            )
            self._temp_dir = ray._private.utils.decode(temp_dir)

        try_to_create_directory(self._temp_dir)

        if self.head:
            self._session_dir = os.path.join(self._temp_dir, self.session_name)
        else:
            session_dir = ray._private.utils.internal_kv_get_with_retry(
                self.get_gcs_client(),
                "session_dir",
                ray_constants.KV_NAMESPACE_SESSION,
                num_retries=NUM_REDIS_GET_RETRIES,
            )
            self._session_dir = ray._private.utils.decode(session_dir)
        session_symlink = os.path.join(self._temp_dir, SESSION_LATEST)

        # Send a warning message if the session exists.
        try_to_create_directory(self._session_dir)
        try_to_symlink(session_symlink, self._session_dir)
        # Create a directory to be used for socket files.
        self._sockets_dir = os.path.join(self._session_dir, "sockets")
        try_to_create_directory(self._sockets_dir)
        # Create a directory to be used for process log files.
        self._logs_dir = os.path.join(self._session_dir, "logs")
        try_to_create_directory(self._logs_dir)
        old_logs_dir = os.path.join(self._logs_dir, "old")
        try_to_create_directory(old_logs_dir)
        # Create a directory to be used for runtime environment.
        self._runtime_env_dir = os.path.join(
            self._session_dir, self._ray_params.runtime_env_dir_name
        )
        try_to_create_directory(self._runtime_env_dir)

    def get_resource_spec(self):
        """Resolve and return the current resource spec for the node."""

        def merge_resources(env_dict, params_dict):
            """Separates special case params and merges two dictionaries, picking from the
            first in the event of a conflict. Also emit a warning on every
            conflict.
            """
            num_cpus = env_dict.pop("CPU", None)
            num_gpus = env_dict.pop("GPU", None)
            memory = env_dict.pop("memory", None)
            object_store_memory = env_dict.pop("object_store_memory", None)

            result = params_dict.copy()
            result.update(env_dict)

            for key in set(env_dict.keys()).intersection(set(params_dict.keys())):
                if params_dict[key] != env_dict[key]:
                    logger.warning(
                        "Autoscaler is overriding your resource:"
                        f"{key}: {params_dict[key]} with {env_dict[key]}."
                    )
            return num_cpus, num_gpus, memory, object_store_memory, result

        if not self._resource_spec:
            env_resources = {}
            env_string = os.getenv(ray_constants.RESOURCES_ENVIRONMENT_VARIABLE)
            if env_string:
                try:
                    env_resources = json.loads(env_string)
                except Exception:
                    logger.exception(f"Failed to load {env_string}")
                    raise
                logger.debug(f"Autoscaler overriding resources: {env_resources}.")
            (
                num_cpus,
                num_gpus,
                memory,
                object_store_memory,
                resources,
            ) = merge_resources(env_resources, self._ray_params.resources)
            self._resource_spec = ResourceSpec(
                self._ray_params.num_cpus if num_cpus is None else num_cpus,
                self._ray_params.num_gpus if num_gpus is None else num_gpus,
                self._ray_params.memory if memory is None else memory,
                self._ray_params.object_store_memory
                if object_store_memory is None
                else object_store_memory,
                resources,
                self._ray_params.redis_max_memory,
            ).resolve(is_head=self.head, node_ip_address=self.node_ip_address)
        return self._resource_spec

    @property
    def node_ip_address(self):
        """Get the IP address of this node."""
        return self._node_ip_address

    @property
    def raylet_ip_address(self):
        """Get the IP address of the raylet that this node connects to."""
        return self._raylet_ip_address

    @property
    def address(self):
        """Get the address for bootstrapping, e.g. the address to pass to
        `ray start` or `ray.int()` to start worker nodes, that has been
        converted to ip:port format.
        """
        return self._gcs_address

    @property
    def gcs_address(self):
        """Get the gcs address."""
        assert self._gcs_address is not None, "Gcs address is not set"
        return self._gcs_address

    @property
    def redis_address(self):
        """Get the cluster Redis address."""
        return self._redis_address

    @property
    def redis_password(self):
        """Get the cluster Redis password"""
        return self._ray_params.redis_password

    @property
    def object_ref_seed(self):
        """Get the seed for deterministic generation of object refs"""
        return self._ray_params.object_ref_seed

    @property
    def plasma_store_socket_name(self):
        """Get the node's plasma store socket name."""
        return self._plasma_store_socket_name

    @property
    def unique_id(self):
        """Get a unique identifier for this node."""
        return f"{self.node_ip_address}:{self._plasma_store_socket_name}"

    @property
    def webui_url(self):
        """Get the cluster's web UI url."""
        return self._webui_url

    @property
    def raylet_socket_name(self):
        """Get the node's raylet socket name."""
        return self._raylet_socket_name

    @property
    def node_manager_port(self):
        """Get the node manager's port."""
        return self._ray_params.node_manager_port

    @property
    def metrics_export_port(self):
        """Get the port that exposes metrics"""
        return self._metrics_export_port

    @property
    def logging_config(self):
        """Get the logging config of the current node."""
        return {
            "log_rotation_max_bytes": self.max_bytes,
            "log_rotation_backup_count": self.backup_count,
        }

    @property
    def address_info(self):
        """Get a dictionary of addresses."""
        return {
            "node_ip_address": self._node_ip_address,
            "raylet_ip_address": self._raylet_ip_address,
            "redis_address": self.redis_address,
            "object_store_address": self._plasma_store_socket_name,
            "raylet_socket_name": self._raylet_socket_name,
            "webui_url": self._webui_url,
            "session_dir": self._session_dir,
            "metrics_export_port": self._metrics_export_port,
            "gcs_address": self.gcs_address,
            "address": self.address,
        }

    def is_head(self):
        return self.head

    def create_redis_client(self):
        """Create a redis client."""
        return ray._private.services.create_redis_client(
            self.redis_address, self._ray_params.redis_password
        )

    def get_gcs_client(self):
        if self._gcs_client is None:
            for _ in range(NUM_REDIS_GET_RETRIES):
                gcs_address = None
                last_ex = None
                try:
                    gcs_address = self.gcs_address
                    self._gcs_client = GcsClient(address=gcs_address)
                except Exception:
                    last_ex = traceback.format_exc()
                    logger.debug(f"Connecting to GCS: {last_ex}")
                    time.sleep(1)
            assert self._gcs_client is not None, (
                f"Failed to connect to GCS at address={gcs_address}. "
                f"Last exception: {last_ex}"
            )
            ray.experimental.internal_kv._initialize_internal_kv(self._gcs_client)
        return self._gcs_client

    def get_temp_dir_path(self):
        """Get the path of the temporary directory."""
        return self._temp_dir

    def get_runtime_env_dir_path(self):
        """Get the path of the runtime env."""
        return self._runtime_env_dir

    def get_session_dir_path(self):
        """Get the path of the session directory."""
        return self._session_dir

    def get_logs_dir_path(self):
        """Get the path of the log files directory."""
        return self._logs_dir

    def get_sockets_dir_path(self):
        """Get the path of the sockets directory."""
        return self._sockets_dir

    def _make_inc_temp(self, suffix="", prefix="", directory_name=None):
        """Return an incremental temporary file name. The file is not created.

        Args:
            suffix (str): The suffix of the temp file.
            prefix (str): The prefix of the temp file.
            directory_name (str) : The base directory of the temp file.

        Returns:
            A string of file name. If there existing a file having
                the same name, the returned name will look like
                "{directory_name}/{prefix}.{unique_index}{suffix}"
        """
        if directory_name is None:
            directory_name = ray._private.utils.get_ray_temp_dir()
        directory_name = os.path.expanduser(directory_name)
        index = self._incremental_dict[suffix, prefix, directory_name]
        # `tempfile.TMP_MAX` could be extremely large,
        # so using `range` in Python2.x should be avoided.
        while index < tempfile.TMP_MAX:
            if index == 0:
                filename = os.path.join(directory_name, prefix + suffix)
            else:
                filename = os.path.join(
                    directory_name, prefix + "." + str(index) + suffix
                )
            index += 1
            if not os.path.exists(filename):
                # Save the index.
                self._incremental_dict[suffix, prefix, directory_name] = index
                return filename

        raise FileExistsError(errno.EEXIST, "No usable temporary filename found")

    def should_redirect_logs(self):
        redirect_output = self._ray_params.redirect_output
        if redirect_output is None:
            # Fall back to stderr redirect environment variable.
            redirect_output = (
                os.environ.get(
                    ray_constants.LOGGING_REDIRECT_STDERR_ENVIRONMENT_VARIABLE
                )
                != "1"
            )
        return redirect_output

    def get_log_file_handles(self, name, unique=False):
        """Open log files with partially randomized filenames, returning the
        file handles. If output redirection has been disabled, no files will
        be opened and `(None, None)` will be returned.

        Args:
            name (str): descriptive string for this log file.
            unique (bool): if true, a counter will be attached to `name` to
                ensure the returned filename is not already used.

        Returns:
            A tuple of two file handles for redirecting (stdout, stderr), or
            `(None, None)` if output redirection is disabled.
        """
        if not self.should_redirect_logs():
            return None, None

        log_stdout, log_stderr = self._get_log_file_names(name, unique=unique)
        return open_log(log_stdout), open_log(log_stderr)

    def _get_log_file_names(self, name, unique=False):
        """Generate partially randomized filenames for log files.

        Args:
            name (str): descriptive string for this log file.
            unique (bool): if true, a counter will be attached to `name` to
                ensure the returned filename is not already used.

        Returns:
            A tuple of two file names for redirecting (stdout, stderr).
        """

        if unique:
            log_stdout = self._make_inc_temp(
                suffix=".out", prefix=name, directory_name=self._logs_dir
            )
            log_stderr = self._make_inc_temp(
                suffix=".err", prefix=name, directory_name=self._logs_dir
            )
        else:
            log_stdout = os.path.join(self._logs_dir, f"{name}.out")
            log_stderr = os.path.join(self._logs_dir, f"{name}.err")
        return log_stdout, log_stderr

    def _get_unused_port(self, allocated_ports=None):
        if allocated_ports is None:
            allocated_ports = set()

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("", 0))
        port = s.getsockname()[1]

        # Try to generate a port that is far above the 'next available' one.
        # This solves issue #8254 where GRPC fails because the port assigned
        # from this method has been used by a different process.
        for _ in range(NUM_PORT_RETRIES):
            new_port = random.randint(port, 65535)
            if new_port in allocated_ports:
                # This port is allocated for other usage already,
                # so we shouldn't use it even if it's not in use right now.
                continue
            new_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                new_s.bind(("", new_port))
            except OSError:
                new_s.close()
                continue
            s.close()
            new_s.close()
            return new_port
        logger.error("Unable to succeed in selecting a random port.")
        s.close()
        return port

    def _prepare_socket_file(self, socket_path, default_prefix):
        """Prepare the socket file for raylet and plasma.

        This method helps to prepare a socket file.
        1. Make the directory if the directory does not exist.
        2. If the socket file exists, do nothing (this just means we aren't the
           first worker on the node).

        Args:
            socket_path (string): the socket file to prepare.
        """
        result = socket_path
        is_mac = sys.platform.startswith("darwin")
        if sys.platform == "win32":
            if socket_path is None:
                result = f"tcp://{self._localhost}" f":{self._get_unused_port()}"
        else:
            if socket_path is None:
                result = self._make_inc_temp(
                    prefix=default_prefix, directory_name=self._sockets_dir
                )
            else:
                try_to_create_directory(os.path.dirname(socket_path))

            # Check socket path length to make sure it's short enough
            maxlen = (104 if is_mac else 108) - 1  # sockaddr_un->sun_path
            if len(result.split("://", 1)[-1].encode("utf-8")) > maxlen:
                raise OSError(
                    f"AF_UNIX path length cannot exceed {maxlen} bytes: {result!r}"
                )
        return result

    def _get_cached_port(
        self, port_name: str, default_port: Optional[int] = None
    ) -> int:
        """Get a port number from a cache on this node.

        Different driver processes on a node should use the same ports for
        some purposes, e.g. exporting metrics.  This method returns a port
        number for the given port name and caches it in a file.  If the
        port isn't already cached, an unused port is generated and cached.

        Args:
            port_name (str): the name of the port, e.g. metrics_export_port
            default_port (Optional[int]): The port to return and cache if no
            port has already been cached for the given port_name.  If None, an
            unused port is generated and cached.
        Returns:
            port (int): the port number.
        """
        file_path = os.path.join(self.get_session_dir_path(), "ports_by_node.json")

        # Maps a Node.unique_id to a dict that maps port names to port numbers.
        ports_by_node: Dict[str, Dict[str, int]] = defaultdict(dict)

        with FileLock(file_path + ".lock"):
            if not os.path.exists(file_path):
                with open(file_path, "w") as f:
                    json.dump({}, f)

            with open(file_path, "r") as f:
                ports_by_node.update(json.load(f))

            if (
                self.unique_id in ports_by_node
                and port_name in ports_by_node[self.unique_id]
            ):
                # The port has already been cached at this node, so use it.
                port = int(ports_by_node[self.unique_id][port_name])
            else:
                # Pick a new port to use and cache it at this node.
                port = default_port or self._get_unused_port(
                    set(ports_by_node[self.unique_id].values())
                )
                ports_by_node[self.unique_id][port_name] = port
                with open(file_path, "w") as f:
                    json.dump(ports_by_node, f)

        return port

    def start_reaper_process(self):
        """
        Start the reaper process.

        This must be the first process spawned and should only be called when
        ray processes should be cleaned up if this process dies.
        """
        assert (
            not self.kernel_fate_share
        ), "a reaper should not be used with kernel fate-sharing"
        process_info = ray._private.services.start_reaper(fate_share=False)
        assert ray_constants.PROCESS_TYPE_REAPER not in self.all_processes
        if process_info is not None:
            self.all_processes[ray_constants.PROCESS_TYPE_REAPER] = [
                process_info,
            ]

    def start_or_configure_redis(self):
        """Starts local Redis or configures external Redis."""
        assert self._redis_address is None
        redis_log_files = []
        if self._ray_params.external_addresses is None:
            redis_log_files = [self.get_log_file_handles("redis", unique=True)]
            for i in range(self._ray_params.num_redis_shards):
                redis_log_files.append(
                    self.get_log_file_handles(f"redis-shard_{i}", unique=True)
                )

        (
            self._redis_address,
            redis_shards,
            process_infos,
        ) = ray._private.services.start_redis(
            self._node_ip_address,
            redis_log_files,
            self.get_resource_spec(),
            self.get_session_dir_path(),
            port=self._ray_params.redis_port,
            redis_shard_ports=self._ray_params.redis_shard_ports,
            num_redis_shards=self._ray_params.num_redis_shards,
            redis_max_clients=self._ray_params.redis_max_clients,
            password=self._ray_params.redis_password,
            fate_share=self.kernel_fate_share,
            external_addresses=self._ray_params.external_addresses,
            port_denylist=self._ray_params.reserved_ports,
        )
        assert ray_constants.PROCESS_TYPE_REDIS_SERVER not in self.all_processes
        self.all_processes[ray_constants.PROCESS_TYPE_REDIS_SERVER] = process_infos

    def start_log_monitor(self):
        """Start the log monitor."""
        process_info = ray._private.services.start_log_monitor(
            self._logs_dir,
            self.gcs_address,
            fate_share=self.kernel_fate_share,
            max_bytes=self.max_bytes,
            backup_count=self.backup_count,
            redirect_logging=self.should_redirect_logs(),
        )
        assert ray_constants.PROCESS_TYPE_LOG_MONITOR not in self.all_processes
        self.all_processes[ray_constants.PROCESS_TYPE_LOG_MONITOR] = [
            process_info,
        ]

    def start_dashboard(self, require_dashboard):
        """Start the dashboard.

        Args:
            require_dashboard (bool): If true, this will raise an exception
                if we fail to start the dashboard. Otherwise it will print
                a warning if we fail to start the dashboard.
        """
        self._webui_url, process_info = ray._private.services.start_dashboard(
            require_dashboard,
            self._ray_params.dashboard_host,
            self.gcs_address,
            self._temp_dir,
            self._logs_dir,
            self._session_dir,
            fate_share=self.kernel_fate_share,
            max_bytes=self.max_bytes,
            backup_count=self.backup_count,
            port=self._ray_params.dashboard_port,
            redirect_logging=self.should_redirect_logs(),
        )
        assert ray_constants.PROCESS_TYPE_DASHBOARD not in self.all_processes
        if process_info is not None:
            self.all_processes[ray_constants.PROCESS_TYPE_DASHBOARD] = [
                process_info,
            ]
            self.get_gcs_client().internal_kv_put(
                b"webui:url",
                self._webui_url.encode(),
                True,
                ray_constants.KV_NAMESPACE_DASHBOARD,
            )

    def start_gcs_server(self):
        """Start the gcs server."""
        gcs_server_port = self._ray_params.gcs_server_port
        assert gcs_server_port > 0
        assert self._gcs_address is None, "GCS server is already running."
        assert self._gcs_client is None, "GCS client is already connected."
        # TODO(mwtian): append date time so restarted GCS uses different files.
        stdout_file, stderr_file = self.get_log_file_handles("gcs_server", unique=True)
        process_info = ray._private.services.start_gcs_server(
            self.redis_address,
            self._logs_dir,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            redis_password=self._ray_params.redis_password,
            config=self._config,
            fate_share=self.kernel_fate_share,
            gcs_server_port=gcs_server_port,
            metrics_agent_port=self._ray_params.metrics_agent_port,
            node_ip_address=self._node_ip_address,
        )
        assert ray_constants.PROCESS_TYPE_GCS_SERVER not in self.all_processes
        self.all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER] = [
            process_info,
        ]
        # Connecting via non-localhost address may be blocked by firewall rule,
        # e.g. https://github.com/ray-project/ray/issues/15780
        # TODO(mwtian): figure out a way to use 127.0.0.1 for local connection
        # when possible.
        self._gcs_address = f"{self._node_ip_address}:" f"{gcs_server_port}"
        # Initialize gcs client, which also waits for GCS to start running.
        self.get_gcs_client()

    def start_raylet(
        self,
        plasma_directory,
        object_store_memory,
        use_valgrind=False,
        use_profiler=False,
    ):
        """Start the raylet.

        Args:
            use_valgrind (bool): True if we should start the process in
                valgrind.
            use_profiler (bool): True if we should start the process in the
                valgrind profiler.
        """
        stdout_file, stderr_file = self.get_log_file_handles("raylet", unique=True)
        process_info = ray._private.services.start_raylet(
            self.redis_address,
            self.gcs_address,
            self._node_ip_address,
            self._ray_params.node_manager_port,
            self._raylet_socket_name,
            self._plasma_store_socket_name,
            self._ray_params.worker_path,
            self._ray_params.setup_worker_path,
            self._ray_params.storage,
            self._temp_dir,
            self._session_dir,
            self._runtime_env_dir,
            self._logs_dir,
            self.get_resource_spec(),
            plasma_directory,
            object_store_memory,
            min_worker_port=self._ray_params.min_worker_port,
            max_worker_port=self._ray_params.max_worker_port,
            worker_port_list=self._ray_params.worker_port_list,
            object_manager_port=self._ray_params.object_manager_port,
            redis_password=self._ray_params.redis_password,
            metrics_agent_port=self._ray_params.metrics_agent_port,
            metrics_export_port=self._metrics_export_port,
            dashboard_agent_listen_port=self._ray_params.dashboard_agent_listen_port,
            use_valgrind=use_valgrind,
            use_profiler=use_profiler,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            config=self._config,
            huge_pages=self._ray_params.huge_pages,
            fate_share=self.kernel_fate_share,
            socket_to_use=None,
            max_bytes=self.max_bytes,
            backup_count=self.backup_count,
            start_initial_python_workers_for_first_job=self._ray_params.start_initial_python_workers_for_first_job,  # noqa: E501
            ray_debugger_external=self._ray_params.ray_debugger_external,
            env_updates=self._ray_params.env_vars,
        )
        assert ray_constants.PROCESS_TYPE_RAYLET not in self.all_processes
        self.all_processes[ray_constants.PROCESS_TYPE_RAYLET] = [process_info]

    def start_worker(self):
        """Start a worker process."""
        raise NotImplementedError

    def start_monitor(self):
        """Start the monitor.

        Autoscaling output goes to these monitor.err/out files, and
        any modification to these files may break existing
        cluster launching commands.
        """
        stdout_file, stderr_file = self.get_log_file_handles("monitor", unique=True)
        process_info = ray._private.services.start_monitor(
            self.redis_address,
            self.gcs_address,
            self._logs_dir,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            autoscaling_config=self._ray_params.autoscaling_config,
            redis_password=self._ray_params.redis_password,
            fate_share=self.kernel_fate_share,
            max_bytes=self.max_bytes,
            backup_count=self.backup_count,
            monitor_ip=self._node_ip_address,
        )
        assert ray_constants.PROCESS_TYPE_MONITOR not in self.all_processes
        self.all_processes[ray_constants.PROCESS_TYPE_MONITOR] = [process_info]

    def start_ray_client_server(self):
        """Start the ray client server process."""
        stdout_file, stderr_file = self.get_log_file_handles(
            "ray_client_server", unique=True
        )
        process_info = ray._private.services.start_ray_client_server(
            self.address,
            self._node_ip_address,
            self._ray_params.ray_client_server_port,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            redis_password=self._ray_params.redis_password,
            fate_share=self.kernel_fate_share,
            metrics_agent_port=self._ray_params.metrics_agent_port,
        )
        assert ray_constants.PROCESS_TYPE_RAY_CLIENT_SERVER not in self.all_processes
        self.all_processes[ray_constants.PROCESS_TYPE_RAY_CLIENT_SERVER] = [
            process_info
        ]

    def _write_cluster_info_to_kv(self):
        """Write the cluster metadata to GCS.
        Cluster metadata is always recorded, but they are
        not reported unless usage report is enabled.
        Check `usage_stats_head.py` for more details.
        """
        # Make sure the cluster metadata wasn't reported before.
        ray_usage_lib.put_cluster_metadata(self.get_gcs_client(), NUM_REDIS_GET_RETRIES)

    def start_head_processes(self):
        """Start head processes on the node."""
        logger.debug(
            f"Process STDOUT and STDERR is being " f"redirected to {self._logs_dir}."
        )
        assert self._redis_address is None
        assert self._gcs_address is None
        assert self._gcs_client is None

        if self._ray_params.external_addresses is not None:
            # This only configures external Redis and does not start local
            # Redis, when external Redis address is specified.
            # TODO(mwtian): after GCS bootstrapping is default and stable,
            # only keep external Redis configuration logic in the function.
            self.start_or_configure_redis()
            # Wait for Redis to become available.
            self.create_redis_client()

        self.start_gcs_server()
        assert self._gcs_client is not None
        self._write_cluster_info_to_kv()

        if not self._ray_params.no_monitor:
            self.start_monitor()

        if self._ray_params.ray_client_server_port:
            self.start_ray_client_server()

        if self._ray_params.include_dashboard:
            self.start_dashboard(require_dashboard=True)
        elif self._ray_params.include_dashboard is None:
            self.start_dashboard(require_dashboard=False)

    def start_ray_processes(self):
        """Start all of the processes on the node."""
        logger.debug(
            f"Process STDOUT and STDERR is being " f"redirected to {self._logs_dir}."
        )

        # Clean up external storage in case a previous Raylet instance crashed
        # on this node and spilled objects remain on disk.
        if not self.head:
            # Get the system config from GCS first if this is a non-head node.
            gcs_options = ray._raylet.GcsClientOptions.from_gcs_address(
                self.gcs_address
            )
            global_state = ray.state.GlobalState()
            global_state._initialize_global_state(gcs_options)
            new_config = global_state.get_system_config()
            assert self._config.items() <= new_config.items(), (
                "The system config from GCS is not a superset of the local"
                " system config. There might be a configuration inconsistency"
                " issue between the head node and non-head nodes."
                f" Local system config: {self._config},"
                f" GCS system config: {new_config}"
            )
            self._config = new_config
        self.destroy_external_storage()

        # Make sure we don't call `determine_plasma_store_config` multiple
        # times to avoid printing multiple warnings.
        resource_spec = self.get_resource_spec()
        (
            plasma_directory,
            object_store_memory,
        ) = ray._private.services.determine_plasma_store_config(
            resource_spec.object_store_memory,
            plasma_directory=self._ray_params.plasma_directory,
            huge_pages=self._ray_params.huge_pages,
        )
        self.start_raylet(plasma_directory, object_store_memory)
        if self._ray_params.include_log_monitor:
            self.start_log_monitor()

    def _kill_process_type(
        self, process_type, allow_graceful=False, check_alive=True, wait=False
    ):
        """Kill a process of a given type.

        If the process type is PROCESS_TYPE_REDIS_SERVER, then we will kill all
        of the Redis servers.

        If the process was started in valgrind, then we will raise an exception
        if the process has a non-zero exit code.

        Args:
            process_type: The type of the process to kill.
            allow_graceful (bool): Send a SIGTERM first and give the process
                time to exit gracefully. If that doesn't work, then use
                SIGKILL. We usually want to do this outside of tests.
            check_alive (bool): If true, then we expect the process to be alive
                and will raise an exception if the process is already dead.
            wait (bool): If true, then this method will not return until the
                process in question has exited.

        Raises:
            This process raises an exception in the following cases:
                1. The process had already died and check_alive is true.
                2. The process had been started in valgrind and had a non-zero
                   exit code.
        """

        # Ensure thread safety
        with self.removal_lock:
            self._kill_process_impl(
                process_type,
                allow_graceful=allow_graceful,
                check_alive=check_alive,
                wait=wait,
            )

    def _kill_process_impl(
        self, process_type, allow_graceful=False, check_alive=True, wait=False
    ):
        """See `_kill_process_type`."""
        if process_type not in self.all_processes:
            return
        process_infos = self.all_processes[process_type]
        if process_type != ray_constants.PROCESS_TYPE_REDIS_SERVER:
            assert len(process_infos) == 1
        for process_info in process_infos:
            process = process_info.process
            # Handle the case where the process has already exited.
            if process.poll() is not None:
                if check_alive:
                    raise RuntimeError(
                        "Attempting to kill a process of type "
                        f"'{process_type}', but this process is already dead."
                    )
                else:
                    continue

            if process_info.use_valgrind:
                process.terminate()
                process.wait()
                if process.returncode != 0:
                    message = (
                        "Valgrind detected some errors in process of "
                        f"type {process_type}. Error code {process.returncode}."
                    )
                    if process_info.stdout_file is not None:
                        with open(process_info.stdout_file, "r") as f:
                            message += "\nPROCESS STDOUT:\n" + f.read()
                    if process_info.stderr_file is not None:
                        with open(process_info.stderr_file, "r") as f:
                            message += "\nPROCESS STDERR:\n" + f.read()
                    raise RuntimeError(message)
                continue

            if process_info.use_valgrind_profiler:
                # Give process signal to write profiler data.
                os.kill(process.pid, signal.SIGINT)
                # Wait for profiling data to be written.
                time.sleep(0.1)

            if allow_graceful:
                process.terminate()
                # Allow the process one second to exit gracefully.
                timeout_seconds = 1
                try:
                    process.wait(timeout_seconds)
                except subprocess.TimeoutExpired:
                    pass

            # If the process did not exit, force kill it.
            if process.poll() is None:
                process.kill()
                # The reason we usually don't call process.wait() here is that
                # there's some chance we'd end up waiting a really long time.
                if wait:
                    process.wait()

        del self.all_processes[process_type]

    def kill_redis(self, check_alive=True):
        """Kill the Redis servers.

        Args:
            check_alive (bool): Raise an exception if any of the processes
                were already dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_REDIS_SERVER, check_alive=check_alive
        )

    def kill_raylet(self, check_alive=True):
        """Kill the raylet.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_RAYLET, check_alive=check_alive
        )

    def kill_log_monitor(self, check_alive=True):
        """Kill the log monitor.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_LOG_MONITOR, check_alive=check_alive
        )

    def kill_reporter(self, check_alive=True):
        """Kill the reporter.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_REPORTER, check_alive=check_alive
        )

    def kill_dashboard(self, check_alive=True):
        """Kill the dashboard.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_DASHBOARD, check_alive=check_alive
        )

    def kill_monitor(self, check_alive=True):
        """Kill the monitor.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_MONITOR, check_alive=check_alive
        )

    def kill_gcs_server(self, check_alive=True):
        """Kill the gcs server.
        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_GCS_SERVER, check_alive=check_alive
        )
        # Clear GCS client and address to indicate no GCS server is running.
        self._gcs_address = None
        self._gcs_client = None

    def kill_reaper(self, check_alive=True):
        """Kill the reaper process.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_REAPER, check_alive=check_alive
        )

    def kill_all_processes(self, check_alive=True, allow_graceful=False):
        """Kill all of the processes.

        Note that This is slower than necessary because it calls kill, wait,
        kill, wait, ... instead of kill, kill, ..., wait, wait, ...

        Args:
            check_alive (bool): Raise an exception if any of the processes were
                already dead.
        """
        # Kill the raylet first. This is important for suppressing errors at
        # shutdown because we give the raylet a chance to exit gracefully and
        # clean up its child worker processes. If we were to kill the plasma
        # store (or Redis) first, that could cause the raylet to exit
        # ungracefully, leading to more verbose output from the workers.
        if ray_constants.PROCESS_TYPE_RAYLET in self.all_processes:
            self._kill_process_type(
                ray_constants.PROCESS_TYPE_RAYLET,
                check_alive=check_alive,
                allow_graceful=allow_graceful,
            )

        if ray_constants.PROCESS_TYPE_GCS_SERVER in self.all_processes:
            self._kill_process_type(
                ray_constants.PROCESS_TYPE_GCS_SERVER,
                check_alive=check_alive,
                allow_graceful=allow_graceful,
            )

        # We call "list" to copy the keys because we are modifying the
        # dictionary while iterating over it.
        for process_type in list(self.all_processes.keys()):
            # Need to kill the reaper process last in case we die unexpectedly
            # while cleaning up.
            if process_type != ray_constants.PROCESS_TYPE_REAPER:
                self._kill_process_type(
                    process_type, check_alive=check_alive, allow_graceful=allow_graceful
                )

        if ray_constants.PROCESS_TYPE_REAPER in self.all_processes:
            self._kill_process_type(
                ray_constants.PROCESS_TYPE_REAPER,
                check_alive=check_alive,
                allow_graceful=allow_graceful,
            )

    def live_processes(self):
        """Return a list of the live processes.

        Returns:
            A list of the live processes.
        """
        result = []
        for process_type, process_infos in self.all_processes.items():
            for process_info in process_infos:
                if process_info.process.poll() is None:
                    result.append((process_type, process_info.process))
        return result

    def dead_processes(self):
        """Return a list of the dead processes.

        Note that this ignores processes that have been explicitly killed,
        e.g., via a command like node.kill_raylet().

        Returns:
            A list of the dead processes ignoring the ones that have been
                explicitly killed.
        """
        result = []
        for process_type, process_infos in self.all_processes.items():
            for process_info in process_infos:
                if process_info.process.poll() is not None:
                    result.append((process_type, process_info.process))
        return result

    def any_processes_alive(self):
        """Return true if any processes are still alive.

        Returns:
            True if any process is still alive.
        """
        return any(self.live_processes())

    def remaining_processes_alive(self):
        """Return true if all remaining processes are still alive.

        Note that this ignores processes that have been explicitly killed,
        e.g., via a command like node.kill_raylet().

        Returns:
            True if any process that wasn't explicitly killed is still alive.
        """
        return not any(self.dead_processes())

    def destroy_external_storage(self):
        object_spilling_config = self._config.get("object_spilling_config", {})
        if object_spilling_config:
            object_spilling_config = json.loads(object_spilling_config)
            from ray import external_storage

            storage = external_storage.setup_external_storage(object_spilling_config)
            storage.destroy_external_storage()

    def validate_external_storage(self):
        """Make sure we can setup the object spilling external storage.
        This will also fill up the default setting for object spilling
        if not specified.
        """
        object_spilling_config = self._config.get("object_spilling_config", {})
        automatic_spilling_enabled = self._config.get(
            "automatic_object_spilling_enabled", True
        )
        if not automatic_spilling_enabled:
            return

        # If the config is not specified, we fill up the default.
        if not object_spilling_config:
            object_spilling_config = json.dumps(
                {"type": "filesystem", "params": {"directory_path": self._session_dir}}
            )

        # Try setting up the storage.
        # Configure the proper system config.
        # We need to set both ray param's system config and self._config
        # because they could've been diverged at this point.
        deserialized_config = json.loads(object_spilling_config)
        self._ray_params._system_config[
            "object_spilling_config"
        ] = object_spilling_config
        self._config["object_spilling_config"] = object_spilling_config

        is_external_storage_type_fs = deserialized_config["type"] == "filesystem"
        self._ray_params._system_config[
            "is_external_storage_type_fs"
        ] = is_external_storage_type_fs
        self._config["is_external_storage_type_fs"] = is_external_storage_type_fs

        # Validate external storage usage.
        from ray import external_storage

        external_storage.setup_external_storage(deserialized_config)
        external_storage.reset_external_storage()
