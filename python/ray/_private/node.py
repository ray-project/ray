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
from collections import defaultdict
from typing import Dict, Optional, Tuple, IO, AnyStr

from filelock import FileLock

import ray
import ray._private.ray_constants as ray_constants
import ray._private.services
from ray._private import storage
from ray._raylet import GcsClient, get_session_key_from_storage
from ray._private.resource_spec import ResourceSpec
from ray._private.services import serialize_config, get_address
from ray._private.utils import open_log, try_to_create_directory, try_to_symlink

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray configures it by default automatically
# using logging.basicConfig in its entry/init points.
logger = logging.getLogger(__name__)


class Node:
    """An encapsulation of the Ray processes on a single node.

    This class is responsible for starting Ray processes and killing them,
    and it also controls the temp file policy.

    Attributes:
        all_processes: A mapping from process type (str) to a list of
            ProcessInfo objects. All lists have length one except for the Redis
            server list, which has multiple.
    """

    def __init__(
        self,
        ray_params,
        head: bool = False,
        shutdown_at_exit: bool = True,
        spawn_reaper: bool = True,
        connect_only: bool = False,
        default_worker: bool = False,
        ray_init_cluster: bool = False,
    ):
        """Start a node.

        Args:
            ray_params: The RayParams to use to configure the node.
            head: True if this is the head node, which means it will
                start additional processes like the Redis servers, monitor
                processes, and web UI.
            shutdown_at_exit: If true, spawned processes will be cleaned
                up if this process exits normally.
            spawn_reaper: If true, spawns a process that will clean up
                other spawned processes if this process dies unexpectedly.
            connect_only: If true, connect to the node without starting
                new processes.
            default_worker: Whether it's running from a ray worker or not
            ray_init_cluster: Whether it's a cluster created by ray.init()
        """
        if shutdown_at_exit:
            if connect_only:
                raise ValueError(
                    "'shutdown_at_exit' and 'connect_only' cannot both be true."
                )
            self._register_shutdown_hooks()
        self._default_worker = default_worker
        self.head = head
        self.kernel_fate_share = bool(
            spawn_reaper and ray._private.utils.detect_fate_sharing_support()
        )
        self.all_processes: dict = {}
        self.removal_lock = threading.Lock()

        self.ray_init_cluster = ray_init_cluster
        if ray_init_cluster:
            assert head, "ray.init() created cluster only has the head node"

        # Set up external Redis when `RAY_REDIS_ADDRESS` is specified.
        redis_address_env = os.environ.get("RAY_REDIS_ADDRESS")
        if ray_params.external_addresses is None and redis_address_env is not None:
            external_redis = redis_address_env.split(",")

            # Reuse primary Redis as Redis shard when there's only one
            # instance provided.
            if len(external_redis) == 1:
                external_redis.append(external_redis[0])
            [primary_redis_ip, port] = external_redis[0].rsplit(":", 1)
            ray_params.external_addresses = external_redis
            ray_params.num_redis_shards = len(external_redis) - 1

        if (
            ray_params._system_config
            and len(ray_params._system_config) > 0
            and (not head and not connect_only)
        ):
            raise ValueError(
                "System config parameters can only be set on the head node."
            )

        ray_params.update_if_absent(
            include_log_monitor=True,
            resources={},
            worker_path=os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "workers",
                "default_worker.py",
            ),
            setup_worker_path=os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "workers",
                ray_constants.SETUP_WORKER_FILENAME,
            ),
        )

        self._resource_spec = None
        self._localhost = socket.gethostbyname("localhost")
        self._ray_params = ray_params
        self._config = ray_params._system_config or {}

        self._dashboard_agent_listen_port = ray_params.dashboard_agent_listen_port
        self._dashboard_grpc_port = ray_params.dashboard_grpc_port

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
            self._init_gcs_client()

        # Register the temp dir.
        self._session_name = ray_params.session_name
        if self._session_name is None:
            if head:
                # We expect this the first time we initialize a cluster, but not during
                # subsequent restarts of the head node.
                maybe_key = self.check_persisted_session_name()
                if maybe_key is None:
                    # date including microsecond
                    date_str = datetime.datetime.today().strftime(
                        "%Y-%m-%d_%H-%M-%S_%f"
                    )
                    self._session_name = f"session_{date_str}_{os.getpid()}"
                else:
                    self._session_name = ray._private.utils.decode(maybe_key)
            else:
                assert not self._default_worker
                session_name = ray._private.utils.internal_kv_get_with_retry(
                    self.get_gcs_client(),
                    "session_name",
                    ray_constants.KV_NAMESPACE_SESSION,
                    num_retries=ray_constants.NUM_REDIS_GET_RETRIES,
                )
                self._session_name = ray._private.utils.decode(session_name)

        # Initialize webui url
        if head:
            self._webui_url = None
        else:
            if ray_params.webui is None:
                assert not self._default_worker
                self._webui_url = ray._private.services.get_webui_url_from_internal_kv()
            else:
                self._webui_url = (
                    f"{ray_params.dashboard_host}:{ray_params.dashboard_port}"
                )

        # It creates a session_dir.
        self._init_temp()

        node_ip_address = ray_params.node_ip_address
        if node_ip_address is None:
            if connect_only:
                node_ip_address = self._wait_and_get_for_node_address()
            else:
                node_ip_address = ray.util.get_node_ip_address()

        assert node_ip_address is not None
        ray_params.update_if_absent(
            node_ip_address=node_ip_address, raylet_ip_address=node_ip_address
        )
        self._node_ip_address = node_ip_address
        if not connect_only:
            ray._private.services.write_node_ip_address(
                self.get_session_dir_path(), node_ip_address
            )

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
        self._raylet_ip_address = raylet_ip_address

        # Validate and initialize the persistent storage API.
        if head:
            storage._init_storage(ray_params.storage, is_head=True)
        else:
            if not self._default_worker:
                storage_uri = ray._private.services.get_storage_uri_from_internal_kv()
            else:
                storage_uri = ray_params.storage
            storage._init_storage(storage_uri, is_head=False)

        # If it is a head node, try validating if
        # external storage is configurable.
        if head:
            self.validate_external_storage()

        if connect_only:
            # Get socket names from the configuration.
            self._plasma_store_socket_name = ray_params.plasma_store_socket_name
            self._raylet_socket_name = ray_params.raylet_socket_name
            self._node_id = ray_params.node_id

            # If user does not provide the socket name, get it from Redis.
            if (
                self._plasma_store_socket_name is None
                or self._raylet_socket_name is None
                or self._ray_params.node_manager_port is None
                or self._node_id is None
            ):
                # Get the address info of the processes to connect to
                # from Redis or GCS.
                node_info = ray._private.services.get_node_to_connect_for_driver(
                    self.gcs_address,
                    self._raylet_ip_address,
                )
                self._plasma_store_socket_name = node_info["object_store_socket_name"]
                self._raylet_socket_name = node_info["raylet_socket_name"]
                self._ray_params.node_manager_port = node_info["node_manager_port"]
                self._node_id = node_info["node_id"]
        else:
            # If the user specified a socket name, use it.
            self._plasma_store_socket_name = self._prepare_socket_file(
                self._ray_params.plasma_store_socket_name, default_prefix="plasma_store"
            )
            self._raylet_socket_name = self._prepare_socket_file(
                self._ray_params.raylet_socket_name, default_prefix="raylet"
            )
            if (
                self._ray_params.env_vars is not None
                and "RAY_OVERRIDE_NODE_ID_FOR_TESTING" in self._ray_params.env_vars
            ):
                node_id = self._ray_params.env_vars["RAY_OVERRIDE_NODE_ID_FOR_TESTING"]
                logger.debug(
                    f"Setting node ID to {node_id} "
                    "based on ray_params.env_vars override"
                )
                self._node_id = node_id
            elif os.environ.get("RAY_OVERRIDE_NODE_ID_FOR_TESTING"):
                node_id = os.environ["RAY_OVERRIDE_NODE_ID_FOR_TESTING"]
                logger.debug(f"Setting node ID to {node_id} based on env override")
                self._node_id = node_id
            else:
                node_id = ray.NodeID.from_random().hex()
                logger.debug(f"Setting node ID to {node_id}")
                self._node_id = node_id

        # The dashboard agent port is assigned first to avoid
        # other processes accidentally taking its default port
        self._dashboard_agent_listen_port = self._get_cached_port(
            "dashboard_agent_listen_port",
            default_port=ray_params.dashboard_agent_listen_port,
        )

        self.metrics_agent_port = self._get_cached_port(
            "metrics_agent_port", default_port=ray_params.metrics_agent_port
        )
        self._metrics_export_port = self._get_cached_port(
            "metrics_export_port", default_port=ray_params.metrics_export_port
        )
        self._runtime_env_agent_port = self._get_cached_port(
            "runtime_env_agent_port",
            default_port=ray_params.runtime_env_agent_port,
        )

        ray_params.update_if_absent(
            metrics_agent_port=self.metrics_agent_port,
            metrics_export_port=self._metrics_export_port,
            dashboard_agent_listen_port=self._dashboard_agent_listen_port,
            runtime_env_agent_port=self._runtime_env_agent_port,
        )

        # Pick a GCS server port.
        if head:
            gcs_server_port = os.getenv(ray_constants.GCS_PORT_ENVIRONMENT_VARIABLE)
            if gcs_server_port:
                ray_params.update_if_absent(gcs_server_port=int(gcs_server_port))
            if ray_params.gcs_server_port is None or ray_params.gcs_server_port == 0:
                ray_params.gcs_server_port = self._get_cached_port("gcs_server_port")

        if not connect_only and spawn_reaper and not self.kernel_fate_share:
            self.start_reaper_process()
        if not connect_only:
            self._ray_params.update_pre_selected_port()

        # Start processes.
        if head:
            self.start_head_processes()

        if not connect_only:
            self.start_ray_processes()
            # we should update the address info after the node has been started
            try:
                ray._private.services.wait_for_node(
                    self.gcs_address,
                    self._plasma_store_socket_name,
                )
            except TimeoutError as te:
                raise Exception(
                    "The current node timed out during startup. This "
                    "could happen because some of the Ray processes "
                    "failed to startup."
                ) from te
            node_info = ray._private.services.get_node(
                self.gcs_address,
                self._node_id,
            )
            if self._ray_params.node_manager_port == 0:
                self._ray_params.node_manager_port = node_info["node_manager_port"]

        # Makes sure the Node object has valid addresses after setup.
        self.validate_ip_port(self.address)
        self.validate_ip_port(self.gcs_address)

        if not connect_only:
            self._record_stats()

    def check_persisted_session_name(self):
        if self._ray_params.external_addresses is None:
            return None
        self._redis_address = self._ray_params.external_addresses[0]
        redis_ip_address, redis_port, enable_redis_ssl = get_address(
            self._redis_address,
        )
        # Address is ip:port or redis://ip:port
        if int(redis_port) < 0:
            raise ValueError(
                f"Invalid Redis port provided: {redis_port}."
                "The port must be a non-negative integer."
            )

        return get_session_key_from_storage(
            redis_ip_address,
            int(redis_port),
            self._ray_params.redis_username,
            self._ray_params.redis_password,
            enable_redis_ssl,
            serialize_config(self._config),
            b"session_name",
        )

    @staticmethod
    def validate_ip_port(ip_port):
        """Validates the address is in the ip:port format"""
        _, _, port = ip_port.rpartition(":")
        if port == ip_port:
            raise ValueError(f"Port is not specified for address {ip_port}")
        try:
            _ = int(port)
        except ValueError:
            raise ValueError(
                f"Unable to parse port number from {port} (full address = {ip_port})"
            )

    def check_version_info(self):
        """Check if the Python and Ray version of this process matches that in GCS.

        This will be used to detect if workers or drivers are started using
        different versions of Python, or Ray.

        Raises:
            Exception: An exception is raised if there is a version mismatch.
        """
        import ray._private.usage.usage_lib as ray_usage_lib

        cluster_metadata = ray_usage_lib.get_cluster_metadata(self.get_gcs_client())
        if cluster_metadata is None:
            cluster_metadata = ray_usage_lib.get_cluster_metadata(self.get_gcs_client())

        if not cluster_metadata:
            return
        node_ip_address = ray._private.services.get_node_ip_address()
        ray._private.utils.check_version_info(
            cluster_metadata, f"node {node_ip_address}"
        )

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
            self._ray_params.update_if_absent(
                temp_dir=ray._private.utils.get_ray_temp_dir()
            )
            self._temp_dir = self._ray_params.temp_dir
        else:
            if self._ray_params.temp_dir is None:
                assert not self._default_worker
                temp_dir = ray._private.utils.internal_kv_get_with_retry(
                    self.get_gcs_client(),
                    "temp_dir",
                    ray_constants.KV_NAMESPACE_SESSION,
                    num_retries=ray_constants.NUM_REDIS_GET_RETRIES,
                )
                self._temp_dir = ray._private.utils.decode(temp_dir)
            else:
                self._temp_dir = self._ray_params.temp_dir

        try_to_create_directory(self._temp_dir)

        if self.head:
            self._session_dir = os.path.join(self._temp_dir, self._session_name)
        else:
            if self._temp_dir is None or self._session_name is None:
                assert not self._default_worker
                session_dir = ray._private.utils.internal_kv_get_with_retry(
                    self.get_gcs_client(),
                    "session_dir",
                    ray_constants.KV_NAMESPACE_SESSION,
                    num_retries=ray_constants.NUM_REDIS_GET_RETRIES,
                )
                self._session_dir = ray._private.utils.decode(session_dir)
            else:
                self._session_dir = os.path.join(self._temp_dir, self._session_name)
        session_symlink = os.path.join(self._temp_dir, ray_constants.SESSION_LATEST)

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

    def _get_node_labels(self):
        def merge_labels(env_override_labels, params_labels):
            """Merges two dictionaries, picking from the
            first in the event of a conflict. Also emit a warning on every
            conflict.
            """

            result = params_labels.copy()
            result.update(env_override_labels)

            for key in set(env_override_labels.keys()).intersection(
                set(params_labels.keys())
            ):
                if params_labels[key] != env_override_labels[key]:
                    logger.warning(
                        "Autoscaler is overriding your label:"
                        f"{key}: {params_labels[key]} to "
                        f"{key}: {env_override_labels[key]}."
                    )
            return result

        env_override_labels = {}
        env_override_labels_string = os.getenv(
            ray_constants.LABELS_ENVIRONMENT_VARIABLE
        )
        if env_override_labels_string:
            try:
                env_override_labels = json.loads(env_override_labels_string)
            except Exception:
                logger.exception(f"Failed to load {env_override_labels_string}")
                raise
            logger.info(f"Autoscaler overriding labels: {env_override_labels}.")

        return merge_labels(env_override_labels, self._ray_params.labels or {})

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
    def node_id(self):
        """Get the node ID."""
        return self._node_id

    @property
    def session_name(self):
        """Get the session name (cluster ID)."""
        return self._session_name

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
        `ray start` or `ray.init()` to start worker nodes, that has been
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
    def redis_username(self):
        """Get the cluster Redis username."""
        return self._ray_params.redis_username

    @property
    def redis_password(self):
        """Get the cluster Redis password."""
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
    def runtime_env_agent_port(self):
        """Get the port that exposes runtime env agent as http"""
        return self._runtime_env_agent_port

    @property
    def runtime_env_agent_address(self):
        """Get the address that exposes runtime env agent as http"""
        return f"http://{self._raylet_ip_address}:{self._runtime_env_agent_port}"

    @property
    def dashboard_agent_listen_port(self):
        """Get the dashboard agent's listen port"""
        return self._dashboard_agent_listen_port

    @property
    def dashboard_grpc_port(self):
        """Get the dashboard head grpc port"""
        return self._dashboard_grpc_port

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
            "dashboard_agent_listen_port": self.dashboard_agent_listen_port,
        }

    def is_head(self):
        return self.head

    def get_gcs_client(self):
        if self._gcs_client is None:
            self._init_gcs_client()
        return self._gcs_client

    def _init_gcs_client(self):
        if self.head:
            gcs_process = self.all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER][
                0
            ].process
        else:
            gcs_process = None

        # TODO(ryw) instead of create a new GcsClient, wrap the one from
        # CoreWorkerProcess to save a grpc channel.
        for _ in range(ray_constants.NUM_REDIS_GET_RETRIES):
            gcs_address = None
            last_ex = None
            try:
                gcs_address = self.gcs_address
                client = GcsClient(
                    address=gcs_address,
                    cluster_id=self._ray_params.cluster_id,  # Hex string
                )
                self.cluster_id = client.cluster_id
                if self.head:
                    # Send a simple request to make sure GCS is alive
                    # if it's a head node.
                    client.internal_kv_get(b"dummy", None)
                self._gcs_client = client
                break
            except Exception:
                if gcs_process is not None and gcs_process.poll() is not None:
                    # GCS has exited.
                    break
                last_ex = traceback.format_exc()
                logger.debug(f"Connecting to GCS: {last_ex}")
                time.sleep(1)

        if self._gcs_client is None:
            if hasattr(self, "_logs_dir"):
                with open(os.path.join(self._logs_dir, "gcs_server.err")) as err:
                    # Use " C " or " E " to exclude the stacktrace.
                    # This should work for most cases, especitally
                    # it's when GCS is starting. Only display last 10 lines of logs.
                    errors = [e for e in err.readlines() if " C " in e or " E " in e][
                        -10:
                    ]
                error_msg = "\n" + "".join(errors) + "\n"
                raise RuntimeError(
                    f"Failed to {'start' if self.head else 'connect to'} GCS. "
                    f" Last {len(errors)} lines of error files:"
                    f"{error_msg}."
                    f"Please check {os.path.join(self._logs_dir, 'gcs_server.out')}"
                    f" for details. Last connection error: {last_ex}"
                )
            else:
                raise RuntimeError(
                    f"Failed to {'start' if self.head else 'connect to'} GCS. Last "
                    f"connection error: {last_ex}"
                )

        ray.experimental.internal_kv._initialize_internal_kv(self._gcs_client)

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

    def _make_inc_temp(
        self, suffix: str = "", prefix: str = "", directory_name: Optional[str] = None
    ):
        """Return an incremental temporary file name. The file is not created.

        Args:
            suffix: The suffix of the temp file.
            prefix: The prefix of the temp file.
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

    def get_log_file_handles(
        self,
        name: str,
        unique: bool = False,
        create_out: bool = True,
        create_err: bool = True,
    ) -> Tuple[Optional[IO[AnyStr]], Optional[IO[AnyStr]]]:
        """Open log files with partially randomized filenames, returning the
        file handles. If output redirection has been disabled, no files will
        be opened and `(None, None)` will be returned.

        Args:
            name: descriptive string for this log file.
            unique: if true, a counter will be attached to `name` to
                ensure the returned filename is not already used.
            create_out: if True, create a .out file.
            create_err: if True, create a .err file.

        Returns:
            A tuple of two file handles for redirecting optional (stdout, stderr),
            or `(None, None)` if output redirection is disabled.
        """
        if not self.should_redirect_logs():
            return None, None

        log_stdout = None
        log_stderr = None

        if create_out:
            log_stdout = open_log(self._get_log_file_name(name, "out", unique=unique))
        if create_err:
            log_stderr = open_log(self._get_log_file_name(name, "err", unique=unique))
        return log_stdout, log_stderr

    def _get_log_file_name(
        self,
        name: str,
        suffix: str,
        unique: bool = False,
    ) -> str:
        """Generate partially randomized filenames for log files.

        Args:
            name: descriptive string for this log file.
            suffix: suffix of the file. Usually it is .out of .err.
            unique: if true, a counter will be attached to `name` to
                ensure the returned filename is not already used.

        Returns:
            A tuple of two file names for redirecting (stdout, stderr).
        """
        # strip if the suffix is something like .out.
        suffix = suffix.strip(".")

        if unique:
            filename = self._make_inc_temp(
                suffix=f".{suffix}", prefix=name, directory_name=self._logs_dir
            )
        else:
            filename = os.path.join(self._logs_dir, f"{name}.{suffix}")
        return filename

    def _get_unused_port(self, allocated_ports=None):
        if allocated_ports is None:
            allocated_ports = set()

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("", 0))
        port = s.getsockname()[1]

        # Try to generate a port that is far above the 'next available' one.
        # This solves issue #8254 where GRPC fails because the port assigned
        # from this method has been used by a different process.
        for _ in range(ray_constants.NUM_PORT_RETRIES):
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

    def _prepare_socket_file(self, socket_path: str, default_prefix: str):
        """Prepare the socket file for raylet and plasma.

        This method helps to prepare a socket file.
        1. Make the directory if the directory does not exist.
        2. If the socket file exists, do nothing (this just means we aren't the
           first worker on the node).

        Args:
            socket_path: the socket file to prepare.
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
            port_name: the name of the port, e.g. metrics_export_port
            default_port (Optional[int]): The port to return and cache if no
            port has already been cached for the given port_name.  If None, an
            unused port is generated and cached.
        Returns:
            port: the port number.
        """
        file_path = os.path.join(self.get_session_dir_path(), "ports_by_node.json")

        # Make sure only the ports in RAY_CACHED_PORTS are cached.
        assert port_name in ray_constants.RAY_ALLOWED_CACHED_PORTS

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
                allocated_ports = set(ports_by_node[self.unique_id].values())

                if default_port is not None and default_port in allocated_ports:
                    # The default port is already in use, so don't use it.
                    default_port = None

                port = default_port or self._get_unused_port(allocated_ports)

                ports_by_node[self.unique_id][port_name] = port
                with open(file_path, "w") as f:
                    json.dump(ports_by_node, f)

        return port

    def _wait_and_get_for_node_address(self, timeout_s: int = 60) -> str:
        """Wait until the RAY_NODE_IP_FILENAME file is avialable.

        RAY_NODE_IP_FILENAME is created when a ray instance is started.

        Args:
            timeout_s: If the ip address is not found within this
                timeout, it will raise ValueError.
        Returns:
            The node_ip_address of the current session if it finds it
            within timeout_s.
        """
        for i in range(timeout_s):
            node_ip_address = ray._private.services.get_cached_node_ip_address(
                self.get_session_dir_path()
            )

            if node_ip_address is not None:
                return node_ip_address

            time.sleep(1)
            if i % 10 == 0:
                logger.info(
                    f"Can't find a `{ray_constants.RAY_NODE_IP_FILENAME}` "
                    f"file from {self.get_session_dir_path()}. "
                    "Have you started Ray instance using "
                    "`ray start` or `ray.init`?"
                )

        raise ValueError(
            f"Can't find a `{ray_constants.RAY_NODE_IP_FILENAME}` "
            f"file from {self.get_session_dir_path()}. "
            f"for {timeout_s} seconds. "
            "A ray instance hasn't started. "
            "Did you do `ray start` or `ray.init` on this host?"
        )

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

    def start_log_monitor(self):
        """Start the log monitor."""
        # Only redirect logs to .err. .err file is only useful when the
        # component has an unexpected output to stdout/stderr.
        _, stderr_file = self.get_log_file_handles(
            "log_monitor", unique=True, create_out=False
        )
        process_info = ray._private.services.start_log_monitor(
            self.get_session_dir_path(),
            self._logs_dir,
            self.gcs_address,
            fate_share=self.kernel_fate_share,
            max_bytes=self.max_bytes,
            backup_count=self.backup_count,
            redirect_logging=self.should_redirect_logs(),
            stdout_file=stderr_file,
            stderr_file=stderr_file,
        )
        assert ray_constants.PROCESS_TYPE_LOG_MONITOR not in self.all_processes
        self.all_processes[ray_constants.PROCESS_TYPE_LOG_MONITOR] = [
            process_info,
        ]

    def start_api_server(
        self, *, include_dashboard: Optional[bool], raise_on_failure: bool
    ):
        """Start the dashboard.

        Args:
            include_dashboard: If true, this will load all dashboard-related modules
                when starting the API server. Otherwise, it will only
                start the modules that are not relevant to the dashboard.
            raise_on_failure: If true, this will raise an exception
                if we fail to start the API server. Otherwise it will print
                a warning if we fail to start the API server.
        """
        # Only redirect logs to .err. .err file is only useful when the
        # component has an unexpected output to stdout/stderr.
        _, stderr_file = self.get_log_file_handles(
            "dashboard", unique=True, create_out=False
        )
        self._webui_url, process_info = ray._private.services.start_api_server(
            include_dashboard,
            raise_on_failure,
            self._ray_params.dashboard_host,
            self.gcs_address,
            self.cluster_id.hex(),
            self._node_ip_address,
            self._temp_dir,
            self._logs_dir,
            self._session_dir,
            port=self._ray_params.dashboard_port,
            dashboard_grpc_port=self._ray_params.dashboard_grpc_port,
            fate_share=self.kernel_fate_share,
            max_bytes=self.max_bytes,
            backup_count=self.backup_count,
            redirect_logging=self.should_redirect_logs(),
            stdout_file=stderr_file,
            stderr_file=stderr_file,
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
            self.session_name,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            redis_username=self._ray_params.redis_username,
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

    def start_raylet(
        self,
        plasma_directory: str,
        object_store_memory: int,
        use_valgrind: bool = False,
        use_profiler: bool = False,
    ):
        """Start the raylet.

        Args:
            use_valgrind: True if we should start the process in
                valgrind.
            use_profiler: True if we should start the process in the
                valgrind profiler.
        """
        stdout_file, stderr_file = self.get_log_file_handles("raylet", unique=True)
        process_info = ray._private.services.start_raylet(
            self.redis_address,
            self.gcs_address,
            self._node_id,
            self._node_ip_address,
            self._ray_params.node_manager_port,
            self._raylet_socket_name,
            self._plasma_store_socket_name,
            self.cluster_id.hex(),
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
            self.session_name,
            is_head_node=self.is_head(),
            min_worker_port=self._ray_params.min_worker_port,
            max_worker_port=self._ray_params.max_worker_port,
            worker_port_list=self._ray_params.worker_port_list,
            object_manager_port=self._ray_params.object_manager_port,
            redis_username=self._ray_params.redis_username,
            redis_password=self._ray_params.redis_password,
            metrics_agent_port=self._ray_params.metrics_agent_port,
            runtime_env_agent_port=self._ray_params.runtime_env_agent_port,
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
            ray_debugger_external=self._ray_params.ray_debugger_external,
            env_updates=self._ray_params.env_vars,
            node_name=self._ray_params.node_name,
            webui=self._webui_url,
            labels=self._get_node_labels(),
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
        from ray.autoscaler.v2.utils import is_autoscaler_v2

        stdout_file, stderr_file = self.get_log_file_handles("monitor", unique=True)
        process_info = ray._private.services.start_monitor(
            self.gcs_address,
            self._logs_dir,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            autoscaling_config=self._ray_params.autoscaling_config,
            fate_share=self.kernel_fate_share,
            max_bytes=self.max_bytes,
            backup_count=self.backup_count,
            monitor_ip=self._node_ip_address,
            autoscaler_v2=is_autoscaler_v2(fetch_from_server=True),
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
            redis_username=self._ray_params.redis_username,
            redis_password=self._ray_params.redis_password,
            fate_share=self.kernel_fate_share,
            runtime_env_agent_address=self.runtime_env_agent_address,
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
        import ray._private.usage.usage_lib as ray_usage_lib

        ray_usage_lib.put_cluster_metadata(
            self.get_gcs_client(), ray_init_cluster=self.ray_init_cluster
        )
        # Make sure GCS is up.
        added = self.get_gcs_client().internal_kv_put(
            b"session_name",
            self._session_name.encode(),
            False,
            ray_constants.KV_NAMESPACE_SESSION,
        )
        if not added:
            curr_val = self.get_gcs_client().internal_kv_get(
                b"session_name", ray_constants.KV_NAMESPACE_SESSION
            )
            assert curr_val == self._session_name.encode("utf-8"), (
                f"Session name {self._session_name} does not match "
                f"persisted value {curr_val}. Perhaps there was an "
                f"error connecting to Redis."
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
        if self._ray_params.storage is not None:
            self.get_gcs_client().internal_kv_put(
                b"storage",
                self._ray_params.storage.encode(),
                True,
                ray_constants.KV_NAMESPACE_SESSION,
            )
        # Add tracing_startup_hook to redis / internal kv manually
        # since internal kv is not yet initialized.
        if self._ray_params.tracing_startup_hook:
            self.get_gcs_client().internal_kv_put(
                b"tracing_startup_hook",
                self._ray_params.tracing_startup_hook.encode(),
                True,
                ray_constants.KV_NAMESPACE_TRACING,
            )

    def start_head_processes(self):
        """Start head processes on the node."""
        logger.debug(
            f"Process STDOUT and STDERR is being " f"redirected to {self._logs_dir}."
        )
        assert self._gcs_address is None
        assert self._gcs_client is None

        self.start_gcs_server()
        assert self.get_gcs_client() is not None
        self._write_cluster_info_to_kv()

        if not self._ray_params.no_monitor:
            self.start_monitor()

        if self._ray_params.ray_client_server_port:
            self.start_ray_client_server()

        if self._ray_params.include_dashboard is None:
            # Default
            raise_on_api_server_failure = False
        else:
            raise_on_api_server_failure = self._ray_params.include_dashboard

        self.start_api_server(
            include_dashboard=self._ray_params.include_dashboard,
            raise_on_failure=raise_on_api_server_failure,
        )

    def start_ray_processes(self):
        """Start all of the processes on the node."""
        logger.debug(
            f"Process STDOUT and STDERR is being " f"redirected to {self._logs_dir}."
        )

        if not self.head:
            # Get the system config from GCS first if this is a non-head node.
            gcs_options = ray._raylet.GcsClientOptions.create(
                self.gcs_address,
                self.cluster_id.hex(),
                allow_cluster_id_nil=False,
                fetch_cluster_id_if_nil=False,
            )
            global_state = ray._private.state.GlobalState()
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
        self,
        process_type,
        allow_graceful: bool = False,
        check_alive: bool = True,
        wait: bool = False,
    ):
        """Kill a process of a given type.

        If the process type is PROCESS_TYPE_REDIS_SERVER, then we will kill all
        of the Redis servers.

        If the process was started in valgrind, then we will raise an exception
        if the process has a non-zero exit code.

        Args:
            process_type: The type of the process to kill.
            allow_graceful: Send a SIGTERM first and give the process
                time to exit gracefully. If that doesn't work, then use
                SIGKILL. We usually want to do this outside of tests.
            check_alive: If true, then we expect the process to be alive
                and will raise an exception if the process is already dead.
            wait: If true, then this method will not return until the
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

    def kill_redis(self, check_alive: bool = True):
        """Kill the Redis servers.

        Args:
            check_alive: Raise an exception if any of the processes
                were already dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_REDIS_SERVER, check_alive=check_alive
        )

    def kill_raylet(self, check_alive: bool = True):
        """Kill the raylet.

        Args:
            check_alive: Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_RAYLET, check_alive=check_alive
        )

    def kill_log_monitor(self, check_alive: bool = True):
        """Kill the log monitor.

        Args:
            check_alive: Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_LOG_MONITOR, check_alive=check_alive
        )

    def kill_reporter(self, check_alive: bool = True):
        """Kill the reporter.

        Args:
            check_alive: Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_REPORTER, check_alive=check_alive
        )

    def kill_dashboard(self, check_alive: bool = True):
        """Kill the dashboard.

        Args:
            check_alive: Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_DASHBOARD, check_alive=check_alive
        )

    def kill_monitor(self, check_alive: bool = True):
        """Kill the monitor.

        Args:
            check_alive: Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_MONITOR, check_alive=check_alive
        )

    def kill_gcs_server(self, check_alive: bool = True):
        """Kill the gcs server.

        Args:
            check_alive: Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_GCS_SERVER, check_alive=check_alive, wait=True
        )
        # Clear GCS client and address to indicate no GCS server is running.
        self._gcs_address = None
        self._gcs_client = None

    def kill_reaper(self, check_alive: bool = True):
        """Kill the reaper process.

        Args:
            check_alive: Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_REAPER, check_alive=check_alive
        )

    def kill_all_processes(self, check_alive=True, allow_graceful=False, wait=False):
        """Kill all of the processes.

        Note that This is slower than necessary because it calls kill, wait,
        kill, wait, ... instead of kill, kill, ..., wait, wait, ...

        Args:
            check_alive: Raise an exception if any of the processes were
                already dead.
            wait: If true, then this method will not return until the
                process in question has exited.
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
                wait=wait,
            )

        if ray_constants.PROCESS_TYPE_GCS_SERVER in self.all_processes:
            self._kill_process_type(
                ray_constants.PROCESS_TYPE_GCS_SERVER,
                check_alive=check_alive,
                allow_graceful=allow_graceful,
                wait=wait,
            )

        # We call "list" to copy the keys because we are modifying the
        # dictionary while iterating over it.
        for process_type in list(self.all_processes.keys()):
            # Need to kill the reaper process last in case we die unexpectedly
            # while cleaning up.
            if process_type != ray_constants.PROCESS_TYPE_REAPER:
                self._kill_process_type(
                    process_type,
                    check_alive=check_alive,
                    allow_graceful=allow_graceful,
                    wait=wait,
                )

        if ray_constants.PROCESS_TYPE_REAPER in self.all_processes:
            self._kill_process_type(
                ray_constants.PROCESS_TYPE_REAPER,
                check_alive=check_alive,
                allow_graceful=allow_graceful,
                wait=wait,
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
            from ray._private import external_storage

            storage = external_storage.setup_external_storage(
                object_spilling_config, self._node_id, self._session_name
            )
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

        if not object_spilling_config:
            object_spilling_config = os.environ.get("RAY_object_spilling_config", "")

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
        from ray._private import external_storage

        # Node ID is available only after GCS is connected. However,
        # validate_external_storage() needs to be called before it to
        # be able to validate the configs early. Therefore, we use a
        # dummy node ID here and make sure external storage can be set
        # up based on the provided config. This storage is destroyed
        # right after the validation.
        dummy_node_id = ray.NodeID.from_random().hex()
        storage = external_storage.setup_external_storage(
            deserialized_config, dummy_node_id, self._session_name
        )
        storage.destroy_external_storage()
        external_storage.reset_external_storage()

    def _record_stats(self):
        # This is only called when a new node is started.
        # Initialize the internal kv so that the metrics can be put
        from ray._private.usage.usage_lib import (
            TagKey,
            record_extra_usage_tag,
            record_hardware_usage,
        )

        if not ray.experimental.internal_kv._internal_kv_initialized():
            ray.experimental.internal_kv._initialize_internal_kv(self.get_gcs_client())
        assert ray.experimental.internal_kv._internal_kv_initialized()
        if self.head:
            # record head node stats
            gcs_storage_type = (
                "redis" if os.environ.get("RAY_REDIS_ADDRESS") is not None else "memory"
            )
            record_extra_usage_tag(TagKey.GCS_STORAGE, gcs_storage_type)
        cpu_model_name = ray._private.utils.get_current_node_cpu_model_name()
        if cpu_model_name:
            # CPU model name can be an arbitrary long string
            # so we truncate it to the first 50 characters
            # to avoid any issues.
            record_hardware_usage(cpu_model_name[:50])
