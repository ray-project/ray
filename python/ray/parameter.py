import logging
import os

import numpy as np

import ray.ray_constants as ray_constants

logger = logging.getLogger(__name__)


class RayParams:
    """A class used to store the parameters used by Ray.

    Attributes:
        redis_address (str): The address of the Redis server to connect to. If
            this address is not provided, then this command will start Redis, a
            raylet, a plasma store, a plasma manager, and some workers.
            It will also kill these processes when Python exits.
        redis_port (int): The port that the primary Redis shard should listen
            to. If None, then it will fall back to
            ray.ray_constants.DEFAULT_PORT, or a random port if the default is
            not available.
        redis_shard_ports: A list of the ports to use for the non-primary Redis
            shards. If None, then it will fall back to the ports right after
            redis_port, or random ports if those are not available.
        num_cpus (int): Number of CPUs to configure the raylet with.
        num_gpus (int): Number of GPUs to configure the raylet with.
        resources: A dictionary mapping the name of a resource to the quantity
            of that resource available.
        memory: Total available memory for workers requesting memory.
        object_store_memory: The amount of memory (in bytes) to start the
            object store with.
        redis_max_memory: The max amount of memory (in bytes) to allow redis
            to use, or None for no limit. Once the limit is exceeded, redis
            will start LRU eviction of entries. This only applies to the
            sharded redis tables (task and object tables).
        object_manager_port int: The port to use for the object manager.
        node_manager_port: The port to use for the node manager.
        gcs_server_port: The port to use for the GCS server.
        node_ip_address (str): The IP address of the node that we are on.
        raylet_ip_address (str): The IP address of the raylet that this node
            connects to.
        min_worker_port (int): The lowest port number that workers will bind
            on. If not set or set to 0, random ports will be chosen.
        max_worker_port (int): The highest port number that workers will bind
            on. If set, min_worker_port must also be set.
        worker_port_list (str): An explicit list of ports to be used for
            workers (comma-separated). Overrides min_worker_port and
            max_worker_port.
        ray_client_server_port (int): The port number the ray client server
            will bind on. If not set, the ray client server will not
            be started.
        object_ref_seed (int): Used to seed the deterministic generation of
            object refs. The same value can be used across multiple runs of the
            same job in order to generate the object refs in a consistent
            manner. However, the same ID should not be used for different jobs.
        redirect_worker_output: True if the stdout and stderr of worker
            processes should be redirected to files.
        redirect_output (bool): True if stdout and stderr for non-worker
            processes should be redirected to files and false otherwise.
        num_redis_shards: The number of Redis shards to start in addition to
            the primary Redis shard.
        redis_max_clients: If provided, attempt to configure Redis with this
            maxclients number.
        redis_password (str): Prevents external clients without the password
            from connecting to Redis if provided.
        plasma_directory: A directory where the Plasma memory mapped files will
            be created.
        worker_path (str): The path of the source code that will be run by the
            worker.
        huge_pages: Boolean flag indicating whether to start the Object
            Store with hugetlbfs support. Requires plasma_directory.
        include_dashboard: Boolean flag indicating whether to start the web
            UI, which displays the status of the Ray cluster. If this value is
            None, then the UI will be started if the relevant dependencies are
            present.
        dashboard_host: The host to bind the web UI server to. Can either be
            localhost (127.0.0.1) or 0.0.0.0 (available from all interfaces).
            By default, this is set to localhost to prevent access from
            external machines.
        dashboard_port: The port to bind the dashboard server to.
            Defaults to 8265.
        logging_level: Logging level, default will be logging.INFO.
        logging_format: Logging format, default contains a timestamp,
            filename, line number, and message. See ray_constants.py.
        plasma_store_socket_name (str): If provided, it will specify the socket
            name used by the plasma store.
        raylet_socket_name (str): If provided, it will specify the socket path
            used by the raylet process.
        temp_dir (str): If provided, it will specify the root temporary
            directory for the Ray process.
        include_log_monitor (bool): If True, then start a log monitor to
            monitor the log files for all processes on this node and push their
            contents to Redis.
        autoscaling_config: path to autoscaling config file.
        java_worker_options (list): The command options for Java worker.
        metrics_agent_port(int): The port to bind metrics agent.
        metrics_export_port(int): The port at which metrics are exposed
            through a Prometheus endpoint.
        no_monitor(bool): If True, the ray autoscaler monitor for this cluster
            will not be started.
        _system_config (dict): Configuration for overriding RayConfig
            defaults. Used to set system configuration and for experimental Ray
            core feature flags.
        lru_evict (bool): Enable LRU eviction if space is needed.
        enable_object_reconstruction (bool): Enable plasma reconstruction on
            failure.
        start_initial_python_workers_for_first_job (bool): If true, start
            initial Python workers for the first job on the node.
    """

    def __init__(self,
                 redis_address=None,
                 num_cpus=None,
                 num_gpus=None,
                 resources=None,
                 memory=None,
                 object_store_memory=None,
                 redis_max_memory=None,
                 redis_port=None,
                 redis_shard_ports=None,
                 object_manager_port=None,
                 node_manager_port=None,
                 gcs_server_port=None,
                 node_ip_address=None,
                 raylet_ip_address=None,
                 min_worker_port=None,
                 max_worker_port=None,
                 worker_port_list=None,
                 ray_client_server_port=None,
                 object_ref_seed=None,
                 driver_mode=None,
                 redirect_worker_output=None,
                 redirect_output=None,
                 num_redis_shards=None,
                 redis_max_clients=None,
                 redis_password=ray_constants.REDIS_DEFAULT_PASSWORD,
                 plasma_directory=None,
                 worker_path=None,
                 huge_pages=False,
                 include_dashboard=None,
                 dashboard_host=ray_constants.DEFAULT_DASHBOARD_IP,
                 dashboard_port=ray_constants.DEFAULT_DASHBOARD_PORT,
                 logging_level=logging.INFO,
                 logging_format=ray_constants.LOGGER_FORMAT,
                 plasma_store_socket_name=None,
                 raylet_socket_name=None,
                 temp_dir=None,
                 include_log_monitor=None,
                 autoscaling_config=None,
                 java_worker_options=None,
                 start_initial_python_workers_for_first_job=False,
                 _system_config=None,
                 enable_object_reconstruction=False,
                 metrics_agent_port=None,
                 metrics_export_port=None,
                 no_monitor=False,
                 lru_evict=False):
        self.object_ref_seed = object_ref_seed
        self.redis_address = redis_address
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.memory = memory
        self.object_store_memory = object_store_memory
        self.resources = resources
        self.redis_max_memory = redis_max_memory
        self.redis_port = redis_port
        self.redis_shard_ports = redis_shard_ports
        self.object_manager_port = object_manager_port
        self.node_manager_port = node_manager_port
        self.gcs_server_port = gcs_server_port
        self.node_ip_address = node_ip_address
        self.raylet_ip_address = raylet_ip_address
        self.min_worker_port = min_worker_port
        self.max_worker_port = max_worker_port
        self.worker_port_list = worker_port_list
        self.ray_client_server_port = ray_client_server_port
        self.driver_mode = driver_mode
        self.redirect_worker_output = redirect_worker_output
        self.redirect_output = redirect_output
        self.num_redis_shards = num_redis_shards
        self.redis_max_clients = redis_max_clients
        self.redis_password = redis_password
        self.plasma_directory = plasma_directory
        self.worker_path = worker_path
        self.huge_pages = huge_pages
        self.include_dashboard = include_dashboard
        self.dashboard_host = dashboard_host
        self.dashboard_port = dashboard_port
        self.plasma_store_socket_name = plasma_store_socket_name
        self.raylet_socket_name = raylet_socket_name
        self.temp_dir = temp_dir
        self.include_log_monitor = include_log_monitor
        self.autoscaling_config = autoscaling_config
        self.java_worker_options = java_worker_options
        self.metrics_agent_port = metrics_agent_port
        self.metrics_export_port = metrics_export_port
        self.no_monitor = no_monitor
        self.start_initial_python_workers_for_first_job = (
            start_initial_python_workers_for_first_job)
        self._system_config = _system_config or {}
        self._lru_evict = lru_evict
        self._enable_object_reconstruction = enable_object_reconstruction
        self._check_usage()

        # Set the internal config options for LRU eviction.
        if lru_evict:
            # Turn off object pinning.
            if self._system_config is None:
                self._system_config = dict()
            if self._system_config.get("object_pinning_enabled", False):
                raise Exception(
                    "Object pinning cannot be enabled if using LRU eviction.")
            self._system_config["object_pinning_enabled"] = False
            self._system_config["free_objects_period_milliseconds"] = 1000

        # Set the internal config options for object reconstruction.
        if enable_object_reconstruction:
            # Turn off object pinning.
            if self._system_config is None:
                self._system_config = dict()
            if lru_evict:
                raise Exception(
                    "Object reconstruction cannot be enabled if using LRU "
                    "eviction.")
            print(self._system_config)
            self._system_config["lineage_pinning_enabled"] = True
            self._system_config["free_objects_period_milliseconds"] = -1

    def update(self, **kwargs):
        """Update the settings according to the keyword arguments.

        Args:
            kwargs: The keyword arguments to set corresponding fields.
        """
        for arg in kwargs:
            if hasattr(self, arg):
                setattr(self, arg, kwargs[arg])
            else:
                raise ValueError(
                    f"Invalid RayParams parameter in update: {arg}")

        self._check_usage()

    def update_if_absent(self, **kwargs):
        """Update the settings when the target fields are None.

        Args:
            kwargs: The keyword arguments to set corresponding fields.
        """
        for arg in kwargs:
            if hasattr(self, arg):
                if getattr(self, arg) is None:
                    setattr(self, arg, kwargs[arg])
            else:
                raise ValueError("Invalid RayParams parameter in"
                                 " update_if_absent: %s" % arg)

        self._check_usage()

    def _check_usage(self):
        if self.worker_port_list is not None:
            for port_str in self.worker_port_list.split(","):
                try:
                    port = int(port_str)
                except ValueError as e:
                    raise ValueError(
                        "worker_port_list must be a comma-separated " +
                        "list of integers: {}".format(e)) from None

                if port < 1024 or port > 65535:
                    raise ValueError(
                        "Ports in worker_port_list must be "
                        "between 1024 and 65535. Got: {}".format(port))

        # Used primarily for testing.
        if os.environ.get("RAY_USE_RANDOM_PORTS", False):
            if self.min_worker_port is None and self.min_worker_port is None:
                self.min_worker_port = 0
                self.max_worker_port = 0

        if self.min_worker_port is not None:
            if self.min_worker_port != 0 and (self.min_worker_port < 1024
                                              or self.min_worker_port > 65535):
                raise ValueError("min_worker_port must be 0 or an integer "
                                 "between 1024 and 65535.")

        if self.max_worker_port is not None:
            if self.min_worker_port is None:
                raise ValueError("If max_worker_port is set, min_worker_port "
                                 "must also be set.")
            elif self.max_worker_port != 0:
                if self.max_worker_port < 1024 or self.max_worker_port > 65535:
                    raise ValueError(
                        "max_worker_port must be 0 or an integer between "
                        "1024 and 65535.")
                elif self.max_worker_port <= self.min_worker_port:
                    raise ValueError("max_worker_port must be higher than "
                                     "min_worker_port.")

        if self.resources is not None:
            assert "CPU" not in self.resources, (
                "'CPU' should not be included in the resource dictionary. Use "
                "num_cpus instead.")
            assert "GPU" not in self.resources, (
                "'GPU' should not be included in the resource dictionary. Use "
                "num_gpus instead.")

        if self.redirect_worker_output is not None:
            raise DeprecationWarning(
                "The redirect_worker_output argument is deprecated. To "
                "control logging to the driver, use the 'log_to_driver' "
                "argument to 'ray.init()'")

        if self.redirect_output is not None:
            raise DeprecationWarning(
                "The redirect_output argument is deprecated.")

        # Parse the numpy version.
        numpy_version = np.__version__.split(".")
        numpy_major, numpy_minor = int(numpy_version[0]), int(numpy_version[1])
        if numpy_major <= 1 and numpy_minor < 16:
            logger.warning("Using ray with numpy < 1.16.0 will result in slow "
                           "serialization. Upgrade numpy if using with ray.")
