from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

import ray.ray_constants as ray_constants


class RayParams(object):
    """A class used to store the parameters used by Ray.

    Attributes:
        redis_address (str): The address of the Redis server to connect to. If
            this address is not provided, then this command will start Redis, a
            raylet, a plasma store, a plasma manager, and some workers.
            It will also kill these processes when Python exits.
        redis_port (int): The port that the primary Redis shard should listen
            to. If None, then a random port will be chosen.
        redis_shard_ports: A list of the ports to use for the non-primary Redis
            shards.
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
        node_ip_address (str): The IP address of the node that we are on.
        object_id_seed (int): Used to seed the deterministic generation of
            object IDs. The same value can be used across multiple runs of the
            same job in order to generate the object IDs in a consistent
            manner. However, the same ID should not be used for different jobs.
        local_mode (bool): True if the code should be executed serially
            without Ray. This is useful for debugging.
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
        include_webui: Boolean flag indicating whether to start the web
            UI, which displays the status of the Ray cluster.
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
        include_java (bool): If True, the raylet backend can also support
            Java worker.
        java_worker_options (str): The command options for Java worker.
        load_code_from_local: Whether load code from local file or from GCS.
        _internal_config (str): JSON configuration for overriding
            RayConfig defaults. For testing purposes ONLY.
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
                 node_ip_address=None,
                 object_id_seed=None,
                 local_mode=False,
                 driver_mode=None,
                 redirect_worker_output=None,
                 redirect_output=None,
                 num_redis_shards=None,
                 redis_max_clients=None,
                 redis_password=None,
                 plasma_directory=None,
                 worker_path=None,
                 huge_pages=False,
                 include_webui=None,
                 logging_level=logging.INFO,
                 logging_format=ray_constants.LOGGER_FORMAT,
                 plasma_store_socket_name=None,
                 raylet_socket_name=None,
                 temp_dir=None,
                 include_log_monitor=None,
                 autoscaling_config=None,
                 include_java=False,
                 java_worker_options=None,
                 load_code_from_local=False,
                 _internal_config=None):
        self.object_id_seed = object_id_seed
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
        self.node_ip_address = node_ip_address
        self.local_mode = local_mode
        self.driver_mode = driver_mode
        self.redirect_worker_output = redirect_worker_output
        self.redirect_output = redirect_output
        self.num_redis_shards = num_redis_shards
        self.redis_max_clients = redis_max_clients
        self.redis_password = redis_password
        self.plasma_directory = plasma_directory
        self.worker_path = worker_path
        self.huge_pages = huge_pages
        self.include_webui = include_webui
        self.plasma_store_socket_name = plasma_store_socket_name
        self.raylet_socket_name = raylet_socket_name
        self.temp_dir = temp_dir
        self.include_log_monitor = include_log_monitor
        self.autoscaling_config = autoscaling_config
        self.include_java = include_java
        self.java_worker_options = java_worker_options
        self.load_code_from_local = load_code_from_local
        self._internal_config = _internal_config
        self._check_usage()

    def update(self, **kwargs):
        """Update the settings according to the keyword arguments.

        Args:
            kwargs: The keyword arguments to set corresponding fields.
        """
        for arg in kwargs:
            if hasattr(self, arg):
                setattr(self, arg, kwargs[arg])
            else:
                raise ValueError("Invalid RayParams parameter in"
                                 " update: %s" % arg)

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
