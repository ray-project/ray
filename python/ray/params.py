from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

import ray.ray_constants as ray_constants


class RayParams(object):
    """A class used to store the parameters used by Ray.

    Attributes:
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
        redis_address (str): The address of the Redis server to connect to. If
            this address is not provided, then this command will start Redis, a
            global scheduler, a local scheduler, a plasma store, a plasma
            manager, and some workers. It will also kill these processes when
            Python exits.
        redis_port (int): The port that the primary Redis shard should listen
            to. If None, then a random port will be chosen. If the key
            "redis_address" is in address_info, then this argument will be
            ignored.
        redis_shard_ports: A list of the ports to use for the non-primary Redis
            shards.
        num_cpus (int): Number of cpus the user wishes all local schedulers to
            be configured with.
        num_gpus (int): Number of gpus the user wishes all local schedulers to
            be configured with.
        num_local_schedulers (int): The number of local schedulers to start.
            This is only provided if start_ray_local is True.
        resources: A dictionary mapping the name of a resource to the quantity
            of that resource available.
        object_store_memory: The amount of memory (in bytes) to start the
            object store with.
        redis_max_memory: The max amount of memory (in bytes) to allow redis
            to use, or None for no limit. Once the limit is exceeded, redis
            will start LRU eviction of entries. This only applies to the
            sharded redis tables (task and object tables).
        object_manager_ports (list): A list of the ports to use for the object
            managers. There should be one per object manager being started on
            this node (typically just one).
        node_manager_ports (list): A list of the ports to use for the node
            managers. There should be one per node manager being started on
            this node (typically just one).
        collect_profiling_data: Whether to collect profiling data from workers.
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
        worker_path (str): The path of the source code that will be run by the
            worker.
        huge_pages: Boolean flag indicating whether to start the Object
            Store with hugetlbfs support. Requires plasma_directory.
        include_webui: Boolean flag indicating whether to start the web
            UI, which is a Jupyter notebook.
        driver_id: The ID of driver.
        configure_logging: True if allow the logging cofiguration here.
            Otherwise, the users may want to configure it by their own.
        logging_level: Logging level, default will be logging.INFO.
        logging_format: Logging format, default will be "%(message)s"
            which means only contains the message.
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
        _internal_config (str): JSON configuration for overriding
            RayConfig defaults. For testing purposes ONLY.
    """

    def __init__(self,
                 address_info=None,
                 start_ray_local=False,
                 redis_address=None,
                 num_cpus=None,
                 num_gpus=None,
                 num_local_schedulers=None,
                 resources=None,
                 object_store_memory=None,
                 redis_max_memory=None,
                 redis_port=None,
                 redis_shard_ports=None,
                 object_manager_ports=None,
                 node_manager_ports=None,
                 collect_profiling_data=True,
                 node_ip_address=None,
                 object_id_seed=None,
                 num_workers=None,
                 local_mode=False,
                 driver_mode=None,
                 redirect_worker_output=False,
                 redirect_output=True,
                 ignore_reinit_error=False,
                 num_redis_shards=None,
                 redis_max_clients=None,
                 redis_password=None,
                 plasma_directory=None,
                 worker_path=None,
                 huge_pages=False,
                 include_webui=True,
                 driver_id=None,
                 configure_logging=True,
                 logging_level=logging.INFO,
                 logging_format=ray_constants.LOGGER_FORMAT,
                 plasma_store_socket_name=None,
                 raylet_socket_name=None,
                 temp_dir=None,
                 include_log_monitor=None,
                 _internal_config=None,
                 autoscaling_config=None):
        self.address_info = address_info
        self.start_ray_local = start_ray_local
        self.object_id_seed = object_id_seed
        self.redis_address = redis_address
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.num_local_schedulers = num_local_schedulers
        self.resources = resources
        self.object_store_memory = object_store_memory
        self.redis_max_memory = redis_max_memory
        self.redis_port = redis_port
        self.redis_shard_ports = redis_shard_ports
        self.object_manager_ports = object_manager_ports
        self.node_manager_ports = node_manager_ports
        self.collect_profiling_data = collect_profiling_data
        self.node_ip_address = node_ip_address
        self.num_workers = num_workers
        self.local_mode = local_mode
        self.driver_mode = driver_mode
        self.redirect_worker_output = redirect_worker_output
        self.redirect_output = redirect_output
        self.ignore_reinit_error = ignore_reinit_error
        self.num_redis_shards = num_redis_shards
        self.redis_max_clients = redis_max_clients
        self.redis_password = redis_password
        self.plasma_directory = plasma_directory
        self.worker_path = worker_path
        self.huge_pages = huge_pages
        self.include_webui = include_webui
        self.driver_id = driver_id
        self.configure_logging = configure_logging
        self.logging_level = logging_level
        self.logging_format = logging_format
        self.plasma_store_socket_name = plasma_store_socket_name
        self.raylet_socket_name = raylet_socket_name
        self.temp_dir = temp_dir
        self.include_log_monitor = include_log_monitor
        self._internal_config = _internal_config
        self.autoscaling_config = autoscaling_config

    def apply_settings(self, **kw_args):
        for arg in kw_args:
            if (hasattr(self, arg)):
                print("Apply value %s to key %s" % (kw_args[arg], arg))
                setattr(self, arg, kw_args[arg])
