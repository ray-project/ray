from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import os

import ray.ray_constants as ray_constants


class RayParams(object):
    """A class used to store the parameters used by Ray.

    Attributes:
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
        use_credis: If True, additionally load the chain-replicated libraries
            into the redis servers.  Defaults to None, which means its value is
            set by the presence of "RAY_USE_NEW_GCS" in os.environ.
        num_cpus (int): Number of cpus the user wishes all local schedulers to
            be configured with.
        num_gpus (int): Number of gpus the user wishes all local schedulers to
            be configured with.
        resources: A dictionary mapping the name of a resource to the quantity
            of that resource available.
        object_store_memory: The amount of memory (in bytes) to start the
            object store with.
        redis_max_memory: The max amount of memory (in bytes) to allow redis
            to use, or None for no limit. Once the limit is exceeded, redis
            will start LRU eviction of entries. This only applies to the
            sharded redis tables (task and object tables).
        object_manager_port (int): The port to use for the object managers.
            There should be one per object manager being started on this node
            (typically just one).
        node_manager_port (int): The port to use for the node managers.
            There should be one per node manager being started on this node
            (typically just one).
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
            UI, which is a Jupyter notebook.
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
                 start_ray_local=False,
                 redis_address=None,
                 num_cpus=None,
                 num_gpus=None,
                 resources=None,
                 object_store_memory=None,
                 redis_max_memory=None,
                 redis_port=None,
                 redis_shard_ports=None,
                 use_credis=None,
                 object_manager_port=None,
                 node_manager_port=None,
                 node_ip_address=None,
                 object_id_seed=None,
                 num_workers=None,
                 local_mode=False,
                 driver_mode=None,
                 redirect_worker_output=False,
                 redirect_output=True,
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
                 _internal_config=None):
        if num_workers is not None:
            raise Exception(
                "The 'num_workers' argument is deprecated. Please use "
                "'num_cpus' instead.")
        self.start_ray_local = start_ray_local
        self.object_id_seed = object_id_seed

        # Node IP address
        self.node_ip_address = node_ip_address

        # Resources settings.
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.resources = resources

        # Redis settings
        self.redis_address = redis_address
        self.redis_port = redis_port
        self.redis_password = redis_password
        self.redis_max_clients = redis_max_clients
        self.redis_max_memory = redis_max_memory
        self.num_redis_shards = num_redis_shards
        self.redis_shard_ports = redis_shard_ports
        if use_credis is None:
            self.use_credis = ("RAY_USE_NEW_GCS" in os.environ)
        else:
            self.use_credis = use_credis

        # Plasma object store settings.
        self.plasma_directory = plasma_directory
        self.plasma_store_socket_name = plasma_store_socket_name
        self.object_store_memory = object_store_memory
        self.huge_pages = huge_pages

        # Raylet settings
        self.raylet_socket_name = raylet_socket_name
        self.object_manager_port = object_manager_port
        self.node_manager_port = node_manager_port
        self.num_workers = num_workers

        # WebUI settings
        self.include_webui = include_webui

        # Logging settings
        self.redirect_output = redirect_output
        self.redirect_worker_output = redirect_worker_output

        self.worker_path = worker_path
        self.local_mode = local_mode
        self.driver_mode = driver_mode

        self.temp_dir = temp_dir
        self.include_log_monitor = include_log_monitor
        self.autoscaling_config = autoscaling_config
        self._internal_config = _internal_config

    def update(self, **kwargs):
        """Update the settings according to the keyword arguments.

        Args:
            kwargs: The keyword arguments to set corresponding fields.
        """
        for arg in kwargs:
            if (hasattr(self, arg)):
                setattr(self, arg, kwargs[arg])
            else:
                raise ValueError("Invalid RayParams parameter in"
                                 " update: %s" % arg)

    @property
    def config(self):
        return json.loads(
            self._internal_config) if self._internal_config else None

    def update_if_absent(self, **kwargs):
        """Update the settings when the target fields are None.

        Args:
            kwargs: The keyword arguments to set corresponding fields.
        """
        for arg in kwargs:
            if (hasattr(self, arg)):
                if getattr(self, arg) is None:
                    setattr(self, arg, kwargs[arg])
            else:
                raise ValueError("Invalid RayParams parameter in"
                                 " update_if_absent: %s" % arg)
