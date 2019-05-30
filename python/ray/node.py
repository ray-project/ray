from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import binascii
import collections
import datetime
import errno
import json
import os
import logging
import multiprocessing
import redis
import sys
import tempfile
import time

import ray
import ray.ray_constants as ray_constants
import ray.services
from ray.utils import try_to_create_directory

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray configures it by default automatically
# using logging.basicConfig in its entry/init points.
logger = logging.getLogger(__name__)

PY3 = sys.version_info.major >= 3

# Location of the redis server and module.
RAY_HOME = os.path.join(os.path.dirname(__file__), "../..")

REDIS_EXECUTABLE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "core/src/ray/thirdparty/redis/src/redis-server")
REDIS_MODULE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "core/src/ray/gcs/redis_module/libray_redis_module.so")

# Location of the credis server and modules.
# credis will be enabled if the environment variable RAY_USE_NEW_GCS is set.
CREDIS_EXECUTABLE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "core/src/credis/redis/src/redis-server")
CREDIS_MASTER_MODULE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "core/src/credis/build/src/libmaster.so")
CREDIS_MEMBER_MODULE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "core/src/credis/build/src/libmember.so")

# Location of the plasma object store executable.
PLASMA_STORE_EXECUTABLE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "core/src/plasma/plasma_store_server")

RAYLET_MONITOR_EXECUTABLE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "core/src/ray/raylet/raylet_monitor")

# Location of the raylet executables.
RAYLET_EXECUTABLE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "core/src/ray/raylet/raylet")

RUN_PLASMA_STORE_PROFILER = False
RUN_PLASMA_STORE_VALGRIND = True
# True if processes are run in the valgrind profiler.
RUN_RAYLET_PROFILER = False

DEFAULT_JAVA_WORKER_OPTIONS = "-classpath {}".format(
    os.path.join(
        os.path.abspath(os.path.dirname(__file__)), "../../../build/java/*"))


class Node(object):
    """An encapsulation of the Ray processes on a single node.

    This class is responsible for starting Ray processes and killing them,
    and it also controls the temp file policy.
    """

    def __init__(self,
                 ray_params,
                 head=False,
                 shutdown_at_exit=True,
                 connect_only=False):
        """Start a node.

        Args:
            ray_params (ray.params.RayParams): The parameters to use to
                configure the node.
            head (bool): True if this is the head node, which means it will
                start additional processes like the Redis servers, monitor
                processes, and web UI.
            shutdown_at_exit (bool): If true, a handler will be registered to
                shutdown the processes started here when the Python interpreter
                exits.
            connect_only (bool): If true, connect to the node without starting
                new processes.
        """
        if shutdown_at_exit and connect_only:
            raise ValueError("'shutdown_at_exit' and 'connect_only' cannot "
                             "be both true.")
        self.head = head
        self._process_control = ray.services.ProcessControl(shutdown_at_exit)
        self._process_control.register_process_type(
            ray_constants.PROCESS_TYPE_REDIS_SERVER, unique=False)
        self._process_control.register_process_type(
            ray_constants.PROCESS_TYPE_MONITOR, unique=True)
        self._process_control.register_process_type(
            ray_constants.PROCESS_TYPE_RAYLET_MONITOR, unique=True)
        self._process_control.register_process_type(
            ray_constants.PROCESS_TYPE_LOG_MONITOR, unique=True)
        self._process_control.register_process_type(
            ray_constants.PROCESS_TYPE_REPORTER, unique=True)
        self._process_control.register_process_type(
            ray_constants.PROCESS_TYPE_DASHBOARD, unique=True)
        self._process_control.register_process_type(
            ray_constants.PROCESS_TYPE_PLASMA_STORE, unique=True)
        self._process_control.register_process_type(
            ray_constants.PROCESS_TYPE_RAYLET, unique=True)
        self._process_control.register_process_type(
            ray_constants.PROCESS_TYPE_WORKER, unique=False)
        # Kill the raylet first. This is important for suppressing errors at
        # shutdown because we give the raylet a chance to exit gracefully and
        # clean up its child worker processes. If we were to kill the plasma
        # store (or Redis) first, that could cause the raylet to exit
        # ungracefully, leading to more verbose output from the workers.
        self._process_control.set_teardown_order([
            ray_constants.PROCESS_TYPE_RAYLET])

        # Try to get node IP address with the parameters.
        if ray_params.node_ip_address:
            node_ip_address = ray_params.node_ip_address
        elif ray_params.redis_address:
            node_ip_address = ray.services.get_node_ip_address(
                ray_params.redis_address)
        else:
            node_ip_address = ray.services.get_node_ip_address()
        self._node_ip_address = node_ip_address

        ray_params.update_if_absent(
            include_log_monitor=True,
            redirect_worker_output=True,
            resources={},
            include_webui=False,
            temp_dir="/tmp/ray",
            # If the object manager port is None, then use 0 to cause
            # the object manager to choose its own port.
            object_manager_port=0,
            # If the node manager port is None, then use 0 to cause the
            # node manager to choose its own port.
            node_manager_port=0,
            java_worker_options=DEFAULT_JAVA_WORKER_OPTIONS,
            worker_path=os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "workers/default_worker.py"))

        self._ray_params = ray_params
        self._redis_address = ray_params.redis_address
        self._redis_shards = []
        self._config = (json.loads(ray_params._internal_config)
                        if ray_params._internal_config else None)

        if head:
            redis_client = None
            # date including microsecond
            date_str = datetime.datetime.today().strftime(
                "%Y-%m-%d_%H-%M-%S_%f")
            self.session_name = "session_{date_str}_{pid}".format(
                pid=os.getpid(), date_str=date_str)
        else:
            redis_client = self.create_redis_client()
            self.session_name = ray.utils.decode(
                redis_client.get("session_name"))

        self._init_temp(redis_client)

        if connect_only:
            # Get socket names from the configuration.
            self._plasma_store_socket_name = (
                ray_params.plasma_store_socket_name)
            self._raylet_socket_name = ray_params.raylet_socket_name

            # If user does not provide the socket name, get it from Redis.
            if (self._plasma_store_socket_name is None
                    or self._raylet_socket_name is None):
                # Get the address info of the processes to connect to
                # from Redis.
                address_info = ray.services.get_address_info_from_redis(
                    self.redis_address,
                    self._node_ip_address,
                    redis_password=self.redis_password)
                self._plasma_store_socket_name = address_info[
                    "object_store_address"]
                self._raylet_socket_name = address_info["raylet_socket_name"]
        else:
            # If the user specified a socket name, use it.
            self._plasma_store_socket_name = self._prepare_socket_file(
                self._ray_params.plasma_store_socket_name,
                default_prefix="plasma_store")
            self._raylet_socket_name = self._prepare_socket_file(
                self._ray_params.raylet_socket_name, default_prefix="raylet")

        if head:
            ray_params.update_if_absent(num_redis_shards=1, include_webui=True)
            self._webui_url = None
        else:
            self._webui_url = (
                ray.services.get_webui_url_from_redis(redis_client))
            ray_params.include_java = (
                ray.services.include_java_from_redis(redis_client))

        # Start processes.
        if head:
            self.start_head_processes()
            redis_client = self.create_redis_client()
            redis_client.set("session_name", self.session_name)
            redis_client.set("session_dir", self._session_dir)
            redis_client.set("temp_dir", self._temp_dir)

        if not connect_only:
            self.start_ray_processes()

    def _init_temp(self, redis_client):
        # Create an dictionary to store temp file index.
        self._incremental_dict = collections.defaultdict(lambda: 0)

        if self.head:
            self._temp_dir = self._ray_params.temp_dir
        else:
            self._temp_dir = ray.utils.decode(redis_client.get("temp_dir"))

        try_to_create_directory(self._temp_dir, warn_if_exist=False)

        if self.head:
            self._session_dir = os.path.join(self._temp_dir, self.session_name)
        else:
            self._session_dir = ray.utils.decode(
                redis_client.get("session_dir"))

        # Send a warning message if the session exists.
        try_to_create_directory(self._session_dir)
        # Create a directory to be used for socket files.
        self._sockets_dir = os.path.join(self._session_dir, "sockets")
        try_to_create_directory(self._sockets_dir, warn_if_exist=False)
        # Create a directory to be used for process log files.
        self._logs_dir = os.path.join(self._session_dir, "logs")
        try_to_create_directory(self._logs_dir, warn_if_exist=False)

    @property
    def node_ip_address(self):
        """Get the cluster Redis address."""
        return self._node_ip_address

    @property
    def redis_address(self):
        """Get the cluster Redis address."""
        return self._redis_address

    @property
    def redis_password(self):
        """Get the cluster Redis password"""
        return self._ray_params.redis_password

    @property
    def load_code_from_local(self):
        return self._ray_params.load_code_from_local

    @property
    def object_id_seed(self):
        """Get the seed for deterministic generation of object IDs"""
        return self._ray_params.object_id_seed

    @property
    def plasma_store_socket_name(self):
        """Get the node's plasma store socket name."""
        return self._plasma_store_socket_name

    @property
    def webui_url(self):
        """Get the cluster's web UI url."""
        return self._webui_url

    @property
    def raylet_socket_name(self):
        """Get the node's raylet socket name."""
        return self._raylet_socket_name

    @property
    def address_info(self):
        """Get a dictionary of addresses."""
        return {
            "node_ip_address": self._node_ip_address,
            "redis_address": self._redis_address,
            "object_store_address": self._plasma_store_socket_name,
            "raylet_socket_name": self._raylet_socket_name,
            "webui_url": self._webui_url,
            "session_dir": self._session_dir,
        }

    def create_redis_client(self):
        """Create a redis client."""
        return ray.services.create_redis_client(
            self._redis_address, self._ray_params.redis_password)

    def get_temp_dir_path(self):
        """Get the path of the temporary directory."""
        return self._temp_dir

    def get_session_dir_path(self):
        """Get the path of the session directory."""
        return self._session_dir

    def get_logs_dir_path(self):
        """Get the path of the log files directory."""
        return self._logs_dir

    def get_sockets_dir_path(self):
        """Get the path of the sockets directory."""
        return self._sockets_dir

    def _make_inc_temp(self, suffix="", prefix="", directory_name="/tmp/ray"):
        """Return a incremental temporary file name. The file is not created.

        Args:
            suffix (str): The suffix of the temp file.
            prefix (str): The prefix of the temp file.
            directory_name (str) : The base directory of the temp file.

        Returns:
            A string of file name. If there existing a file having
                the same name, the returned name will look like
                "{directory_name}/{prefix}.{unique_index}{suffix}"
        """
        directory_name = os.path.expanduser(directory_name)
        index = self._incremental_dict[suffix, prefix, directory_name]
        # `tempfile.TMP_MAX` could be extremely large,
        # so using `range` in Python2.x should be avoided.
        while index < tempfile.TMP_MAX:
            if index == 0:
                filename = os.path.join(directory_name, prefix + suffix)
            else:
                filename = os.path.join(directory_name,
                                        prefix + "." + str(index) + suffix)
            index += 1
            if not os.path.exists(filename):
                # Save the index.
                self._incremental_dict[suffix, prefix, directory_name] = index
                return filename

        raise FileExistsError(errno.EEXIST,
                              "No usable temporary filename found")

    def new_log_files(self, name, redirect_output=True):
        """Generate partially randomized filenames for log files.

        Args:
            name (str): descriptive string for this log file.
            redirect_output (bool): True if files should be generated for
                logging stdout and stderr and false if stdout and stderr
                should not be redirected.
                If it is None, it will use the "redirect_output" Ray parameter.

        Returns:
            If redirect_output is true, this will return a tuple of two
                file handles. The first is for redirecting stdout and the
                second is for redirecting stderr.
                If redirect_output is false, this will return a tuple
                of two None objects.
        """
        if redirect_output is None:
            redirect_output = self._ray_params.redirect_output
        if not redirect_output:
            return None, None

        log_stdout = self._make_inc_temp(
            suffix=".out", prefix=name, directory_name=self._logs_dir)
        log_stderr = self._make_inc_temp(
            suffix=".err", prefix=name, directory_name=self._logs_dir)
        # Line-buffer the output (mode 1).
        log_stdout_file = open(log_stdout, "a", buffering=1)
        log_stderr_file = open(log_stderr, "a", buffering=1)
        return log_stdout_file, log_stderr_file

    def _prepare_socket_file(self, socket_path, default_prefix):
        """Prepare the socket file for raylet and plasma.

        This method helps to prepare a socket file.
        1. Make the directory if the directory does not exist.
        2. If the socket file exists, raise exception.

        Args:
            socket_path (string): the socket file to prepare.
        """
        if socket_path is not None:
            if os.path.exists(socket_path):
                raise Exception("Socket file {} exists!".format(socket_path))
            socket_dir = os.path.dirname(socket_path)
            try_to_create_directory(socket_dir)
            return socket_path
        return self._make_inc_temp(
            prefix=default_prefix, directory_name=self._sockets_dir)

    def start_redis(self):
        """Start the Redis global state store."""
        num_redis_shards = self._ray_params.num_redis_shards
        redis_shard_ports = self._ray_params.redis_shard_ports
        if redis_shard_ports is None:
            redis_shard_ports = num_redis_shards * [None]
        elif len(redis_shard_ports) != num_redis_shards:
            raise Exception(
                "The number of Redis shard ports does not match the "
                "number of Redis shards.")

        use_credis = ("RAY_USE_NEW_GCS" in os.environ)
        if use_credis:
            if self.redis_password is not None:
                # TODO(pschafhalter) remove this once credis supports
                # authenticating Redis ports
                raise Exception("Setting the `redis_password` argument is not "
                                "supported in credis. To run Ray with "
                                "password-protected Redis ports, ensure that "
                                "the environment variable"
                                "`RAY_USE_NEW_GCS=off`.")
            assert num_redis_shards == 1, (
                "For now, RAY_USE_NEW_GCS supports 1 shard, and credis "
                "supports 1-node chain for that shard only.")

        if use_credis:
            redis_executable = CREDIS_EXECUTABLE
            # TODO(suquark): We need credis here because some symbols need
            # to be imported from credis dynamically through dlopen when Ray
            # is built with RAY_USE_NEW_GCS=on. We should remove them later
            # for the primary shard.
            # See src/ray/gcs/redis_module/ray_redis_module.cc
            redis_modules = [CREDIS_MASTER_MODULE, REDIS_MODULE]
        else:
            redis_executable = REDIS_EXECUTABLE
            redis_modules = [REDIS_MODULE]

        redis_stdout_file, redis_stderr_file = self.new_log_files("redis")
        # Start the primary Redis shard.
        port = self._start_redis_instance(
            redis_executable,
            modules=redis_modules,
            port=self._ray_params.redis_port,
            password=self.redis_password,
            redis_max_clients=self._ray_params.redis_max_clients,
            # Below we use None to indicate no limit on the memory of the
            # primary Redis shard.
            redis_max_memory=None,
            stdout_file=redis_stdout_file,
            stderr_file=redis_stderr_file)
        self._redis_address = ray.services.address(self._node_ip_address, port)

        # Register the number of Redis shards in the primary shard, so that clients
        # know how many redis shards to expect under RedisShards.
        primary_redis_client = self.create_redis_client()
        primary_redis_client.set("NumRedisShards", str(num_redis_shards))

        # Put the redirect_worker_output bool in the Redis shard so that workers
        # can access it and know whether or not to redirect their output.
        primary_redis_client.set("RedirectOutput", 1
        if self._ray_params.redirect_worker_output else 0)

        # put the include_java bool to primary redis-server, so that other nodes
        # can access it and know whether or not to enable cross-languages.
        primary_redis_client.set("INCLUDE_JAVA", 1
        if self._ray_params.include_java else 0)

        # Store version information in the primary Redis shard.
        ray.services._put_version_info_in_redis(primary_redis_client)

        # Calculate the redis memory.
        redis_max_memory = self._ray_params.redis_max_memory
        system_memory = ray.utils.get_system_memory()
        if redis_max_memory is None:
            redis_max_memory = min(
                ray_constants.DEFAULT_REDIS_MAX_MEMORY_BYTES,
                max(
                    int(system_memory * 0.2),
                    ray_constants.REDIS_MINIMUM_MEMORY_BYTES))
        if redis_max_memory < ray_constants.REDIS_MINIMUM_MEMORY_BYTES:
            raise ValueError(
                "Attempting to cap Redis memory usage at {} bytes, "
                "but the minimum allowed is {} bytes.".format(
                    redis_max_memory,
                    ray_constants.REDIS_MINIMUM_MEMORY_BYTES))

        # Start other Redis shards. Each Redis shard logs to a separate file,
        # prefixed by "redis-<shard number>".
        redis_shards = []
        for i in range(num_redis_shards):
            redis_stdout_file, redis_stderr_file = self.new_log_files(
                "redis-shard_" + str(i))
            if use_credis:
                redis_executable = CREDIS_EXECUTABLE
                # It is important to load the credis module
                # BEFORE the ray module, as the latter contains an extern
                # declaration that the former supplies.
                redis_modules = [CREDIS_MEMBER_MODULE, REDIS_MODULE]
            else:
                redis_executable = REDIS_EXECUTABLE
                redis_modules = [REDIS_MODULE]

            redis_shard_port = self._start_redis_instance(
                redis_executable,
                modules=redis_modules,
                port=redis_shard_ports[i],
                password=self.redis_password,
                redis_max_clients=self._ray_params.redis_max_clients,
                redis_max_memory=redis_max_memory,
                stdout_file=redis_stdout_file,
                stderr_file=redis_stderr_file)

            shard_address = ray.services.address(
                self._node_ip_address, redis_shard_port)
            redis_shards.append(shard_address)
            # Store redis shard information in the primary redis shard.
            primary_redis_client.rpush("RedisShards", shard_address)

        self._redis_shards = redis_shards

        if use_credis:
            # Configure the chain state. The way it is intended to work is
            # the following:
            #
            # PRIMARY_SHARD
            #
            # SHARD_1 (master replica) -> SHARD_1 (member replica)
            #                                        -> SHARD_1 (member replica)
            #
            # SHARD_2 (master replica) -> SHARD_2 (member replica)
            #                                        -> SHARD_2 (member replica)
            # ...
            #
            #
            # If we have credis members in future, their modules should be:
            # [CREDIS_MEMBER_MODULE, REDIS_MODULE],
            # and they will be initialized by
            # execute_command("MEMBER.CONNECT_TO_MASTER", node_ip_address, port)
            #
            # Currently we have num_redis_shards == 1, so only one chain will be
            # created, and the chain only contains master.

            # TODO(suquark): Currently, this is not correct because we are
            # using the master replica as the primary shard. This should be
            # fixed later. I had tried to fix it but failed because of heartbeat
            # issues.
            shard_client = redis.StrictRedis(
                host=self._node_ip_address,
                port=redis_shard_port,
                password=self.redis_password)
            primary_redis_client.execute_command("MASTER.ADD",
                                                 self._node_ip_address,
                                                 redis_shard_port)
            shard_client.execute_command("MEMBER.CONNECT_TO_MASTER",
                                         self._node_ip_address, port)

    def _start_redis_instance(self,
                              executable,
                              modules,
                              port=None,
                              redis_max_clients=None,
                              num_retries=20,
                              stdout_file=None,
                              stderr_file=None,
                              password=None,
                              redis_max_memory=None):
        """Start a single Redis server.

        Notes:
            If "port" is not None, then we will only use this port and try
            only once. Otherwise, random ports will be used and the maximum
            retries count is "num_retries".

        Args:
            executable (str): Full path of the redis-server executable.
            modules (list of str): A list of pathnames, pointing to the redis
                module(s) that will be loaded in this redis server.
            port (int): If provided, start a Redis server with this port.
            redis_max_clients: If this is provided, Ray will attempt to configure
                Redis with this maxclients number.
            num_retries (int): The number of times to attempt to start Redis. If a
                port is provided, this defaults to 1.
            stdout_file: A file handle opened for writing to redirect stdout to. If
                no redirection should happen, then this should be None.
            stderr_file: A file handle opened for writing to redirect stderr to. If
                no redirection should happen, then this should be None.
            password (str): Prevents external clients without the password
                from connecting to Redis if provided.
            redis_max_memory: The max amount of memory (in bytes) to allow redis
                to use, or None for no limit. Once the limit is exceeded, redis
                will start LRU eviction of entries.

        Returns:
            A tuple of the port used by Redis and ProcessInfo for the process that
                was started. If a port is passed in, then the returned port value
                is the same.

        Raises:
            Exception: An exception is raised if Redis could not be started.
        """
        assert os.path.isfile(executable)
        for module in modules:
            assert os.path.isfile(module)
        counter = 0
        if port is not None:
            # If a port is specified, then try only once to connect.
            # This ensures that we will use the given port.
            num_retries = 1
        else:
            port = ray.services.new_port()

        load_module_args = []
        for module in modules:
            load_module_args += ["--loadmodule", module]

        while counter < num_retries:
            if counter > 0:
                logger.warning("Redis failed to start, retrying now.")

            # Construct the command to start the Redis server.
            command = [executable]
            if password:
                command += ["--requirepass", password]
            command += (
                    ["--port", str(port), "--loglevel",
                     "warning"] + load_module_args)
            process_info = self._process_control.start_ray_process(
                command,
                ray_constants.PROCESS_TYPE_REDIS_SERVER,
                stdout_file=stdout_file,
                stderr_file=stderr_file)
            time.sleep(0.1)
            # Check if Redis successfully started (or at least if it the executable
            # did not exit within 0.1 seconds).
            if process_info.process.poll() is None:
                break
            port = ray.services.new_port()
            counter += 1
        if counter == num_retries:
            raise Exception(
                "Couldn't start Redis. Check log files: {} {}".format(
                    stdout_file.name, stderr_file.name))

        ray.services.setup_redis_instance(port, password, redis_max_memory,
                                          redis_max_clients)

        return port

    def start_log_monitor(self):
        """Start the log monitor."""
        stdout_file, stderr_file = self.new_log_files("log_monitor")
        log_monitor_filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "log_monitor.py")
        command = [
            sys.executable, "-u", log_monitor_filepath,
            "--redis-address={}".format(self._redis_address),
            "--logs-dir={}".format(self._logs_dir)
        ]
        if self.redis_password:
            command += ["--redis-password", self.redis_password]
        self._process_control.start_ray_process(
            command,
            ray_constants.PROCESS_TYPE_LOG_MONITOR,
            stdout_file=stdout_file,
            stderr_file=stderr_file)

    def start_reporter(self):
        """Start the reporter."""
        stdout_file, stderr_file = self.new_log_files("reporter", True)
        reporter_filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "reporter.py")
        command = [
            sys.executable, "-u", reporter_filepath,
            "--redis-address={}".format(self._redis_address)
        ]
        redis_password = self._ray_params.redis_password or ""
        if redis_password:
            command += ["--redis-password", redis_password]

        try:
            import psutil  # noqa: F401
        except ImportError:
            logger.warning(
                "Failed to start the reporter. The reporter requires "
                "'pip install psutil'.")
            return None

        self._process_control.start_ray_process(
            command,
            ray_constants.PROCESS_TYPE_REPORTER,
            stdout_file=stdout_file,
            stderr_file=stderr_file)

    def start_dashboard(self):
        """Start the dashboard."""
        if not PY3:
            return None
        stdout_file, stderr_file = self.new_log_files("dashboard", True)
        port = ray.utils.get_next_available_socket(8080)
        token = ray.utils.decode(binascii.hexlify(os.urandom(24)))

        dashboard_filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "dashboard/dashboard.py")
        command = [
            sys.executable,
            "-u",
            dashboard_filepath,
            "--redis-address={}".format(self._redis_address),
            "--http-port={}".format(port),
            "--token={}".format(token),
            "--temp-dir={}".format(self._temp_dir),
        ]
        if self.redis_password:
            command += ["--redis-password", self.redis_password]

        try:
            import aiohttp  # noqa: F401
            import psutil  # noqa: F401
        except ImportError:
            raise ImportError(
                "Failed to start the dashboard. The dashboard requires "
                "Python 3 as well as 'pip install aiohttp psutil'.")

        self._process_control.start_ray_process(
            command,
            ray_constants.PROCESS_TYPE_DASHBOARD,
            stdout_file=stdout_file,
            stderr_file=stderr_file)
        dashboard_url = "http://{}:{}/?token={}".format(
            self._node_ip_address, port, token)
        print("\n" + "=" * 70)
        print("View the dashboard at {}".format(dashboard_url))
        print("=" * 70 + "\n")
        self._webui_url = dashboard_url
        redis_client = self.create_redis_client()
        redis_client.hmset("webui", {"url": self._webui_url})

    def start_plasma_store(self):
        """Start the plasma store."""
        stdout_file, stderr_file = self.new_log_files("plasma_store")
        object_store_memory = self._ray_params.object_store_memory
        plasma_directory = self._ray_params.plasma_directory
        huge_pages = self._ray_params.huge_pages
        plasma_store_socket_name = self._plasma_store_socket_name

        object_store_memory, plasma_directory = (
            ray.services.determine_plasma_store_config(
                object_store_memory, plasma_directory, huge_pages))

        # Print the object store memory using two decimal places.
        object_store_memory_str = (object_store_memory / 10**7) / 10**2
        logger.info("Starting the Plasma object store with {} GB memory "
                    "using {}.".format(
            round(object_store_memory_str, 2), plasma_directory))

        if RUN_PLASMA_STORE_VALGRIND and RUN_PLASMA_STORE_PROFILER:
            raise Exception(
                "Cannot use valgrind and profiler at the same time.")

        if huge_pages and not (sys.platform == "linux"
                               or sys.platform == "linux2"):
            raise Exception("The huge_pages argument is only supported on "
                            "Linux.")

        if huge_pages and plasma_directory is None:
            raise Exception("If huge_pages is True, then the "
                            "plasma_directory argument must be provided.")

        if not isinstance(object_store_memory, int):
            raise Exception("plasma_store_memory should be an integer.")

        command = [
            PLASMA_STORE_EXECUTABLE, "-s", plasma_store_socket_name, "-m",
            str(object_store_memory)
        ]
        if plasma_directory is not None:
            command += ["-d", plasma_directory]
        if huge_pages:
            command += ["-h"]
        # Start the Plasma store.
        self._process_control.start_ray_process(
            command,
            ray_constants.PROCESS_TYPE_PLASMA_STORE,
            use_valgrind=RUN_PLASMA_STORE_VALGRIND,
            use_valgrind_profiler=RUN_PLASMA_STORE_PROFILER,
            stdout_file=stdout_file,
            stderr_file=stderr_file)

    def start_raylet(self, use_valgrind=False, use_profiler=False):
        """Start the raylet.

        Args:
            use_valgrind (bool): True if we should start the process in
                valgrind.
            use_profiler (bool): True if we should start the process in the
                valgrind profiler.
        """
        stdout_file, stderr_file = self.new_log_files("raylet")
        if use_valgrind and use_profiler:
            raise Exception(
                "Cannot use valgrind and profiler at the same time.")

        num_cpus = self._ray_params.num_cpus
        num_gpus = self._ray_params.num_gpus
        cpu_count = multiprocessing.cpu_count()

        num_initial_workers = (num_cpus if num_cpus is not None else cpu_count)

        static_resources = ray.services.check_and_update_resources(
            num_cpus, num_gpus, self._ray_params.resources)

        # Limit the number of workers that can be started in parallel by the
        # raylet. However, make sure it is at least 1.
        num_cpus_static = static_resources.get("CPU", 0)
        maximum_startup_concurrency = max(1, min(cpu_count, num_cpus_static))

        # Format the resource argument in a form like 'CPU,1.0,GPU,0,Custom,3'.
        resource_argument = ",".join(
            ["{},{}".format(*kv) for kv in static_resources.items()])

        python_worker_command = self._start_python_worker_command()
        if self._ray_params.include_java:
            java_worker_command = self._start_java_worker_command()
        else:
            java_worker_command = ""

        gcs_ip_address, gcs_port = self._redis_address.split(":")

        command = [
            RAYLET_EXECUTABLE,
            "--raylet_socket_name={}".format(self._raylet_socket_name),
            "--store_socket_name={}".format(self._plasma_store_socket_name),
            "--object_manager_port={}".format(
                self._ray_params.object_manager_port),
            "--node_manager_port={}".format(
                self._ray_params.node_manager_port),
            "--node_ip_address={}".format(self._node_ip_address),
            "--redis_address={}".format(gcs_ip_address),
            "--redis_port={}".format(gcs_port),
            "--num_initial_workers={}".format(num_initial_workers),
            "--maximum_startup_concurrency={}".format(
                maximum_startup_concurrency),
            "--static_resource_list={}".format(resource_argument),
            "--config_list={}".format(self._get_config_list()),
            "--python_worker_command={}".format(python_worker_command),
            "--java_worker_command={}".format(java_worker_command),
            "--redis_password={}".format(self.redis_password or ""),
            "--temp_dir={}".format(self._temp_dir),
        ]
        self._process_control.start_ray_process(
            command,
            ray_constants.PROCESS_TYPE_RAYLET,
            use_valgrind=use_valgrind,
            use_gdb=False,
            use_valgrind_profiler=use_profiler,
            use_perftools_profiler=("RAYLET_PERFTOOLS_PATH" in os.environ),
            stdout_file=stdout_file,
            stderr_file=stderr_file)

    def new_worker_redirected_log_file(self, worker_id):
        """Create new logging files for workers to redirect its output."""
        worker_stdout_file, worker_stderr_file = (self.new_log_files(
            "worker-" + ray.utils.binary_to_hex(worker_id), True))
        return worker_stdout_file, worker_stderr_file

    def _start_python_worker_command(self):
        """Get the command to start the worker."""
        command = [
            sys.executable, "-u", self._ray_params.worker_path,
            "--node-ip-address=" + self._node_ip_address,
            "--object-store-name=" + self._plasma_store_socket_name,
            "--raylet-name=" + self._raylet_socket_name,
            "--redis-address=" + self._redis_address,
            "--temp-dir=" + self._temp_dir
        ]
        if self.redis_password:
            command += ["--redis_password={}".format(self.redis_password)]
        if self.load_code_from_local:
            command += ["--load-code-from-local"]
        return command

    def start_python_worker(self):
        """Start a Python worker process."""
        command = self._start_python_worker_command()
        self._process_control.start_ray_process(
            command, ray_constants.PROCESS_TYPE_WORKER)

    def _start_java_worker_command(self):
        """"""
        assert self._ray_params.java_worker_options is not None
        java_worker_options = self._ray_params.java_worker_options
        command = [
            "java", "-Dray.home={}".format(RAY_HOME),
            "-Dray.log-dir={}".format(self._logs_dir),

        ]
        if self._redis_address is not None:
            command.append(
                "-Dray.redis.address={}".format(self._redis_address))

        if self._plasma_store_socket_name is not None:
            command.append("-Dray.object-store.socket-name={}".format(
                self._plasma_store_socket_name))

        if self._raylet_socket_name is not None:
            command.append("-Dray.raylet.socket-name={}".format(
                self._raylet_socket_name))

        if self.redis_password is not None:
            command.append("-Dray.redis.password={}".format(
                self.redis_password))

        if java_worker_options:
            # Put `java_worker_options` in the last, so it can overwrite the
            # above options.
            command.append(java_worker_options)
        command.append("org.ray.runtime.runner.worker.DefaultWorker")
        return command

    def start_java_worker(self):
        """Start a Java worker process."""
        command = self._start_java_worker_command()
        self._process_control.start_ray_process(
            command, ray_constants.PROCESS_TYPE_WORKER)

    def start_monitor(self):
        """Start the monitor."""
        stdout_file, stderr_file = self.new_log_files("monitor")
        monitor_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "monitor.py")
        command = [
            sys.executable, "-u", monitor_path,
            "--redis-address=" + str(self._redis_address)
        ]
        if self._ray_params.autoscaling_config:
            command.append("--autoscaling-config=" + str(
                self._ray_params.autoscaling_config))
        if self._ray_params.redis_password:
            command.append(
                "--redis-password=" + self._ray_params.redis_password)
        self._process_control.start_ray_process(
            command,
            ray_constants.PROCESS_TYPE_MONITOR,
            stdout_file=stdout_file,
            stderr_file=stderr_file)

    def _get_config_list(self):
        config = self._config or {}
        return ",".join(["{},{}".format(*kv) for kv in config.items()])

    def start_raylet_monitor(self):
        """Start the raylet monitor to monitor the other processes."""
        stdout_file, stderr_file = self.new_log_files("raylet_monitor")
        gcs_ip_address, gcs_port = self._redis_address.split(":")
        command = [
            RAYLET_MONITOR_EXECUTABLE,
            "--redis_address={}".format(gcs_ip_address),
            "--redis_port={}".format(gcs_port),
            "--config_list={}".format(self._get_config_list()),
        ]
        if self.redis_password:
            command += ["--redis_password={}".format(self.redis_password)]
        self._process_control.start_ray_process(
            command,
            ray_constants.PROCESS_TYPE_RAYLET_MONITOR,
            stdout_file=stdout_file,
            stderr_file=stderr_file)

    def start_head_processes(self):
        """Start head processes on the node."""
        logger.info(
            "Process STDOUT and STDERR is being redirected to {}.".format(
                self._logs_dir))
        assert self._redis_address is None
        # If this is the head node, start the relevant head node processes.
        self.start_redis()
        self.start_monitor()
        self.start_raylet_monitor()
        # The dashboard is Python3.x only.
        if PY3 and self._ray_params.include_webui:
            self.start_dashboard()

    def start_ray_processes(self):
        """Start all of the processes on the node."""
        logger.info(
            "Process STDOUT and STDERR is being redirected to {}.".format(
                self._logs_dir))

        self.start_plasma_store()
        self.start_raylet(use_valgrind=RUN_RAYLET_PROFILER)
        if PY3:
            self.start_reporter()

        if self._ray_params.include_log_monitor:
            self.start_log_monitor()

    def kill_redis(self, check_alive=True):
        """Kill the Redis servers.

        Args:
            check_alive (bool): Raise an exception if any of the processes
                were already dead.
        """
        self._process_control.kill_process_type(
            ray_constants.PROCESS_TYPE_REDIS_SERVER, check_alive=check_alive)

    def kill_plasma_store(self, check_alive=True):
        """Kill the plasma store.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._process_control.kill_process_type(
            ray_constants.PROCESS_TYPE_PLASMA_STORE, check_alive=check_alive)

    def kill_raylet(self, check_alive=True):
        """Kill the raylet.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._process_control.kill_process_type(
            ray_constants.PROCESS_TYPE_RAYLET, check_alive=check_alive)

    def kill_log_monitor(self, check_alive=True):
        """Kill the log monitor.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._process_control.kill_process_type(
            ray_constants.PROCESS_TYPE_LOG_MONITOR, check_alive=check_alive)

    def kill_reporter(self, check_alive=True):
        """Kill the reporter.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        # reporter is started only in PY3.
        if PY3:
            self._process_control.kill_process_type(
                ray_constants.PROCESS_TYPE_REPORTER, check_alive=check_alive)

    def kill_dashboard(self, check_alive=True):
        """Kill the dashboard.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._process_control.kill_process_type(
            ray_constants.PROCESS_TYPE_DASHBOARD, check_alive=check_alive)

    def kill_monitor(self, check_alive=True):
        """Kill the monitor.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._process_control.kill_process_type(
            ray_constants.PROCESS_TYPE_MONITOR, check_alive=check_alive)

    def kill_raylet_monitor(self, check_alive=True):
        """Kill the raylet monitor.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._process_control.kill_process_type(
            ray_constants.PROCESS_TYPE_RAYLET_MONITOR, check_alive=check_alive)

    def kill_all_processes(self, check_alive=True, allow_graceful=False):
        """Kill all of the processes.

        Note that This is slower than necessary because it calls kill, wait,
        kill, wait, ... instead of kill, kill, ..., wait, wait, ...

        Args:
            check_alive (bool): Raise an exception if any of the processes were
                already dead.
        """
        self._process_control.kill_all_processes(check_alive, allow_graceful)

    def live_processes(self):
        """Return a list of the live processes.

        Returns:
            A list of the live processes.
        """
        return self._process_control.live_processes()

    def dead_processes(self):
        """Return a list of the dead processes.

        Note that this ignores processes that have been explicitly killed,
        e.g., via a command like node.kill_raylet().

        Returns:
            A list of the dead processes ignoring the ones that have been
                explicitly killed.
        """
        return self._process_control.dead_processes()

    def any_processes_alive(self):
        """Return true if any processes are still alive.

        Returns:
            True if any process is still alive.
        """
        return self._process_control.any_processes_alive()

    def remaining_processes_alive(self):
        """Return true if all remaining processes are still alive.

        Note that this ignores processes that have been explicitly killed,
        e.g., via a command like node.kill_raylet().

        Returns:
            True if any process that wasn't explicitly killed is still alive.
        """
        return self._process_control.remaining_processes_alive()


class LocalNode(object):
    """Imitate the node that manages the processes in local mode."""

    def kill_all_processes(self, *args, **kwargs):
        """Kill all of the processes."""
        pass  # Keep this function empty because it will be used in worker.py

    @property
    def address_info(self):
        """Get a dictionary of addresses."""
        return {}  # Return a null dict.
