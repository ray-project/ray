import atexit
import collections
import datetime
import errno
import os
import logging
import random
import signal
import socket
import subprocess
import sys
import tempfile
import time

import ray
import ray.ray_constants as ray_constants
import ray.services
import ray.utils
from ray.resource_spec import ResourceSpec
from ray.utils import try_to_create_directory, try_to_symlink

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray configures it by default automatically
# using logging.basicConfig in its entry/init points.
logger = logging.getLogger(__name__)

SESSION_LATEST = "session_latest"
NUMBER_OF_PORT_RETRIES = 40


class Node:
    """An encapsulation of the Ray processes on a single node.

    This class is responsible for starting Ray processes and killing them,
    and it also controls the temp file policy.

    Attributes:
        all_processes (dict): A mapping from process type (str) to a list of
            ProcessInfo objects. All lists have length one except for the Redis
            server list, which has multiple.
    """

    def __init__(self,
                 ray_params,
                 head=False,
                 shutdown_at_exit=True,
                 spawn_reaper=True,
                 connect_only=False):
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
                raise ValueError("'shutdown_at_exit' and 'connect_only' "
                                 "cannot both be true.")
            self._register_shutdown_hooks()

        self.head = head
        self.kernel_fate_share = bool(
            spawn_reaper and ray.utils.detect_fate_sharing_support())
        self.all_processes = {}

        # Try to get node IP address with the parameters.
        if ray_params.node_ip_address:
            node_ip_address = ray_params.node_ip_address
        elif ray_params.redis_address:
            node_ip_address = ray.services.get_node_ip_address(
                ray_params.redis_address)
        else:
            node_ip_address = ray.services.get_node_ip_address()
        self._node_ip_address = node_ip_address

        if ray_params.raylet_ip_address:
            raylet_ip_address = ray_params.raylet_ip_address
        else:
            raylet_ip_address = node_ip_address

        if raylet_ip_address != node_ip_address and (not connect_only or head):
            raise ValueError(
                "The raylet IP address should only be different than the node "
                "IP address when connecting to an existing raylet; i.e., when "
                "head=False and connect_only=True.")

        self._raylet_ip_address = raylet_ip_address

        ray_params.update_if_absent(
            include_log_monitor=True,
            resources={},
            temp_dir=ray.utils.get_ray_temp_dir(),
            worker_path=os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "workers/default_worker.py"))

        self._resource_spec = None
        self._localhost = socket.gethostbyname("localhost")
        self._ray_params = ray_params
        self._redis_address = ray_params.redis_address
        self._config = ray_params._internal_config

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
                    or self._raylet_socket_name is None
                    or self._ray_params.node_manager_port is None):
                # Get the address info of the processes to connect to
                # from Redis.
                address_info = ray.services.get_address_info_from_redis(
                    self.redis_address,
                    self._raylet_ip_address,
                    redis_password=self.redis_password)
                self._plasma_store_socket_name = address_info[
                    "object_store_address"]
                self._raylet_socket_name = address_info["raylet_socket_name"]
                self._ray_params.node_manager_port = address_info[
                    "node_manager_port"]
        else:
            # If the user specified a socket name, use it.
            self._plasma_store_socket_name = self._prepare_socket_file(
                self._ray_params.plasma_store_socket_name,
                default_prefix="plasma_store")
            self._raylet_socket_name = self._prepare_socket_file(
                self._ray_params.raylet_socket_name, default_prefix="raylet")

        if head:
            ray_params.update_if_absent(num_redis_shards=1)
            self._webui_url = None
        else:
            self._webui_url = (
                ray.services.get_webui_url_from_redis(redis_client))
            ray_params.include_java = (
                ray.services.include_java_from_redis(redis_client))

        if head or not connect_only:
            # We need to start a local raylet.
            if (self._ray_params.node_manager_port is None
                    or self._ray_params.node_manager_port == 0):
                # No port specified. Pick a random port for the raylet to use.
                # NOTE: There is a possible but unlikely race condition where
                # the port is bound by another process between now and when the
                # raylet starts.
                self._ray_params.node_manager_port, self._socket = \
                    self._get_unused_port(close_on_exit=False)

        if not connect_only and spawn_reaper and not self.kernel_fate_share:
            self.start_reaper_process()

        # Start processes.
        if head:
            self.start_head_processes()
            redis_client = self.create_redis_client()
            redis_client.set("session_name", self.session_name)
            redis_client.set("session_dir", self._session_dir)
            redis_client.set("temp_dir", self._temp_dir)

        if not connect_only:
            self.start_ray_processes()

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

        ray.utils.set_sigterm_handler(sigterm_handler)

    def _init_temp(self, redis_client):
        # Create an dictionary to store temp file index.
        self._incremental_dict = collections.defaultdict(lambda: 0)

        if self.head:
            self._temp_dir = self._ray_params.temp_dir
        else:
            self._temp_dir = ray.utils.decode(redis_client.get("temp_dir"))

        try_to_create_directory(self._temp_dir)

        if self.head:
            self._session_dir = os.path.join(self._temp_dir, self.session_name)
        else:
            self._session_dir = ray.utils.decode(
                redis_client.get("session_dir"))
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

    def get_resource_spec(self):
        """Resolve and return the current resource spec for the node."""
        if not self._resource_spec:
            self._resource_spec = ResourceSpec(
                self._ray_params.num_cpus, self._ray_params.num_gpus,
                self._ray_params.memory, self._ray_params.object_store_memory,
                self._ray_params.resources,
                self._ray_params.redis_max_memory).resolve(is_head=self.head)
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
        """Get the cluster address."""
        return self._redis_address

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
    def unique_id(self):
        """Get a unique identifier for this node."""
        return "{}:{}".format(self.node_ip_address,
                              self._plasma_store_socket_name)

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
    def socket(self):
        """Get the socket reserving the node manager's port"""
        try:
            return self._socket
        except AttributeError:
            return None

    @property
    def address_info(self):
        """Get a dictionary of addresses."""
        return {
            "node_ip_address": self._node_ip_address,
            "raylet_ip_address": self._raylet_ip_address,
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

    def _make_inc_temp(self, suffix="", prefix="", directory_name=None):
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
        if directory_name is None:
            directory_name = ray.utils.get_ray_temp_dir()
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

    def new_log_files(self, name):
        """Generate partially randomized filenames for log files.

        Args:
            name (str): descriptive string for this log file.

        Returns:
            A tuple of two file handles for redirecting (stdout, stderr).
        """
        redirect_output = self._ray_params.redirect_output

        if redirect_output is None:
            # Make the default behavior match that of glog.
            redirect_output = os.getenv("GLOG_logtostderr") != "1"

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

    def _get_unused_port(self, close_on_exit=True):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("", 0))
        port = s.getsockname()[1]

        # Try to generate a port that is far above the 'next available' one.
        # This solves issue #8254 where GRPC fails because the port assigned
        # from this method has been used by a different process.
        for _ in range(NUMBER_OF_PORT_RETRIES):
            new_port = random.randint(port, 65535)
            new_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                new_s.bind(("", new_port))
            except OSError:
                new_s.close()
                continue
            s.close()
            if close_on_exit:
                new_s.close()
            return new_port, new_s
        logger.error("Unable to succeed in selecting a random port.")
        if close_on_exit:
            s.close()
        return port, s

    def _prepare_socket_file(self, socket_path, default_prefix):
        """Prepare the socket file for raylet and plasma.

        This method helps to prepare a socket file.
        1. Make the directory if the directory does not exist.
        2. If the socket file exists, raise exception.

        Args:
            socket_path (string): the socket file to prepare.
        """
        result = socket_path
        is_mac = sys.platform.startswith("darwin")
        if sys.platform == "win32":
            if socket_path is None:
                result = "tcp://{}:{}".format(self._localhost,
                                              self._get_unused_port()[0])
        else:
            if socket_path is None:
                result = self._make_inc_temp(
                    prefix=default_prefix, directory_name=self._sockets_dir)
            else:
                if os.path.exists(socket_path):
                    raise RuntimeError(
                        "Socket file {} exists!".format(socket_path))
                try_to_create_directory(os.path.dirname(socket_path))

            # Check socket path length to make sure it's short enough
            maxlen = (104 if is_mac else 108) - 1  # sockaddr_un->sun_path
            if len(result.split("://", 1)[-1].encode("utf-8")) > maxlen:
                raise OSError("AF_UNIX path length cannot exceed "
                              "{} bytes: {!r}".format(maxlen, result))
        return result

    def start_reaper_process(self):
        """
        Start the reaper process.

        This must be the first process spawned and should only be called when
        ray processes should be cleaned up if this process dies.
        """
        assert not self.kernel_fate_share, (
            "a reaper should not be used with kernel fate-sharing")
        process_info = ray.services.start_reaper(fate_share=False)
        assert ray_constants.PROCESS_TYPE_REAPER not in self.all_processes
        if process_info is not None:
            self.all_processes[ray_constants.PROCESS_TYPE_REAPER] = [
                process_info,
            ]

    def start_redis(self):
        """Start the Redis servers."""
        assert self._redis_address is None
        redis_log_files = [self.new_log_files("redis")]
        for i in range(self._ray_params.num_redis_shards):
            redis_log_files.append(self.new_log_files("redis-shard_" + str(i)))

        (self._redis_address, redis_shards,
         process_infos) = ray.services.start_redis(
             self._node_ip_address,
             redis_log_files,
             self.get_resource_spec(),
             port=self._ray_params.redis_port,
             redis_shard_ports=self._ray_params.redis_shard_ports,
             num_redis_shards=self._ray_params.num_redis_shards,
             redis_max_clients=self._ray_params.redis_max_clients,
             redirect_worker_output=True,
             password=self._ray_params.redis_password,
             include_java=self._ray_params.include_java,
             fate_share=self.kernel_fate_share)
        assert (
            ray_constants.PROCESS_TYPE_REDIS_SERVER not in self.all_processes)
        self.all_processes[ray_constants.PROCESS_TYPE_REDIS_SERVER] = (
            process_infos)

    def start_log_monitor(self):
        """Start the log monitor."""
        stdout_file, stderr_file = self.new_log_files("log_monitor")
        process_info = ray.services.start_log_monitor(
            self.redis_address,
            self._logs_dir,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            redis_password=self._ray_params.redis_password,
            fate_share=self.kernel_fate_share)
        assert ray_constants.PROCESS_TYPE_LOG_MONITOR not in self.all_processes
        self.all_processes[ray_constants.PROCESS_TYPE_LOG_MONITOR] = [
            process_info,
        ]

    def start_reporter(self):
        """Start the reporter."""
        stdout_file, stderr_file = self.new_log_files("reporter")
        process_info = ray.services.start_reporter(
            self.redis_address,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            redis_password=self._ray_params.redis_password,
            fate_share=self.kernel_fate_share)
        assert ray_constants.PROCESS_TYPE_REPORTER not in self.all_processes
        if process_info is not None:
            self.all_processes[ray_constants.PROCESS_TYPE_REPORTER] = [
                process_info,
            ]

    def start_dashboard(self, require_webui):
        """Start the dashboard.

        Args:
            require_webui (bool): If true, this will raise an exception if we
                fail to start the webui. Otherwise it will print a warning if
                we fail to start the webui.
        """
        stdout_file, stderr_file = self.new_log_files("dashboard")
        self._webui_url, process_info = ray.services.start_dashboard(
            require_webui,
            self._ray_params.webui_host,
            self.redis_address,
            self._temp_dir,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            redis_password=self._ray_params.redis_password,
            fate_share=self.kernel_fate_share)
        assert ray_constants.PROCESS_TYPE_DASHBOARD not in self.all_processes
        if process_info is not None:
            self.all_processes[ray_constants.PROCESS_TYPE_DASHBOARD] = [
                process_info,
            ]
            redis_client = self.create_redis_client()
            redis_client.hmset("webui", {"url": self._webui_url})

    def start_plasma_store(self):
        """Start the plasma store."""
        stdout_file, stderr_file = self.new_log_files("plasma_store")
        process_info = ray.services.start_plasma_store(
            self.get_resource_spec(),
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            plasma_directory=self._ray_params.plasma_directory,
            huge_pages=self._ray_params.huge_pages,
            plasma_store_socket_name=self._plasma_store_socket_name,
            fate_share=self.kernel_fate_share)
        assert (
            ray_constants.PROCESS_TYPE_PLASMA_STORE not in self.all_processes)
        self.all_processes[ray_constants.PROCESS_TYPE_PLASMA_STORE] = [
            process_info,
        ]

    def start_gcs_server(self):
        """Start the gcs server.
        """
        stdout_file, stderr_file = self.new_log_files("gcs_server")
        process_info = ray.services.start_gcs_server(
            self._redis_address,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            redis_password=self._ray_params.redis_password,
            config=self._config,
            fate_share=self.kernel_fate_share)
        assert (
            ray_constants.PROCESS_TYPE_GCS_SERVER not in self.all_processes)
        self.all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER] = [
            process_info,
        ]

    def start_raylet(self, use_valgrind=False, use_profiler=False):
        """Start the raylet.

        Args:
            use_valgrind (bool): True if we should start the process in
                valgrind.
            use_profiler (bool): True if we should start the process in the
                valgrind profiler.
        """
        stdout_file, stderr_file = self.new_log_files("raylet")
        process_info = ray.services.start_raylet(
            self._redis_address,
            self._node_ip_address,
            self._ray_params.node_manager_port,
            self._raylet_socket_name,
            self._plasma_store_socket_name,
            self._ray_params.worker_path,
            self._temp_dir,
            self._session_dir,
            self.get_resource_spec(),
            self._ray_params.min_worker_port,
            self._ray_params.max_worker_port,
            self._ray_params.object_manager_port,
            self._ray_params.redis_password,
            use_valgrind=use_valgrind,
            use_profiler=use_profiler,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            config=self._config,
            include_java=self._ray_params.include_java,
            java_worker_options=self._ray_params.java_worker_options,
            load_code_from_local=self._ray_params.load_code_from_local,
            fate_share=self.kernel_fate_share,
            socket_to_use=self.socket)
        assert ray_constants.PROCESS_TYPE_RAYLET not in self.all_processes
        self.all_processes[ray_constants.PROCESS_TYPE_RAYLET] = [process_info]

    def new_worker_redirected_log_file(self, worker_id):
        """Create new logging files for workers to redirect its output."""
        worker_stdout_file, worker_stderr_file = (
            self.new_log_files("worker-" + ray.utils.binary_to_hex(worker_id)))
        return worker_stdout_file, worker_stderr_file

    def start_worker(self):
        """Start a worker process."""
        raise NotImplementedError

    def start_monitor(self):
        """Start the monitor."""
        stdout_file, stderr_file = self.new_log_files("monitor")
        process_info = ray.services.start_monitor(
            self._redis_address,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            autoscaling_config=self._ray_params.autoscaling_config,
            redis_password=self._ray_params.redis_password,
            fate_share=self.kernel_fate_share)
        assert ray_constants.PROCESS_TYPE_MONITOR not in self.all_processes
        self.all_processes[ray_constants.PROCESS_TYPE_MONITOR] = [process_info]

    def start_raylet_monitor(self):
        """Start the raylet monitor."""
        stdout_file, stderr_file = self.new_log_files("raylet_monitor")
        process_info = ray.services.start_raylet_monitor(
            self._redis_address,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            redis_password=self._ray_params.redis_password,
            config=self._config,
            fate_share=self.kernel_fate_share)
        assert (ray_constants.PROCESS_TYPE_RAYLET_MONITOR not in
                self.all_processes)
        self.all_processes[ray_constants.PROCESS_TYPE_RAYLET_MONITOR] = [
            process_info,
        ]

    def start_head_processes(self):
        """Start head processes on the node."""
        logger.debug(
            "Process STDOUT and STDERR is being redirected to {}.".format(
                self._logs_dir))
        assert self._redis_address is None
        # If this is the head node, start the relevant head node processes.
        self.start_redis()

        if ray_constants.GCS_SERVICE_ENABLED:
            self.start_gcs_server()
        else:
            self.start_raylet_monitor()

        self.start_monitor()

        if self._ray_params.include_webui:
            self.start_dashboard(require_webui=True)
        elif self._ray_params.include_webui is None:
            self.start_dashboard(require_webui=False)

    def start_ray_processes(self):
        """Start all of the processes on the node."""
        logger.debug(
            "Process STDOUT and STDERR is being redirected to {}.".format(
                self._logs_dir))

        self.start_plasma_store()
        self.start_raylet()
        self.start_reporter()

        if self._ray_params.include_log_monitor:
            self.start_log_monitor()

    def _kill_process_type(self,
                           process_type,
                           allow_graceful=False,
                           check_alive=True,
                           wait=False):
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
                        "'{}', but this process is already dead."
                        .format(process_type))
                else:
                    continue

            if process_info.use_valgrind:
                process.terminate()
                process.wait()
                if process.returncode != 0:
                    message = ("Valgrind detected some errors in process of "
                               "type {}. Error code {}.".format(
                                   process_type, process.returncode))
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
            ray_constants.PROCESS_TYPE_REDIS_SERVER, check_alive=check_alive)

    def kill_plasma_store(self, check_alive=True):
        """Kill the plasma store.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_PLASMA_STORE, check_alive=check_alive)

    def kill_raylet(self, check_alive=True):
        """Kill the raylet.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_RAYLET, check_alive=check_alive)

    def kill_log_monitor(self, check_alive=True):
        """Kill the log monitor.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_LOG_MONITOR, check_alive=check_alive)

    def kill_reporter(self, check_alive=True):
        """Kill the reporter.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_REPORTER, check_alive=check_alive)

    def kill_dashboard(self, check_alive=True):
        """Kill the dashboard.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_DASHBOARD, check_alive=check_alive)

    def kill_monitor(self, check_alive=True):
        """Kill the monitor.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_MONITOR, check_alive=check_alive)

    def kill_gcs_server(self, check_alive=True):
        """Kill the gcs server.
        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_GCS_SERVER, check_alive=check_alive)

    def kill_raylet_monitor(self, check_alive=True):
        """Kill the raylet monitor.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_RAYLET_MONITOR, check_alive=check_alive)

    def kill_reaper(self, check_alive=True):
        """Kill the reaper process.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_REAPER, check_alive=check_alive)

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
                allow_graceful=allow_graceful)

        if ray_constants.PROCESS_TYPE_GCS_SERVER in self.all_processes:
            self._kill_process_type(
                ray_constants.PROCESS_TYPE_GCS_SERVER,
                check_alive=check_alive,
                allow_graceful=allow_graceful)

        # We call "list" to copy the keys because we are modifying the
        # dictionary while iterating over it.
        for process_type in list(self.all_processes.keys()):
            # Need to kill the reaper process last in case we die unexpectedly
            # while cleaning up.
            if process_type != ray_constants.PROCESS_TYPE_REAPER:
                self._kill_process_type(
                    process_type,
                    check_alive=check_alive,
                    allow_graceful=allow_graceful)

        if ray_constants.PROCESS_TYPE_REAPER in self.all_processes:
            self._kill_process_type(
                ray_constants.PROCESS_TYPE_REAPER,
                check_alive=check_alive,
                allow_graceful=allow_graceful)

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
