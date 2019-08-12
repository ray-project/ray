from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import atexit
import collections
import datetime
import errno
import json
import os
import logging
import signal
import sys
import tempfile
import threading
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


class Node(object):
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

        ray_params.update_if_absent(
            include_log_monitor=True,
            resources={},
            include_webui=False,
            temp_dir="/tmp/ray",
            worker_path=os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "workers/default_worker.py"))

        self._ray_params = ray_params
        self._redis_address = ray_params.redis_address
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

        if shutdown_at_exit:
            atexit.register(lambda: self.kill_all_processes(
                check_alive=False, allow_graceful=True))

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
        """Start the Redis servers."""
        assert self._redis_address is None
        redis_log_files = [self.new_log_files("redis")]
        for i in range(self._ray_params.num_redis_shards):
            redis_log_files.append(self.new_log_files("redis-shard_" + str(i)))

        (self._redis_address, redis_shards,
         process_infos) = ray.services.start_redis(
             self._node_ip_address,
             redis_log_files,
             port=self._ray_params.redis_port,
             redis_shard_ports=self._ray_params.redis_shard_ports,
             num_redis_shards=self._ray_params.num_redis_shards,
             redis_max_clients=self._ray_params.redis_max_clients,
             redirect_worker_output=True,
             password=self._ray_params.redis_password,
             include_java=self._ray_params.include_java,
             redis_max_memory=self._ray_params.redis_max_memory)
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
            redis_password=self._ray_params.redis_password)
        assert ray_constants.PROCESS_TYPE_LOG_MONITOR not in self.all_processes
        self.all_processes[ray_constants.PROCESS_TYPE_LOG_MONITOR] = [
            process_info
        ]

    def start_reporter(self):
        """Start the reporter."""
        stdout_file, stderr_file = self.new_log_files("reporter", True)
        process_info = ray.services.start_reporter(
            self.redis_address,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            redis_password=self._ray_params.redis_password)
        assert ray_constants.PROCESS_TYPE_REPORTER not in self.all_processes
        if process_info is not None:
            self.all_processes[ray_constants.PROCESS_TYPE_REPORTER] = [
                process_info
            ]

    def start_dashboard(self):
        """Start the dashboard."""
        stdout_file, stderr_file = self.new_log_files("dashboard", True)
        self._webui_url, process_info = ray.services.start_dashboard(
            self.redis_address,
            self._temp_dir,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            redis_password=self._ray_params.redis_password)
        assert ray_constants.PROCESS_TYPE_DASHBOARD not in self.all_processes
        if process_info is not None:
            self.all_processes[ray_constants.PROCESS_TYPE_DASHBOARD] = [
                process_info
            ]
            redis_client = self.create_redis_client()
            redis_client.hmset("webui", {"url": self._webui_url})

    def start_plasma_store(self):
        """Start the plasma store."""
        stdout_file, stderr_file = self.new_log_files("plasma_store")
        process_info = ray.services.start_plasma_store(
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            object_store_memory=self._ray_params.object_store_memory,
            plasma_directory=self._ray_params.plasma_directory,
            huge_pages=self._ray_params.huge_pages,
            plasma_store_socket_name=self._plasma_store_socket_name)
        assert (
            ray_constants.PROCESS_TYPE_PLASMA_STORE not in self.all_processes)
        self.all_processes[ray_constants.PROCESS_TYPE_PLASMA_STORE] = [
            process_info
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
            self._raylet_socket_name,
            self._plasma_store_socket_name,
            self._ray_params.worker_path,
            self._temp_dir,
            self._session_dir,
            self._ray_params.num_cpus,
            self._ray_params.num_gpus,
            self._ray_params.resources,
            self._ray_params.object_manager_port,
            self._ray_params.node_manager_port,
            self._ray_params.redis_password,
            use_valgrind=use_valgrind,
            use_profiler=use_profiler,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            config=self._config,
            include_java=self._ray_params.include_java,
            java_worker_options=self._ray_params.java_worker_options,
            load_code_from_local=self._ray_params.load_code_from_local,
        )
        assert ray_constants.PROCESS_TYPE_RAYLET not in self.all_processes
        self.all_processes[ray_constants.PROCESS_TYPE_RAYLET] = [process_info]

    def new_worker_redirected_log_file(self, worker_id):
        """Create new logging files for workers to redirect its output."""
        worker_stdout_file, worker_stderr_file = (self.new_log_files(
            "worker-" + ray.utils.binary_to_hex(worker_id), True))
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
            redis_password=self._ray_params.redis_password)
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
            config=self._config)
        assert (ray_constants.PROCESS_TYPE_RAYLET_MONITOR not in
                self.all_processes)
        self.all_processes[ray_constants.PROCESS_TYPE_RAYLET_MONITOR] = [
            process_info
        ]

    def start_head_processes(self):
        """Start head processes on the node."""
        logger.debug(
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
        logger.debug(
            "Process STDOUT and STDERR is being redirected to {}.".format(
                self._logs_dir))

        self.start_plasma_store()
        self.start_raylet()
        if PY3:
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
                    raise Exception("Attempting to kill a process of type "
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
                    raise Exception(message)
                continue

            if process_info.use_valgrind_profiler:
                # Give process signal to write profiler data.
                os.kill(process.pid, signal.SIGINT)
                # Wait for profiling data to be written.
                time.sleep(0.1)

            if allow_graceful:
                # Allow the process one second to exit gracefully.
                process.terminate()
                timer = threading.Timer(1, lambda process: process.kill(),
                                        [process])
                try:
                    timer.start()
                    process.wait()
                finally:
                    timer.cancel()

                if process.poll() is not None:
                    continue

            # If the process did not exit within one second, force kill it.
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
        # reporter is started only in PY3.
        if PY3:
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

    def kill_raylet_monitor(self, check_alive=True):
        """Kill the raylet monitor.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_RAYLET_MONITOR, check_alive=check_alive)

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

        # We call "list" to copy the keys because we are modifying the
        # dictionary while iterating over it.
        for process_type in list(self.all_processes.keys()):
            self._kill_process_type(
                process_type,
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


class LocalNode(object):
    """Imitate the node that manages the processes in local mode."""

    def kill_all_processes(self, *args, **kwargs):
        """Kill all of the processes."""
        pass  # Keep this function empty because it will be used in worker.py

    @property
    def address_info(self):
        """Get a dictionary of addresses."""
        return {}  # Return a null dict.
