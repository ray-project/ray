from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import atexit
import collections
import json
import os
import logging
import signal
import threading
import time

import ray
from ray.tempfile_services import (
    get_logs_dir_path, get_object_store_socket_name, get_raylet_socket_name,
    new_log_monitor_log_file, new_monitor_log_file,
    new_raylet_monitor_log_file, new_plasma_store_log_file,
    new_raylet_log_file, new_webui_log_file, set_temp_root)

ProcessInfo = collections.namedtuple(
    "ProcessInfo", ["process", "use_valgrind", "use_profiler"])

PROCESS_TYPE_MONITOR = "monitor"
PROCESS_TYPE_RAYLET_MONITOR = "raylet_monitor"
PROCESS_TYPE_LOG_MONITOR = "log_monitor"
PROCESS_TYPE_WORKER = "worker"
PROCESS_TYPE_RAYLET = "raylet"
PROCESS_TYPE_PLASMA_STORE = "plasma_store"
PROCESS_TYPE_REDIS_SERVER = "redis_server"
PROCESS_TYPE_WEB_UI = "web_ui"

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray configures it by default automatically
# using logging.basicConfig in its entry/init points.
logger = logging.getLogger(__name__)


class Node(object):
    """An encapsulation of the Ray processes on a single node.

    This class is responsible for starting Ray processes and killing them.

    Attributes:
        all_processes: A mapping from process type (which is a string) to a
            list of ProcessInfo objects. All lists have length one except for
            the Redis server list, which has multiple.
    """

    def __init__(self, ray_params, head=False, shutdown_at_exit=True):
        """Start a node.

        Args:
            ray_params (ray.params.RayParams): The parameters to use to
                configure the node.
            head (bool): True if this is the head node, which means it will
                start additional processes like the Redis servers, monitor
                processes, and web UI.
            shutdown_at_exit (bool): True if a handler should be registered to
                shutdown the processes started here when the Python interpreter
                exits.
        """
        self.all_processes = {}

        ray_params.update_if_absent(
            node_ip_address=ray.services.get_node_ip_address(),
            include_log_monitor=True,
            resources={},
            include_webui=False,
            worker_path=os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "workers/default_worker.py"))

        if head:
            ray_params.update_if_absent(num_redis_shards=1, include_webui=True)

        self._ray_params = ray_params
        self._config = (json.loads(ray_params._internal_config)
                        if ray_params._internal_config else None)
        self._node_ip_address = ray_params.node_ip_address
        self._redis_address = ray_params.redis_address
        self._plasma_store_socket_name = None
        self._raylet_socket_name = None
        self._webui_url = None

        self.start_ray_processes()

        if shutdown_at_exit:
            atexit.register(lambda: self.kill_all_processes(
                check_alive=False, allow_graceful=True))

    @property
    def node_ip_address(self):
        """Get the cluster Redis address."""
        return self._node_ip_address

    @property
    def redis_address(self):
        """Get the cluster Redis address."""
        return self._redis_address

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

    def start_redis(self):
        """Start the Redis servers."""
        assert self._redis_address is None
        (self._redis_address, redis_shards,
         processes) = ray.services.start_redis(
             self._node_ip_address,
             port=self._ray_params.redis_port,
             redis_shard_ports=self._ray_params.redis_shard_ports,
             num_redis_shards=self._ray_params.num_redis_shards,
             redis_max_clients=self._ray_params.redis_max_clients,
             redirect_output=self._ray_params.redirect_output,
             redirect_worker_output=self._ray_params.redirect_worker_output,
             password=self._ray_params.redis_password,
             redis_max_memory=self._ray_params.redis_max_memory)
        assert PROCESS_TYPE_REDIS_SERVER not in self.all_processes
        self.all_processes[PROCESS_TYPE_REDIS_SERVER] = []
        for p in processes:
            process_info = ProcessInfo(
                process=p, use_valgrind=False, use_profiler=False)
            self.all_processes[PROCESS_TYPE_REDIS_SERVER].append(process_info)
        return self.redis_address, redis_shards

    def start_log_monitor(self):
        """Start the log monitor."""
        stdout_file, stderr_file = new_log_monitor_log_file()
        p = ray.services.start_log_monitor(
            self.redis_address,
            self._node_ip_address,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            redis_password=self._ray_params.redis_password)
        assert PROCESS_TYPE_LOG_MONITOR not in self.all_processes
        self.all_processes[PROCESS_TYPE_LOG_MONITOR] = [
            ProcessInfo(process=p, use_valgrind=False, use_profiler=False)
        ]

    def start_ui(self):
        """Start the web UI."""
        stdout_file, stderr_file = new_webui_log_file()
        self._webui_url, p = ray.services.start_ui(
            self._redis_address,
            stdout_file=stdout_file,
            stderr_file=stderr_file)
        assert PROCESS_TYPE_WEB_UI not in self.all_processes
        if p is not None:
            self.all_processes[PROCESS_TYPE_WEB_UI] = [
                ProcessInfo(process=p, use_valgrind=False, use_profiler=False)
            ]

    def start_plasma_store(self):
        """Start the plasma store."""
        assert self._plasma_store_socket_name is None
        # If the user specified a socket name, use it.
        self._plasma_store_socket_name = (
            self._ray_params.plasma_store_socket_name
            or get_object_store_socket_name())
        stdout_file, stderr_file = (new_plasma_store_log_file(
            self._ray_params.redirect_output))
        self._plasma_store_socket_name, p = ray.services.start_plasma_store(
            self._node_ip_address,
            self._redis_address,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            object_store_memory=self._ray_params.object_store_memory,
            plasma_directory=self._ray_params.plasma_directory,
            huge_pages=self._ray_params.huge_pages,
            plasma_store_socket_name=self._plasma_store_socket_name,
            redis_password=self._ray_params.redis_password)
        assert PROCESS_TYPE_PLASMA_STORE not in self.all_processes
        self.all_processes[PROCESS_TYPE_PLASMA_STORE] = [
            ProcessInfo(process=p, use_valgrind=False, use_profiler=False)
        ]

    def start_raylet(self, use_valgrind=False, use_profiler=False):
        """Start the raylet."""
        assert self._raylet_socket_name is None
        # If the user specified a socket name, use it.
        self._raylet_socket_name = (self._ray_params.raylet_socket_name
                                    or get_raylet_socket_name())
        stdout_file, stderr_file = new_raylet_log_file(
            redirect_output=self._ray_params.redirect_worker_output)
        p = ray.services.start_raylet(
            self._redis_address,
            self._node_ip_address,
            self._raylet_socket_name,
            self._plasma_store_socket_name,
            self._ray_params.worker_path,
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
            config=self._config)
        assert PROCESS_TYPE_RAYLET not in self.all_processes
        self.all_processes[PROCESS_TYPE_RAYLET] = [
            ProcessInfo(
                process=p,
                use_valgrind=use_valgrind,
                use_profiler=use_profiler)
        ]

    def start_worker(self):
        """Start a worker process."""
        raise NotImplementedError("This has not been implemented.")

    def start_monitor(self):
        """Start the monitor."""
        stdout_file, stderr_file = new_monitor_log_file(
            self._ray_params.redirect_output)
        p = ray.services.start_monitor(
            self._redis_address,
            self._node_ip_address,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            autoscaling_config=self._ray_params.autoscaling_config,
            redis_password=self._ray_params.redis_password)
        assert PROCESS_TYPE_MONITOR not in self.all_processes
        self.all_processes[PROCESS_TYPE_MONITOR] = [
            ProcessInfo(process=p, use_valgrind=False, use_profiler=False)
        ]

    def start_raylet_monitor(self):
        """Start the raylet monitor."""
        stdout_file, stderr_file = new_raylet_monitor_log_file(
            self._ray_params.redirect_output)
        p = ray.services.start_raylet_monitor(
            self._redis_address,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            redis_password=self._ray_params.redis_password,
            config=self._config)
        assert PROCESS_TYPE_RAYLET_MONITOR not in self.all_processes
        self.all_processes[PROCESS_TYPE_RAYLET_MONITOR] = [
            ProcessInfo(process=p, use_valgrind=False, use_profiler=False)
        ]

    def start_ray_processes(self):
        """Start all of the processes on the node."""
        set_temp_root(self._ray_params.temp_dir)
        logger.info(
            "Process STDOUT and STDERR is being redirected to {}.".format(
                get_logs_dir_path()))

        # If this is the head node, start the relevant head node processes.
        if self._redis_address is None:
            self.start_redis()
            self.start_monitor()
            self.start_raylet_monitor()

        self.start_plasma_store()
        self.start_raylet()

        if self._ray_params.include_log_monitor:
            self.start_log_monitor()
        if self._ray_params.include_webui:
            self.start_ui()

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
        if process_type != PROCESS_TYPE_REDIS_SERVER:
            assert len(process_infos) == 1
        for process_info in process_infos:
            p = process_info.process
            # Handle the case where the process has already exited.
            if p.poll() is not None:
                if check_alive:
                    raise Exception("Attempting to kill a process of type "
                                    "'{}', but this process is already dead."
                                    .format(process_type))
                else:
                    continue

            if process_info.use_valgrind:
                p.terminate()
                p.wait()
                if p.returncode != 0:
                    raise Exception("Valgrind detected some errors.")
                continue

            if process_info.use_profiler:
                # Give process signal to write profiler data.
                os.kill(p.pid, signal.SIGINT)
                # Wait for profiling data to be written.
                time.sleep(0.1)

            if allow_graceful:
                # Allow the process one second to exit gracefully.
                p.terminate()
                timer = threading.Timer(1, lambda p: p.kill(), [p])
                try:
                    timer.start()
                    p.wait()
                finally:
                    timer.cancel()

                if p.poll() is not None:
                    continue

            # If the process did not exit within one second, force kill it.
            p.kill()
            # The reason we usually don't call p.wait() here is that there's
            # some chance we'd end up waiting a really long time.
            if wait:
                p.wait()

        del self.all_processes[process_type]

    def kill_redis(self, check_alive=True):
        """Kill the Redis servers.

        Args:
            check_alive (bool): Raise an exception if any of the processes
                were already dead.
        """
        self._kill_process_type(
            PROCESS_TYPE_REDIS_SERVER, check_alive=check_alive)

    def kill_plasma_store(self, check_alive=True):
        """Kill the plasma store.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            PROCESS_TYPE_PLASMA_STORE, check_alive=check_alive)

    def kill_raylet(self, check_alive=True):
        """Kill the raylet.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(PROCESS_TYPE_RAYLET, check_alive=check_alive)

    def kill_log_monitor(self, check_alive=True):
        """Kill the log monitor.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            PROCESS_TYPE_LOG_MONITOR, check_alive=check_alive)

    def kill_monitor(self, check_alive=True):
        """Kill the monitor.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(PROCESS_TYPE_MONITOR, check_alive=check_alive)

    def kill_raylet_monitor(self, check_alive=True):
        """Kill the raylet monitor.

        Args:
            check_alive (bool): Raise an exception if the process was already
                dead.
        """
        self._kill_process_type(
            PROCESS_TYPE_RAYLET_MONITOR, check_alive=check_alive)

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
        if PROCESS_TYPE_RAYLET in self.all_processes:
            self._kill_process_type(
                PROCESS_TYPE_RAYLET,
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
