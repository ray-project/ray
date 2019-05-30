from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import atexit
import collections
import json
import logging
import multiprocessing
import os
import random
import resource
import signal
import socket
import subprocess
import sys
import threading
import time
import redis

import pyarrow
# Ray modules
import ray
import ray.ray_constants as ray_constants

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)

ProcessInfo = collections.namedtuple("ProcessInfo", [
    "process", "stdout_file", "stderr_file", "use_valgrind", "use_gdb",
    "use_valgrind_profiler", "use_perftools_profiler", "use_tmux"
])

ProcessProperty = collections.namedtuple("ProcessProperty", ["unique"])


def address(ip_address, port):
    return ip_address + ":" + str(port)


def new_port():
    return random.randint(10000, 65535)


def include_java_from_redis(redis_client):
    """This is used for query include_java bool from redis.

    Args:
        redis_client (StrictRedis): The redis client to GCS.

    Returns:
        True if this cluster backend enables Java worker.
    """
    return redis_client.get("INCLUDE_JAVA") == b"1"


def get_address_info_from_redis_helper(redis_address,
                                       node_ip_address,
                                       redis_password=None):
    redis_ip_address, redis_port = redis_address.split(":")
    # For this command to work, some other client (on the same machine as
    # Redis) must have run "CONFIG SET protected-mode no".
    redis_client = create_redis_client(redis_address, password=redis_password)

    client_table = ray.state._parse_client_table(redis_client)
    if len(client_table) == 0:
        raise Exception(
            "Redis has started but no raylets have registered yet.")

    relevant_client = None
    for client_info in client_table:
        client_node_ip_address = client_info["NodeManagerAddress"]
        if (client_node_ip_address == node_ip_address
                or (client_node_ip_address == "127.0.0.1"
                    and redis_ip_address == get_node_ip_address())):
            relevant_client = client_info
            break
    if relevant_client is None:
        raise Exception(
            "Redis has started but no raylets have registered yet.")

    return {
        "object_store_address": relevant_client["ObjectStoreSocketName"],
        "raylet_socket_name": relevant_client["RayletSocketName"],
    }


def get_address_info_from_redis(redis_address,
                                node_ip_address,
                                num_retries=5,
                                redis_password=None):
    counter = 0
    while True:
        try:
            return get_address_info_from_redis_helper(
                redis_address, node_ip_address, redis_password=redis_password)
        except Exception:
            if counter == num_retries:
                raise
            # Some of the information may not be in Redis yet, so wait a little
            # bit.
            logger.warning(
                "Some processes that the driver needs to connect to have "
                "not registered with Redis, so retrying. Have you run "
                "'ray start' on this node?")
            time.sleep(1)
        counter += 1


def get_webui_url_from_redis(redis_client):
    webui_url = redis_client.hmget("webui", "url")[0]
    return ray.utils.decode(webui_url) if webui_url is not None else None


def remaining_processes_alive():
    """See if the remaining processes are alive or not.

    Note that this ignores processes that have been explicitly killed,
    e.g., via a command like node.kill_raylet().

    Returns:
        True if the remaining processes started by ray.init() are alive and
            False otherwise.

    Raises:
        Exception: An exception is raised if the processes were not started by
            ray.init().
    """
    if ray.worker._global_node is None:
        raise Exception("This process is not in a position to determine "
                        "whether all processes are alive or not.")
    return ray.worker._global_node.remaining_processes_alive()


def address_to_ip(address):
    """Convert a hostname to a numerical IP addresses in an address.

    This should be a no-op if address already contains an actual numerical IP
    address.

    Args:
        address: This can be either a string containing a hostname (or an IP
            address) and a port or it can be just an IP address.

    Returns:
        The same address but with the hostname replaced by a numerical IP
            address.
    """
    address_parts = address.split(":")
    ip_address = socket.gethostbyname(address_parts[0])
    # Make sure localhost isn't resolved to the loopback ip
    if ip_address == "127.0.0.1":
        ip_address = get_node_ip_address()
    return ":".join([ip_address] + address_parts[1:])


def get_node_ip_address(address="8.8.8.8:53"):
    """Determine the IP address of the local node.

    Args:
        address (str): The IP address and port of any known live service on the
            network you care about.

    Returns:
        The IP address of the current node.
    """
    ip_address, port = address.split(":")
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # This command will raise an exception if there is no internet
        # connection.
        s.connect((ip_address, int(port)))
        node_ip_address = s.getsockname()[0]
    except Exception as e:
        node_ip_address = "127.0.0.1"
        # [Errno 101] Network is unreachable
        if e.errno == 101:
            try:
                # try get node ip address from host name
                host_name = socket.getfqdn(socket.gethostname())
                node_ip_address = socket.gethostbyname(host_name)
            except Exception:
                pass
    finally:
        s.close()

    return node_ip_address


def create_redis_client(redis_address, password=None):
    """Create a Redis client.

    Args:
        The IP address, port, and password of the Redis server.

    Returns:
        A Redis client.
    """
    redis_ip_address, redis_port = redis_address.split(":")
    # For this command to work, some other client (on the same machine
    # as Redis) must have run "CONFIG SET protected-mode no".
    return redis.StrictRedis(
        host=redis_ip_address, port=int(redis_port), password=password)


class ProcessControl(object):
    def __init__(self, shutdown_at_exit=True):
        self._processes = collections.defaultdict(lambda :[])
        self._process_types = {}
        self._teardown_order = []
        if shutdown_at_exit:
            atexit.register(lambda: self.kill_all_processes(
                check_alive=False, allow_graceful=True))

    def set_teardown_order(self, order):
        self._teardown_order = order

    def register_process_type(self, process_type, unique=False):
        """Register a process type so that it can be managed"""
        self._process_types[process_type] = ProcessProperty(unique=unique)

    def add_process(self, process_type, process_info):
        # We could also use a defaultdict for this.
        self._processes[process_type].append(process_info)

    def exists_process_type(self, process_type):
        return len(self._processes[process_type]) > 0

    def start_ray_process(self,
                          command,
                          process_type,
                          env_updates=None,
                          cwd=None,
                          use_valgrind=False,
                          use_gdb=False,
                          use_valgrind_profiler=False,
                          use_perftools_profiler=False,
                          use_tmux=False,
                          stdout_file=None,
                          stderr_file=None):
        """Start one of the Ray processes.

        TODO(rkn): We need to figure out how these commands interact. For example,
        it may only make sense to start a process in gdb if we also start it in
        tmux. Similarly, certain combinations probably don't make sense, like
        simultaneously running the process in valgrind and the profiler.

        Args:
            command (List[str]): The command to use to start the Ray process.
            process_type (str): The type of the process that is being started
                (e.g., "raylet").
            env_updates (dict): A dictionary of additional environment variables to
                run the command with (in addition to the caller's environment
                variables).
            cwd (str): The directory to run the process in.
            use_valgrind (bool): True if we should start the process in valgrind.
            use_gdb (bool): True if we should start the process in gdb.
            use_valgrind_profiler (bool): True if we should start the process in
                the valgrind profiler.
            use_perftools_profiler (bool): True if we should profile the process
                using perftools.
            use_tmux (bool): True if we should start the process in tmux.
            stdout_file: A file handle opened for writing to redirect stdout to. If
                no redirection should happen, then this should be None.
            stderr_file: A file handle opened for writing to redirect stderr to. If
                no redirection should happen, then this should be None.

        Returns:
            Information about the process that was started including a handle to
                the process that was started.
        """
        if process_type not in self._process_types:
            raise Exception("Unexcepted process type {}.".format(process_type))
        if self._process_types[process_type].unique:
            if len(self._processes[process_type]) > 0:
                raise Exception("There has been a process instance.")
        # Detect which flags are set through environment variables.
        valgrind_env_var = "RAY_{}_VALGRIND".format(process_type.upper())
        if os.environ.get(valgrind_env_var) == "1":
            logger.info("Detected environment variable '%s'.",
                        valgrind_env_var)
            use_valgrind = True
        valgrind_profiler_env_var = "RAY_{}_VALGRIND_PROFILER".format(
            process_type.upper())
        if os.environ.get(valgrind_profiler_env_var) == "1":
            logger.info("Detected environment variable '%s'.",
                        valgrind_profiler_env_var)
            use_valgrind_profiler = True
        perftools_profiler_env_var = "RAY_{}_PERFTOOLS_PROFILER".format(
            process_type.upper())
        if os.environ.get(perftools_profiler_env_var) == "1":
            logger.info("Detected environment variable '%s'.",
                        perftools_profiler_env_var)
            use_perftools_profiler = True
        tmux_env_var = "RAY_{}_TMUX".format(process_type.upper())
        if os.environ.get(tmux_env_var) == "1":
            logger.info("Detected environment variable '%s'.", tmux_env_var)
            use_tmux = True
        gdb_env_var = "RAY_{}_GDB".format(process_type.upper())
        if os.environ.get(gdb_env_var) == "1":
            logger.info("Detected environment variable '%s'.", gdb_env_var)
            use_gdb = True

        if sum(
                [use_gdb, use_valgrind, use_valgrind_profiler,
                 use_perftools_profiler
                 ]) > 1:
            raise ValueError(
                "At most one of the 'use_gdb', 'use_valgrind', "
                "'use_valgrind_profiler', and 'use_perftools_profiler' flags can "
                "be used at a time.")
        if env_updates is None:
            env_updates = {}
        if not isinstance(env_updates, dict):
            raise ValueError(
                "The 'env_updates' argument must be a dictionary.")

        modified_env = os.environ.copy()
        modified_env.update(env_updates)

        if use_gdb:
            if not use_tmux:
                raise ValueError(
                    "If 'use_gdb' is true, then 'use_tmux' must be true as well.")

            # TODO(suquark): Any better temp file creation here?
            gdb_init_path = "/tmp/ray/gdb_init_{}_{}".format(
                process_type, time.time())
            ray_process_path = command[0]
            ray_process_args = command[1:]
            run_args = " ".join(
                ["'{}'".format(arg) for arg in ray_process_args])
            with open(gdb_init_path, "w") as gdb_init_file:
                gdb_init_file.write("run {}".format(run_args))
            command = ["gdb", ray_process_path, "-x", gdb_init_path]

        if use_valgrind:
            command = [
                          "valgrind", "--track-origins=yes",
                          "--leak-check=full",
                          "--show-leak-kinds=all",
                          "--leak-check-heuristics=stdstring",
                          "--error-exitcode=1"
                      ] + command

        if use_valgrind_profiler:
            command = ["valgrind", "--tool=callgrind"] + command

        if use_perftools_profiler:
            modified_env["LD_PRELOAD"] = os.environ["PERFTOOLS_PATH"]
            modified_env["CPUPROFILE"] = os.environ["PERFTOOLS_LOGFILE"]

        if use_tmux:
            # The command has to be created exactly as below to ensure that it
            # works on all versions of tmux. (Tested with tmux 1.8-5, travis'
            # version, and tmux 2.1)
            command = ["tmux", "new-session", "-d",
                       "{}".format(" ".join(command))]

        process = subprocess.Popen(
            command,
            env=modified_env,
            cwd=cwd,
            stdout=stdout_file,
            stderr=stderr_file)

        process_info = ProcessInfo(
            process=process,
            stdout_file=stdout_file.name if stdout_file is not None else None,
            stderr_file=stderr_file.name if stderr_file is not None else None,
            use_valgrind=use_valgrind,
            use_gdb=use_gdb,
            use_valgrind_profiler=use_valgrind_profiler,
            use_perftools_profiler=use_perftools_profiler,
            use_tmux=use_tmux)
        self._processes[process_type].append(process_info)
        return process_info

    def kill_process_type(self,
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
        process_infos = self._processes[process_type]
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

        del self._processes[process_type]

    def kill_all_processes(self, check_alive=True, allow_graceful=False):
        """Kill all of the processes.

        Note that This is slower than necessary because it calls kill, wait,
        kill, wait, ... instead of kill, kill, ..., wait, wait, ...

        Args:
            check_alive (bool): Raise an exception if any of the processes were
                already dead.
        """
        for process_type in self._teardown_order:
            if process_type in self._processes:
                self.kill_process_type(
                    process_type,
                    check_alive=check_alive,
                    allow_graceful=allow_graceful)

        # We call "list" to copy the keys because we are modifying the
        # dictionary while iterating over it.
        for process_type in list(self._processes.keys()):
            self.kill_process_type(
                process_type,
                check_alive=check_alive,
                allow_graceful=allow_graceful)

    def live_processes(self):
        """Return a list of the live processes.

        Returns:
            A list of the live processes.
        """
        result = []
        for process_type, process_infos in self._processes.items():
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
        for process_type, process_infos in self._processes.items():
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


def wait_for_redis_to_start(redis_ip_address,
                            redis_port,
                            password=None,
                            num_retries=5):
    """Wait for a Redis server to be available.

    This is accomplished by creating a Redis client and sending a random
    command to the server until the command gets through.

    Args:
        redis_ip_address (str): The IP address of the redis server.
        redis_port (int): The port of the redis server.
        password (str): The password of the redis server.
        num_retries (int): The number of times to try connecting with redis.
            The client will sleep for one second between attempts.

    Raises:
        Exception: An exception is raised if we could not connect with Redis.
    """
    redis_client = redis.StrictRedis(
        host=redis_ip_address, port=redis_port, password=password)
    # Wait for the Redis server to start.
    counter = 0
    while counter < num_retries:
        try:
            # Run some random command and see if it worked.
            logger.info(
                "Waiting for redis server at {}:{} to respond...".format(
                    redis_ip_address, redis_port))
            redis_client.client_list()
        except redis.ConnectionError:
            # Wait a little bit.
            time.sleep(1)
            logger.info("Failed to connect to the redis server, retrying.")
            counter += 1
        else:
            break
    if counter == num_retries:
        raise Exception("Unable to connect to Redis. If the Redis instance is "
                        "on a different machine, check that your firewall is "
                        "configured properly.")


def _autodetect_num_gpus():
    """Attempt to detect the number of GPUs on this machine.

    TODO(rkn): This currently assumes Nvidia GPUs and Linux.

    Returns:
        The number of GPUs if any were detected, otherwise 0.
    """
    proc_gpus_path = "/proc/driver/nvidia/gpus"
    if os.path.isdir(proc_gpus_path):
        return len(os.listdir(proc_gpus_path))
    return 0


def _compute_version_info():
    """Compute the versions of Python, pyarrow, and Ray.

    Returns:
        A tuple containing the version information.
    """
    ray_version = ray.__version__
    python_version = ".".join(map(str, sys.version_info[:3]))
    pyarrow_version = pyarrow.__version__
    return ray_version, python_version, pyarrow_version


def _put_version_info_in_redis(redis_client):
    """Store version information in Redis.

    This will be used to detect if workers or drivers are started using
    different versions of Python, pyarrow, or Ray.

    Args:
        redis_client: A client for the primary Redis shard.
    """
    redis_client.set("VERSION_INFO", json.dumps(_compute_version_info()))


def check_version_info(redis_client):
    """Check if various version info of this process is correct.

    This will be used to detect if workers or drivers are started using
    different versions of Python, pyarrow, or Ray. If the version
    information is not present in Redis, then no check is done.

    Args:
        redis_client: A client for the primary Redis shard.

    Raises:
        Exception: An exception is raised if there is a version mismatch.
    """
    redis_reply = redis_client.get("VERSION_INFO")

    # Don't do the check if there is no version information in Redis. This
    # is to make it easier to do things like start the processes by hand.
    if redis_reply is None:
        return

    true_version_info = tuple(json.loads(ray.utils.decode(redis_reply)))
    version_info = _compute_version_info()
    if version_info != true_version_info:
        node_ip_address = ray.services.get_node_ip_address()
        error_message = ("Version mismatch: The cluster was started with:\n"
                         "    Ray: " + true_version_info[0] + "\n"
                         "    Python: " + true_version_info[1] + "\n"
                         "    Pyarrow: " + str(true_version_info[2]) + "\n"
                         "This process on node " + node_ip_address +
                         " was started with:" + "\n"
                         "    Ray: " + version_info[0] + "\n"
                         "    Python: " + version_info[1] + "\n"
                         "    Pyarrow: " + str(version_info[2]))
        if version_info[:2] != true_version_info[:2]:
            raise Exception(error_message)
        else:
            logger.warning(error_message)


def setup_redis_instance(port, password,
                         redis_max_memory=None,
                         redis_max_clients=None):
    # Create a Redis client just for configuring Redis.
    redis_client = redis.StrictRedis(
        host="127.0.0.1", port=port, password=password)
    # Wait for the Redis server to start.
    wait_for_redis_to_start("127.0.0.1", port, password=password)
    # Configure Redis to generate keyspace notifications. TODO(rkn): Change
    # this to only generate notifications for the export keys.
    redis_client.config_set("notify-keyspace-events", "Kl")

    # Configure Redis to not run in protected mode so that processes on other
    # hosts can connect to it. TODO(rkn): Do this in a more secure way.
    redis_client.config_set("protected-mode", "no")

    # Discard old task and object metadata.
    if redis_max_memory is not None:
        redis_client.config_set("maxmemory", str(redis_max_memory))
        redis_client.config_set("maxmemory-policy", "allkeys-lru")
        redis_client.config_set("maxmemory-samples", "10")
        logger.info("Starting Redis shard with {} GB max memory.".format(
            round(redis_max_memory / 1e9, 2)))

    # If redis_max_clients is provided, attempt to raise the number of maximum
    # number of Redis clients.
    if redis_max_clients is not None:
        redis_client.config_set("maxclients", str(redis_max_clients))
    else:
        # If redis_max_clients is not provided, determine the current ulimit.
        # We will use this to attempt to raise the maximum number of Redis
        # clients.
        current_max_clients = int(
            redis_client.config_get("maxclients")["maxclients"])
        # The below command should be the same as doing ulimit -n.
        ulimit_n = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        # The quantity redis_client_buffer appears to be the required buffer
        # between the maximum number of redis clients and ulimit -n. That is,
        # if ulimit -n returns 10000, then we can set maxclients to
        # 10000 - redis_client_buffer.
        redis_client_buffer = 32
        if current_max_clients < ulimit_n - redis_client_buffer:
            redis_client.config_set("maxclients",
                                    ulimit_n - redis_client_buffer)

    # Increase the hard and soft limits for the redis client pubsub buffer to
    # 128MB. This is a hack to make it less likely for pubsub messages to be
    # dropped and for pubsub connections to therefore be killed.
    cur_config = (redis_client.config_get("client-output-buffer-limit")[
        "client-output-buffer-limit"])
    cur_config_list = cur_config.split()
    assert len(cur_config_list) == 12
    cur_config_list[8:] = ["pubsub", "134217728", "134217728", "60"]
    redis_client.config_set("client-output-buffer-limit",
                            " ".join(cur_config_list))
    # Put a time stamp in Redis to indicate when it was started.
    redis_client.set("redis_start_time", time.time())


def check_and_update_resources(num_cpus, num_gpus, resources):
    """Sanity check a resource dictionary and add sensible defaults.

    Args:
        num_cpus: The number of CPUs.
        num_gpus: The number of GPUs.
        resources: A dictionary mapping resource names to resource quantities.

    Returns:
        A new resource dictionary.
    """
    if resources is None:
        resources = {}
    resources = resources.copy()
    assert "CPU" not in resources
    assert "GPU" not in resources
    if num_cpus is not None:
        resources["CPU"] = num_cpus
    if num_gpus is not None:
        resources["GPU"] = num_gpus

    if "CPU" not in resources:
        # By default, use the number of hardware execution threads for the
        # number of cores.
        resources["CPU"] = multiprocessing.cpu_count()

    # See if CUDA_VISIBLE_DEVICES has already been set.
    gpu_ids = ray.utils.get_cuda_visible_devices()

    # Check that the number of GPUs that the raylet wants doesn't
    # excede the amount allowed by CUDA_VISIBLE_DEVICES.
    if ("GPU" in resources and gpu_ids is not None
            and resources["GPU"] > len(gpu_ids)):
        raise Exception("Attempting to start raylet with {} GPUs, "
                        "but CUDA_VISIBLE_DEVICES contains {}.".format(
                            resources["GPU"], gpu_ids))

    if "GPU" not in resources:
        # Try to automatically detect the number of GPUs.
        resources["GPU"] = _autodetect_num_gpus()
        # Don't use more GPUs than allowed by CUDA_VISIBLE_DEVICES.
        if gpu_ids is not None:
            resources["GPU"] = min(resources["GPU"], len(gpu_ids))

    resources = {
        resource_label: resource_quantity
        for resource_label, resource_quantity in resources.items()
        if resource_quantity != 0
    }

    # Check types.
    for _, resource_quantity in resources.items():
        assert (isinstance(resource_quantity, int)
                or isinstance(resource_quantity, float))
        if (isinstance(resource_quantity, float)
                and not resource_quantity.is_integer()):
            raise ValueError(
                "Resource quantities must all be whole numbers. Received {}.".
                format(resources))
        if resource_quantity < 0:
            raise ValueError(
                "Resource quantities must be nonnegative. Received {}.".format(
                    resources))
        if resource_quantity > ray_constants.MAX_RESOURCE_QUANTITY:
            raise ValueError("Resource quantities must be at most {}.".format(
                ray_constants.MAX_RESOURCE_QUANTITY))

    return resources


def determine_plasma_store_config(object_store_memory=None,
                                  plasma_directory=None,
                                  huge_pages=False):
    """Figure out how to configure the plasma object store.

    This will determine which directory to use for the plasma store (e.g.,
    /tmp or /dev/shm) and how much memory to start the store with. On Linux,
    we will try to use /dev/shm unless the shared memory file system is too
    small, in which case we will fall back to /tmp. If any of the object store
    memory or plasma directory parameters are specified by the user, then those
    values will be preserved.

    Args:
        object_store_memory (int): The user-specified object store memory
            parameter.
        plasma_directory (str): The user-specified plasma directory parameter.
        huge_pages (bool): The user-specified huge pages parameter.

    Returns:
        A tuple of the object store memory to use and the plasma directory to
            use. If either of these values is specified by the user, then that
            value will be preserved.
    """
    system_memory = ray.utils.get_system_memory()

    # Choose a default object store size.
    if object_store_memory is None:
        object_store_memory = int(system_memory * 0.3)
        # Cap memory to avoid memory waste and perf issues on large nodes
        if (object_store_memory >
                ray_constants.DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES):
            logger.warning(
                "Warning: Capping object memory store to {}GB. ".format(
                    ray_constants.DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES // 1e9)
                + "To increase this further, specify `object_store_memory` "
                "when calling ray.init() or ray start.")
            object_store_memory = (
                ray_constants.DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES)

    # Determine which directory to use. By default, use /tmp on MacOS and
    # /dev/shm on Linux, unless the shared-memory file system is too small,
    # in which case we default to /tmp on Linux.
    if plasma_directory is None:
        if sys.platform == "linux" or sys.platform == "linux2":
            shm_avail = ray.utils.get_shared_memory_bytes()
            # Compare the requested memory size to the memory available in
            # /dev/shm.
            if shm_avail > object_store_memory:
                plasma_directory = "/dev/shm"
            else:
                plasma_directory = "/tmp"
                logger.warning(
                    "WARNING: The object store is using /tmp instead of "
                    "/dev/shm because /dev/shm has only {} bytes available. "
                    "This may slow down performance! You may be able to free "
                    "up space by deleting files in /dev/shm or terminating "
                    "any running plasma_store_server processes. If you are "
                    "inside a Docker container, you may need to pass an "
                    "argument with the flag '--shm-size' to 'docker run'.".
                    format(shm_avail))
        else:
            plasma_directory = "/tmp"

        # Do some sanity checks.
        if object_store_memory > system_memory:
            raise Exception(
                "The requested object store memory size is greater "
                "than the total available memory.")
    else:
        plasma_directory = os.path.abspath(plasma_directory)
        logger.warning("WARNING: object_store_memory is not verified when "
                       "plasma_directory is set.")

    if not os.path.isdir(plasma_directory):
        raise Exception(
            "The file {} does not exist or is not a directory.".format(
                plasma_directory))

    if object_store_memory < ray_constants.OBJECT_STORE_MINIMUM_MEMORY_BYTES:
        raise ValueError(
            "Attempting to cap object store memory usage at {} "
            "bytes, but the minimum allowed is {} bytes.".format(
                object_store_memory,
                ray_constants.OBJECT_STORE_MINIMUM_MEMORY_BYTES))

    return object_store_memory, plasma_directory
