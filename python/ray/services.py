from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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
from collections import OrderedDict
import redis

import pyarrow
# Ray modules
import ray.ray_constants
import ray.plasma

from ray.tempfile_services import (
    get_ipython_notebook_path, get_logs_dir_path, get_raylet_socket_name,
    get_temp_root, new_log_monitor_log_file, new_monitor_log_file,
    new_plasma_store_log_file, new_raylet_log_file, new_redis_log_file,
    new_webui_log_file, set_temp_root)

PROCESS_TYPE_MONITOR = "monitor"
PROCESS_TYPE_LOG_MONITOR = "log_monitor"
PROCESS_TYPE_WORKER = "worker"
PROCESS_TYPE_RAYLET = "raylet"
PROCESS_TYPE_PLASMA_STORE = "plasma_store"
PROCESS_TYPE_REDIS_SERVER = "redis_server"
PROCESS_TYPE_WEB_UI = "web_ui"

# Max bytes to allocate to plasma unless overriden by the user
MAX_DEFAULT_MEM = 20 * 1000 * 1000 * 1000

# This is a dictionary tracking all of the processes of different types that
# have been started by this services module. Note that the order of the keys is
# important because it determines the order in which these processes will be
# terminated when Ray exits, and certain orders will cause errors to be logged
# to the screen.
all_processes = OrderedDict(
    [(PROCESS_TYPE_MONITOR, []), (PROCESS_TYPE_LOG_MONITOR, []),
     (PROCESS_TYPE_WORKER, []), (PROCESS_TYPE_RAYLET, []),
     (PROCESS_TYPE_PLASMA_STORE, []), (PROCESS_TYPE_REDIS_SERVER, []),
     (PROCESS_TYPE_WEB_UI, [])], )

# True if processes are run in the valgrind profiler.
RUN_RAYLET_PROFILER = False
RUN_PLASMA_STORE_PROFILER = False

# Location of the redis server and module.
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

# Location of the raylet executables.
RAYLET_MONITOR_EXECUTABLE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "core/src/ray/raylet/raylet_monitor")
RAYLET_EXECUTABLE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "core/src/ray/raylet/raylet")

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray configures it by default automatically
# using logging.basicConfig in its entry/init points.
logger = logging.getLogger(__name__)


def address(ip_address, port):
    return ip_address + ":" + str(port)


def get_ip_address(address):
    assert type(address) == str, "Address must be a string"
    ip_address = address.split(":")[0]
    return ip_address


def get_port(address):
    try:
        port = int(address.split(":")[1])
    except Exception:
        raise Exception("Unable to parse port from address {}".format(address))
    return port


def new_port():
    return random.randint(10000, 65535)


def kill_process(p):
    """Kill a process.

    Args:
        p: The process to kill.

    Returns:
        True if the process was killed successfully and false otherwise.
    """
    if p.poll() is not None:
        # The process has already terminated.
        return True
    if any([RUN_RAYLET_PROFILER, RUN_PLASMA_STORE_PROFILER]):
        # Give process signal to write profiler data.
        os.kill(p.pid, signal.SIGINT)
        # Wait for profiling data to be written.
        time.sleep(0.1)

    # Allow the process one second to exit gracefully.
    p.terminate()
    timer = threading.Timer(1, lambda p: p.kill(), [p])
    try:
        timer.start()
        p.wait()
    finally:
        timer.cancel()

    if p.poll() is not None:
        return True

    # If the process did not exit within one second, force kill it.
    p.kill()
    if p.poll() is not None:
        return True

    # The process was not killed for some reason.
    return False


def cleanup():
    """When running in local mode, shutdown the Ray processes.

    This method is used to shutdown processes that were started with
    services.start_ray_head(). It kills all scheduler, object store, and worker
    processes that were started by this services module. Driver processes are
    started and disconnected by worker.py.
    """
    successfully_shut_down = True
    # Terminate the processes in reverse order.
    for process_type in all_processes.keys():
        # Kill all of the processes of a certain type.
        for p in all_processes[process_type]:
            success = kill_process(p)
            successfully_shut_down = successfully_shut_down and success
        # Reset the list of processes of this type.
        all_processes[process_type] = []
    if not successfully_shut_down:
        logger.warning("Ray did not shut down properly.")


def all_processes_alive(exclude=None):
    """Check if all of the processes are still alive.

    Args:
        exclude: Don't check the processes whose types are in this list.
    """

    if exclude is None:
        exclude = []
    for process_type, processes in all_processes.items():
        # Note that p.poll() returns the exit code that the process exited
        # with, so an exit code of None indicates that the process is still
        # alive.
        processes_alive = [p.poll() is None for p in processes]
        if not all(processes_alive) and process_type not in exclude:
            logger.warning(
                "A process of type {} has died.".format(process_type))
            return False
    return True


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

    return node_ip_address


def record_log_files_in_redis(redis_address,
                              node_ip_address,
                              log_files,
                              password=None):
    """Record in Redis that a new log file has been created.

    This is used so that each log monitor can check Redis and figure out which
    log files it is reponsible for monitoring.

    Args:
        redis_address: The address of the redis server.
        node_ip_address: The IP address of the node that the log file exists
            on.
        log_files: A list of file handles for the log files. If one of the file
            handles is None, we ignore it.
        password (str): The password of the redis server.
    """
    for log_file in log_files:
        if log_file is not None:
            redis_ip_address, redis_port = redis_address.split(":")
            redis_client = redis.StrictRedis(
                host=redis_ip_address, port=redis_port, password=password)
            # The name of the key storing the list of log filenames for this IP
            # address.
            log_file_list_key = "LOG_FILENAMES:{}".format(node_ip_address)
            redis_client.rpush(log_file_list_key, log_file.name)


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


def start_redis(node_ip_address,
                port=None,
                redis_shard_ports=None,
                num_redis_shards=1,
                redis_max_clients=None,
                redirect_output=False,
                redirect_worker_output=False,
                cleanup=True,
                password=None,
                use_credis=None):
    """Start the Redis global state store.

    Args:
        node_ip_address: The IP address of the current node. This is only used
            for recording the log filenames in Redis.
        port (int): If provided, the primary Redis shard will be started on
            this port.
        redis_shard_ports: A list of the ports to use for the non-primary Redis
            shards.
        num_redis_shards (int): If provided, the number of Redis shards to
            start, in addition to the primary one. The default value is one
            shard.
        redis_max_clients: If this is provided, Ray will attempt to configure
            Redis with this maxclients number.
        redirect_output (bool): True if output should be redirected to a file
            and false otherwise.
        redirect_worker_output (bool): True if worker output should be
            redirected to a file and false otherwise. Workers will have access
            to this value when they start up.
        cleanup (bool): True if using Ray in local mode. If cleanup is true,
            then all Redis processes started by this method will be killed by
            services.cleanup() when the Python process that imported services
            exits.
        password (str): Prevents external clients without the password
            from connecting to Redis if provided.
        use_credis: If True, additionally load the chain-replicated libraries
            into the redis servers.  Defaults to None, which means its value is
            set by the presence of "RAY_USE_NEW_GCS" in os.environ.

    Returns:
        A tuple of the address for the primary Redis shard and a list of
            addresses for the remaining shards.
    """
    redis_stdout_file, redis_stderr_file = new_redis_log_file(redirect_output)

    if redis_shard_ports is None:
        redis_shard_ports = num_redis_shards * [None]
    elif len(redis_shard_ports) != num_redis_shards:
        raise Exception("The number of Redis shard ports does not match the "
                        "number of Redis shards.")

    if use_credis is None:
        use_credis = ("RAY_USE_NEW_GCS" in os.environ)
    if use_credis and password is not None:
        # TODO(pschafhalter) remove this once credis supports
        # authenticating Redis ports
        raise Exception("Setting the `redis_password` argument is not "
                        "supported in credis. To run Ray with "
                        "password-protected Redis ports, ensure that "
                        "the environment variable `RAY_USE_NEW_GCS=off`.")
    if not use_credis:
        assigned_port, _ = _start_redis_instance(
            node_ip_address=node_ip_address,
            port=port,
            redis_max_clients=redis_max_clients,
            stdout_file=redis_stdout_file,
            stderr_file=redis_stderr_file,
            cleanup=cleanup,
            password=password)
    else:
        assigned_port, _ = _start_redis_instance(
            node_ip_address=node_ip_address,
            port=port,
            redis_max_clients=redis_max_clients,
            stdout_file=redis_stdout_file,
            stderr_file=redis_stderr_file,
            cleanup=cleanup,
            executable=CREDIS_EXECUTABLE,
            # It is important to load the credis module BEFORE the ray module,
            # as the latter contains an extern declaration that the former
            # supplies.
            modules=[CREDIS_MASTER_MODULE, REDIS_MODULE],
            password=password)
    if port is not None:
        assert assigned_port == port
    port = assigned_port
    redis_address = address(node_ip_address, port)

    # Register the number of Redis shards in the primary shard, so that clients
    # know how many redis shards to expect under RedisShards.
    primary_redis_client = redis.StrictRedis(
        host=node_ip_address, port=port, password=password)
    primary_redis_client.set("NumRedisShards", str(num_redis_shards))

    # Put the redirect_worker_output bool in the Redis shard so that workers
    # can access it and know whether or not to redirect their output.
    primary_redis_client.set("RedirectOutput", 1
                             if redirect_worker_output else 0)

    # Store version information in the primary Redis shard.
    _put_version_info_in_redis(primary_redis_client)

    # Start other Redis shards. Each Redis shard logs to a separate file,
    # prefixed by "redis-<shard number>".
    redis_shards = []
    for i in range(num_redis_shards):
        redis_stdout_file, redis_stderr_file = new_redis_log_file(
            redirect_output, shard_number=i)
        if not use_credis:
            redis_shard_port, _ = _start_redis_instance(
                node_ip_address=node_ip_address,
                port=redis_shard_ports[i],
                redis_max_clients=redis_max_clients,
                stdout_file=redis_stdout_file,
                stderr_file=redis_stderr_file,
                cleanup=cleanup,
                password=password)
        else:
            assert num_redis_shards == 1, \
                "For now, RAY_USE_NEW_GCS supports 1 shard, and credis "\
                "supports 1-node chain for that shard only."
            redis_shard_port, _ = _start_redis_instance(
                node_ip_address=node_ip_address,
                port=redis_shard_ports[i],
                redis_max_clients=redis_max_clients,
                stdout_file=redis_stdout_file,
                stderr_file=redis_stderr_file,
                cleanup=cleanup,
                password=password,
                executable=CREDIS_EXECUTABLE,
                # It is important to load the credis module BEFORE the ray
                # module, as the latter contains an extern declaration that the
                # former supplies.
                modules=[CREDIS_MEMBER_MODULE, REDIS_MODULE])

        if redis_shard_ports[i] is not None:
            assert redis_shard_port == redis_shard_ports[i]
        shard_address = address(node_ip_address, redis_shard_port)
        redis_shards.append(shard_address)
        # Store redis shard information in the primary redis shard.
        primary_redis_client.rpush("RedisShards", shard_address)

    if use_credis:
        shard_client = redis.StrictRedis(
            host=node_ip_address, port=redis_shard_port, password=password)
        # Configure the chain state.
        primary_redis_client.execute_command("MASTER.ADD", node_ip_address,
                                             redis_shard_port)
        shard_client.execute_command("MEMBER.CONNECT_TO_MASTER",
                                     node_ip_address, port)

    return redis_address, redis_shards


def _start_redis_instance(node_ip_address="127.0.0.1",
                          port=None,
                          redis_max_clients=None,
                          num_retries=20,
                          stdout_file=None,
                          stderr_file=None,
                          cleanup=True,
                          password=None,
                          executable=REDIS_EXECUTABLE,
                          modules=None):
    """Start a single Redis server.

    Args:
        node_ip_address (str): The IP address of the current node. This is only
            used for recording the log filenames in Redis.
        port (int): If provided, start a Redis server with this port.
        redis_max_clients: If this is provided, Ray will attempt to configure
            Redis with this maxclients number.
        num_retries (int): The number of times to attempt to start Redis. If a
            port is provided, this defaults to 1.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        cleanup (bool): True if using Ray in local mode. If cleanup is true,
            then this process will be killed by serices.cleanup() when the
            Python process that imported services exits.
        password (str): Prevents external clients without the password
            from connecting to Redis if provided.
        executable (str): Full path tho the redis-server executable.
        modules (list of str): A list of pathnames, pointing to the redis
            module(s) that will be loaded in this redis server.  If None, load
            the default Ray redis module.

    Returns:
        A tuple of the port used by Redis and a handle to the process that was
            started. If a port is passed in, then the returned port value is
            the same.

    Raises:
        Exception: An exception is raised if Redis could not be started.
    """
    assert os.path.isfile(executable)
    if modules is None:
        modules = [REDIS_MODULE]
    for module in modules:
        assert os.path.isfile(module)
    counter = 0
    if port is not None:
        # If a port is specified, then try only once to connect.
        num_retries = 1
    else:
        port = new_port()

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
            ["--port", str(port), "--loglevel", "warning"] + load_module_args)

        p = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
        time.sleep(0.1)
        # Check if Redis successfully started (or at least if it the executable
        # did not exit within 0.1 seconds).
        if p.poll() is None:
            if cleanup:
                all_processes[PROCESS_TYPE_REDIS_SERVER].append(p)
            break
        port = new_port()
        counter += 1
    if counter == num_retries:
        raise Exception("Couldn't start Redis. Check log files: {} {}".format(
            stdout_file.name, stderr_file.name))

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
    # Record the log files in Redis.
    record_log_files_in_redis(
        address(node_ip_address, port),
        node_ip_address, [stdout_file, stderr_file],
        password=password)
    return port, p


def start_log_monitor(redis_address,
                      node_ip_address,
                      stdout_file=None,
                      stderr_file=None,
                      cleanup=cleanup,
                      redis_password=None):
    """Start a log monitor process.

    Args:
        redis_address (str): The address of the Redis instance.
        node_ip_address (str): The IP address of the node that this log monitor
            is running on.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        cleanup (bool): True if using Ray in local mode. If cleanup is true,
            then this process will be killed by services.cleanup() when the
            Python process that imported services exits.
        redis_password (str): The password of the redis server.
    """
    log_monitor_filepath = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "log_monitor.py")
    command = [
        sys.executable, "-u", log_monitor_filepath, "--redis-address",
        redis_address, "--node-ip-address", node_ip_address
    ]
    if redis_password:
        command += ["--redis-password", redis_password]
    p = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
    if cleanup:
        all_processes[PROCESS_TYPE_LOG_MONITOR].append(p)
    record_log_files_in_redis(
        redis_address,
        node_ip_address, [stdout_file, stderr_file],
        password=redis_password)


def start_ui(redis_address, stdout_file=None, stderr_file=None, cleanup=True):
    """Start a UI process.

    Args:
        redis_address: The address of the primary Redis shard.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        cleanup (bool): True if using Ray in local mode. If cleanup is true,
            then this process will be killed by services.cleanup() when the
            Python process that imported services exits.
    """

    port = 8888
    while True:
        try:
            port_test_socket = socket.socket()
            port_test_socket.bind(("127.0.0.1", port))
            port_test_socket.close()
            break
        except socket.error:
            port += 1
    new_env = os.environ.copy()
    new_env["REDIS_ADDRESS"] = redis_address
    # We generate the token used for authentication ourselves to avoid
    # querying the jupyter server.
    new_notebook_directory, webui_url, token = (
        get_ipython_notebook_path(port))
    # The --ip=0.0.0.0 flag is intended to enable connecting to a notebook
    # running within a docker container (from the outside).
    command = [
        "jupyter", "notebook", "--no-browser", "--port={}".format(port),
        "--ip=0.0.0.0", "--NotebookApp.iopub_data_rate_limit=10000000000",
        "--NotebookApp.open_browser=False",
        "--NotebookApp.token={}".format(token)
    ]
    # If the user is root, add the --allow-root flag.
    if os.geteuid() == 0:
        command.append("--allow-root")

    try:
        ui_process = subprocess.Popen(
            command,
            env=new_env,
            cwd=new_notebook_directory,
            stdout=stdout_file,
            stderr=stderr_file)
    except Exception:
        logger.warning("Failed to start the UI, you may need to run "
                       "'pip install jupyter'.")
    else:
        if cleanup:
            all_processes[PROCESS_TYPE_WEB_UI].append(ui_process)
        logger.info("\n" + "=" * 70)
        logger.info("View the web UI at {}".format(webui_url))
        logger.info("=" * 70 + "\n")
        return webui_url


def check_and_update_resources(resources):
    """Sanity check a resource dictionary and add sensible defaults.

    Args:
        resources: A dictionary mapping resource names to resource quantities.

    Returns:
        A new resource dictionary.
    """
    if resources is None:
        resources = {}
    resources = resources.copy()
    if "CPU" not in resources:
        # By default, use the number of hardware execution threads for the
        # number of cores.
        resources["CPU"] = multiprocessing.cpu_count()

    # See if CUDA_VISIBLE_DEVICES has already been set.
    gpu_ids = ray.utils.get_cuda_visible_devices()

    # Check that the number of GPUs that the local scheduler wants doesn't
    # excede the amount allowed by CUDA_VISIBLE_DEVICES.
    if ("GPU" in resources and gpu_ids is not None
            and resources["GPU"] > len(gpu_ids)):
        raise Exception("Attempting to start local scheduler with {} GPUs, "
                        "but CUDA_VISIBLE_DEVICES contains {}.".format(
                            resources["GPU"], gpu_ids))

    if "GPU" not in resources:
        # Try to automatically detect the number of GPUs.
        resources["GPU"] = _autodetect_num_gpus()
        # Don't use more GPUs than allowed by CUDA_VISIBLE_DEVICES.
        if gpu_ids is not None:
            resources["GPU"] = min(resources["GPU"], len(gpu_ids))

    # Check types.
    for _, resource_quantity in resources.items():
        assert (isinstance(resource_quantity, int)
                or isinstance(resource_quantity, float))
        if (isinstance(resource_quantity, float)
                and not resource_quantity.is_integer()):
            raise ValueError("Resource quantities must all be whole numbers.")

        if resource_quantity > ray.ray_constants.MAX_RESOURCE_QUANTITY:
            raise ValueError("Resource quantities must be at most {}.".format(
                ray.ray_constants.MAX_RESOURCE_QUANTITY))

    return resources


def start_raylet(redis_address,
                 node_ip_address,
                 raylet_name,
                 plasma_store_name,
                 worker_path,
                 resources=None,
                 object_manager_port=None,
                 node_manager_port=None,
                 num_workers=0,
                 use_valgrind=False,
                 use_profiler=False,
                 stdout_file=None,
                 stderr_file=None,
                 cleanup=True,
                 config=None,
                 redis_password=None):
    """Start a raylet, which is a combined local scheduler and object manager.

    Args:
        redis_address (str): The address of the Redis instance.
        node_ip_address (str): The IP address of the node that this local
            scheduler is running on.
        plasma_store_name (str): The name of the plasma store socket to connect
            to.
        raylet_name (str): The name of the raylet socket to create.
        worker_path (str): The path of the script to use when the local
            scheduler starts up new workers.
        resources: The resources that this raylet has.
        object_manager_port (int): The port to use for the object manager. If
            this is not provided, we will use 0 and the object manager will
            choose its own port.
        node_manager_port (int): The port to use for the node manager. If
            this is not provided, we will use 0 and the node manager will
            choose its own port.
        use_valgrind (bool): True if the raylet should be started inside
            of valgrind. If this is True, use_profiler must be False.
        use_profiler (bool): True if the raylet should be started inside
            a profiler. If this is True, use_valgrind must be False.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        cleanup (bool): True if using Ray in local mode. If cleanup is true,
            then this process will be killed by serices.cleanup() when the
            Python process that imported services exits.
        config (dict|None): Optional Raylet configuration that will
            override defaults in RayConfig.
        redis_password (str): The password of the redis server.

    Returns:
        The raylet socket name.
    """
    config = config or {}
    config_str = ",".join(["{},{}".format(*kv) for kv in config.items()])

    if use_valgrind and use_profiler:
        raise Exception("Cannot use valgrind and profiler at the same time.")

    static_resources = check_and_update_resources(resources)

    # Limit the number of workers that can be started in parallel by the
    # raylet. However, make sure it is at least 1.
    maximum_startup_concurrency = max(
        1, min(multiprocessing.cpu_count(), static_resources["CPU"]))

    # Format the resource argument in a form like 'CPU,1.0,GPU,0,Custom,3'.
    resource_argument = ",".join(
        ["{},{}".format(*kv) for kv in static_resources.items()])

    gcs_ip_address, gcs_port = redis_address.split(":")

    # Create the command that the Raylet will use to start workers.
    start_worker_command = ("{} {} "
                            "--node-ip-address={} "
                            "--object-store-name={} "
                            "--raylet-name={} "
                            "--redis-address={} "
                            "--temp-dir={}".format(
                                sys.executable, worker_path, node_ip_address,
                                plasma_store_name, raylet_name, redis_address,
                                get_temp_root()))
    if redis_password:
        start_worker_command += " --redis-password {}".format(redis_password)

    # If the object manager port is None, then use 0 to cause the object
    # manager to choose its own port.
    if object_manager_port is None:
        object_manager_port = 0
    # If the node manager port is None, then use 0 to cause the node manager
    # to choose its own port.
    if node_manager_port is None:
        node_manager_port = 0

    command = [
        RAYLET_EXECUTABLE,
        raylet_name,
        plasma_store_name,
        str(object_manager_port),
        str(node_manager_port),
        node_ip_address,
        gcs_ip_address,
        gcs_port,
        str(num_workers),
        str(maximum_startup_concurrency),
        resource_argument,
        config_str,
        start_worker_command,
        "",  # Worker command for Java, not needed for Python.
        redis_password or "",
    ]

    if use_valgrind:
        pid = subprocess.Popen(
            [
                "valgrind", "--track-origins=yes", "--leak-check=full",
                "--show-leak-kinds=all", "--leak-check-heuristics=stdstring",
                "--error-exitcode=1"
            ] + command,
            stdout=stdout_file,
            stderr=stderr_file)
    elif use_profiler:
        pid = subprocess.Popen(
            ["valgrind", "--tool=callgrind"] + command,
            stdout=stdout_file,
            stderr=stderr_file)
    else:
        pid = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)

    if cleanup:
        all_processes[PROCESS_TYPE_RAYLET].append(pid)
    record_log_files_in_redis(
        redis_address,
        node_ip_address, [stdout_file, stderr_file],
        password=redis_password)

    return raylet_name


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
        object_store_memory = int(system_memory * 0.4)
        # Cap memory to avoid memory waste and perf issues on large nodes
        if object_store_memory > MAX_DEFAULT_MEM:
            logger.warning(
                "Warning: Capping object memory store to {}GB. ".format(
                    MAX_DEFAULT_MEM // 1e9) +
                "To increase this further, specify `object_store_memory` "
                "when calling ray.init() or ray start.")
            object_store_memory = MAX_DEFAULT_MEM

    if plasma_directory is not None:
        plasma_directory = os.path.abspath(plasma_directory)

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
                    "argument with the flag '--shm-size' to 'docker run'."
                    .format(shm_avail))
        else:
            plasma_directory = "/tmp"

    # Do some sanity checks.
    if object_store_memory > system_memory:
        raise Exception("The requested object store memory size is greater "
                        "than the total available memory.")

    if not os.path.isdir(plasma_directory):
        raise Exception("The file {} does not exist or is not a directory."
                        .format(plasma_directory))

    return object_store_memory, plasma_directory


def start_plasma_store(node_ip_address,
                       redis_address,
                       object_manager_port=None,
                       store_stdout_file=None,
                       store_stderr_file=None,
                       object_store_memory=None,
                       cleanup=True,
                       plasma_directory=None,
                       huge_pages=False,
                       plasma_store_socket_name=None,
                       redis_password=None):
    """This method starts an object store process.

    Args:
        node_ip_address (str): The IP address of the node running the object
            store.
        redis_address (str): The address of the Redis instance to connect to.
        object_manager_port (int): The port to use for the object manager. If
            this is not provided, one will be generated randomly.
        store_stdout_file: A file handle opened for writing to redirect stdout
            to. If no redirection should happen, then this should be None.
        store_stderr_file: A file handle opened for writing to redirect stderr
            to. If no redirection should happen, then this should be None.
        object_store_memory: The amount of memory (in bytes) to start the
            object store with.
        cleanup (bool): True if using Ray in local mode. If cleanup is true,
            then this process will be killed by serices.cleanup() when the
            Python process that imported services exits.
        plasma_directory: A directory where the Plasma memory mapped files will
            be created.
        huge_pages: Boolean flag indicating whether to start the Object
            Store with hugetlbfs support. Requires plasma_directory.
        redis_password (str): The password of the redis server.

    Return:
        The Plasma store socket name.
    """
    object_store_memory, plasma_directory = determine_plasma_store_config(
        object_store_memory, plasma_directory, huge_pages)

    # Print the object store memory using two decimal places.
    object_store_memory_str = (object_store_memory / 10**7) / 10**2
    logger.info("Starting the Plasma object store with {} GB memory "
                "using {}.".format(object_store_memory_str, plasma_directory))
    # Start the Plasma store.
    plasma_store_name, p1 = ray.plasma.start_plasma_store(
        plasma_store_memory=object_store_memory,
        use_profiler=RUN_PLASMA_STORE_PROFILER,
        stdout_file=store_stdout_file,
        stderr_file=store_stderr_file,
        plasma_directory=plasma_directory,
        huge_pages=huge_pages,
        socket_name=plasma_store_socket_name)

    if cleanup:
        all_processes[PROCESS_TYPE_PLASMA_STORE].append(p1)
    record_log_files_in_redis(
        redis_address,
        node_ip_address, [store_stdout_file, store_stderr_file],
        password=redis_password)

    return plasma_store_name


def start_worker(node_ip_address,
                 object_store_name,
                 local_scheduler_name,
                 redis_address,
                 worker_path,
                 stdout_file=None,
                 stderr_file=None,
                 cleanup=True):
    """This method starts a worker process.

    Args:
        node_ip_address (str): The IP address of the node that this worker is
            running on.
        object_store_name (str): The name of the object store.
        local_scheduler_name (str): The name of the local scheduler.
        redis_address (str): The address that the Redis server is listening on.
        worker_path (str): The path of the source code which the worker process
            will run.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        cleanup (bool): True if using Ray in local mode. If cleanup is true,
            then this process will be killed by services.cleanup() when the
            Python process that imported services exits. This is True by
            default.
    """
    command = [
        sys.executable, "-u", worker_path,
        "--node-ip-address=" + node_ip_address,
        "--object-store-name=" + object_store_name,
        "--local-scheduler-name=" + local_scheduler_name,
        "--redis-address=" + str(redis_address),
        "--temp-dir=" + get_temp_root()
    ]
    p = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
    if cleanup:
        all_processes[PROCESS_TYPE_WORKER].append(p)
    record_log_files_in_redis(redis_address, node_ip_address,
                              [stdout_file, stderr_file])


def start_monitor(redis_address,
                  node_ip_address,
                  stdout_file=None,
                  stderr_file=None,
                  cleanup=True,
                  autoscaling_config=None,
                  redis_password=None):
    """Run a process to monitor the other processes.

    Args:
        redis_address (str): The address that the Redis server is listening on.
        node_ip_address: The IP address of the node that this process will run
            on.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        cleanup (bool): True if using Ray in local mode. If cleanup is true,
            then this process will be killed by services.cleanup() when the
            Python process that imported services exits. This is True by
            default.
        autoscaling_config: path to autoscaling config file.
        redis_password (str): The password of the redis server.
    """
    monitor_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "monitor.py")
    command = [
        sys.executable, "-u", monitor_path,
        "--redis-address=" + str(redis_address)
    ]
    if autoscaling_config:
        command.append("--autoscaling-config=" + str(autoscaling_config))
    if redis_password:
        command.append("--redis-password=" + redis_password)
    p = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
    if cleanup:
        all_processes[PROCESS_TYPE_MONITOR].append(p)
    record_log_files_in_redis(
        redis_address,
        node_ip_address, [stdout_file, stderr_file],
        password=redis_password)


def start_raylet_monitor(redis_address,
                         stdout_file=None,
                         stderr_file=None,
                         cleanup=True,
                         redis_password=None,
                         config=None):
    """Run a process to monitor the other processes.

    Args:
        redis_address (str): The address that the Redis server is listening on.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        cleanup (bool): True if using Ray in local mode. If cleanup is true,
            then this process will be killed by services.cleanup() when the
            Python process that imported services exits. This is True by
            default.
        redis_password (str): The password of the redis server.
        config (dict|None): Optional configuration that will
            override defaults in RayConfig.
    """
    gcs_ip_address, gcs_port = redis_address.split(":")
    redis_password = redis_password or ""
    config = config or {}
    config_str = ",".join(["{},{}".format(*kv) for kv in config.items()])
    command = [RAYLET_MONITOR_EXECUTABLE, gcs_ip_address, gcs_port, config_str]
    if redis_password:
        command += [redis_password]
    p = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
    if cleanup:
        all_processes[PROCESS_TYPE_MONITOR].append(p)


def start_ray_processes(address_info=None,
                        object_manager_ports=None,
                        node_manager_ports=None,
                        node_ip_address="127.0.0.1",
                        redis_port=None,
                        redis_shard_ports=None,
                        num_workers=None,
                        num_local_schedulers=1,
                        object_store_memory=None,
                        num_redis_shards=1,
                        redis_max_clients=None,
                        redis_password=None,
                        worker_path=None,
                        cleanup=True,
                        redirect_worker_output=False,
                        redirect_output=False,
                        include_log_monitor=False,
                        include_webui=False,
                        start_workers_from_local_scheduler=True,
                        resources=None,
                        plasma_directory=None,
                        huge_pages=False,
                        autoscaling_config=None,
                        plasma_store_socket_name=None,
                        raylet_socket_name=None,
                        temp_dir=None,
                        _internal_config=None):
    """Helper method to start Ray processes.

    Args:
        address_info (dict): A dictionary with address information for
            processes that have already been started. If provided, address_info
            will be modified to include processes that are newly started.
        object_manager_ports (list): A list of the ports to use for the object
            managers. There should be one per object manager being started on
            this node (typically just one).
        node_manager_ports (list): A list of the ports to use for the node
            managers. There should be one per node manager being started on
            this node (typically just one).
        node_ip_address (str): The IP address of this node.
        redis_port (int): The port that the primary Redis shard should listen
            to. If None, then a random port will be chosen. If the key
            "redis_address" is in address_info, then this argument will be
            ignored.
        redis_shard_ports: A list of the ports to use for the non-primary Redis
            shards.
        num_workers (int): The number of workers to start.
        num_local_schedulers (int): The total number of local schedulers
            required. This is also the total number of object stores required.
            This method will start new instances of local schedulers and object
            stores until there are num_local_schedulers existing instances of
            each, including ones already registered with the given
            address_info.
        object_store_memory: The amount of memory (in bytes) to start the
            object store with.
        num_redis_shards: The number of Redis shards to start in addition to
            the primary Redis shard.
        redis_max_clients: If provided, attempt to configure Redis with this
            maxclients number.
        redis_password (str): Prevents external clients without the password
            from connecting to Redis if provided.
        worker_path (str): The path of the source code that will be run by the
            worker.
        cleanup (bool): If cleanup is true, then the processes started here
            will be killed by services.cleanup() when the Python process that
            called this method exits.
        redirect_worker_output: True if the stdout and stderr of worker
            processes should be redirected to files.
        redirect_output (bool): True if stdout and stderr for non-worker
            processes should be redirected to files and false otherwise.
        include_log_monitor (bool): If True, then start a log monitor to
            monitor the log files for all processes on this node and push their
            contents to Redis.
        include_webui (bool): If True, then attempt to start the web UI. Note
            that this is only possible with Python 3.
        start_workers_from_local_scheduler (bool): If this flag is True, then
            start the initial workers from the local scheduler. Else, start
            them from Python.
        resources: A dictionary mapping resource name to the quantity of that
            resource.
        plasma_directory: A directory where the Plasma memory mapped files will
            be created.
        huge_pages: Boolean flag indicating whether to start the Object
            Store with hugetlbfs support. Requires plasma_directory.
        autoscaling_config: path to autoscaling config file.
        plasma_store_socket_name (str): If provided, it will specify the socket
            name used by the plasma store.
        raylet_socket_name (str): If provided, it will specify the socket path
            used by the raylet process.
        temp_dir (str): If provided, it will specify the root temporary
            directory for the Ray process.
        _internal_config (str): JSON configuration for overriding
            RayConfig defaults. For testing purposes ONLY.

    Returns:
        A dictionary of the address information for the processes that were
            started.
    """

    set_temp_root(temp_dir)

    logger.info("Process STDOUT and STDERR is being redirected to {}.".format(
        get_logs_dir_path()))

    config = json.loads(_internal_config) if _internal_config else None

    if resources is None:
        resources = {}
    if not isinstance(resources, list):
        resources = num_local_schedulers * [resources]

    if num_workers is not None:
        raise Exception("The 'num_workers' argument is deprecated. Please use "
                        "'num_cpus' instead.")
    else:
        workers_per_local_scheduler = []
        for resource_dict in resources:
            cpus = resource_dict.get("CPU")
            workers_per_local_scheduler.append(cpus if cpus is not None else
                                               multiprocessing.cpu_count())

    if address_info is None:
        address_info = {}
    address_info["node_ip_address"] = node_ip_address

    if worker_path is None:
        worker_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "workers/default_worker.py")

    # Start Redis if there isn't already an instance running. TODO(rkn): We are
    # suppressing the output of Redis because on Linux it prints a bunch of
    # warning messages when it starts up. Instead of suppressing the output, we
    # should address the warnings.
    redis_address = address_info.get("redis_address")
    redis_shards = address_info.get("redis_shards", [])
    if redis_address is None:
        redis_address, redis_shards = start_redis(
            node_ip_address,
            port=redis_port,
            redis_shard_ports=redis_shard_ports,
            num_redis_shards=num_redis_shards,
            redis_max_clients=redis_max_clients,
            redirect_output=True,
            redirect_worker_output=redirect_worker_output,
            cleanup=cleanup,
            password=redis_password)
        address_info["redis_address"] = redis_address
        time.sleep(0.1)

        # Start monitoring the processes.
        monitor_stdout_file, monitor_stderr_file = new_monitor_log_file(
            redirect_output)
        start_monitor(
            redis_address,
            node_ip_address,
            stdout_file=monitor_stdout_file,
            stderr_file=monitor_stderr_file,
            cleanup=cleanup,
            autoscaling_config=autoscaling_config,
            redis_password=redis_password)
        start_raylet_monitor(
            redis_address,
            stdout_file=monitor_stdout_file,
            stderr_file=monitor_stderr_file,
            cleanup=cleanup,
            redis_password=redis_password,
            config=config)
    if redis_shards == []:
        # Get redis shards from primary redis instance.
        redis_ip_address, redis_port = redis_address.split(":")
        redis_client = redis.StrictRedis(
            host=redis_ip_address, port=redis_port, password=redis_password)
        redis_shards = redis_client.lrange("RedisShards", start=0, end=-1)
        redis_shards = [ray.utils.decode(shard) for shard in redis_shards]
        address_info["redis_shards"] = redis_shards

    # Start the log monitor, if necessary.
    if include_log_monitor:
        log_monitor_stdout_file, log_monitor_stderr_file = (
            new_log_monitor_log_file())
        start_log_monitor(
            redis_address,
            node_ip_address,
            stdout_file=log_monitor_stdout_file,
            stderr_file=log_monitor_stderr_file,
            cleanup=cleanup,
            redis_password=redis_password)

    # Initialize with existing services.
    if "object_store_addresses" not in address_info:
        address_info["object_store_addresses"] = []
    object_store_addresses = address_info["object_store_addresses"]
    if "raylet_socket_names" not in address_info:
        address_info["raylet_socket_names"] = []
    raylet_socket_names = address_info["raylet_socket_names"]

    # Get the ports to use for the object managers if any are provided.
    if not isinstance(object_manager_ports, list):
        assert object_manager_ports is None or num_local_schedulers == 1
        object_manager_ports = num_local_schedulers * [object_manager_ports]
    assert len(object_manager_ports) == num_local_schedulers
    if not isinstance(node_manager_ports, list):
        assert node_manager_ports is None or num_local_schedulers == 1
        node_manager_ports = num_local_schedulers * [node_manager_ports]
    assert len(node_manager_ports) == num_local_schedulers

    # Start any object stores that do not yet exist.
    for i in range(num_local_schedulers - len(object_store_addresses)):
        # Start Plasma.
        plasma_store_stdout_file, plasma_store_stderr_file = (
            new_plasma_store_log_file(i, redirect_output))

        object_store_address = start_plasma_store(
            node_ip_address,
            redis_address,
            store_stdout_file=plasma_store_stdout_file,
            store_stderr_file=plasma_store_stderr_file,
            object_store_memory=object_store_memory,
            cleanup=cleanup,
            plasma_directory=plasma_directory,
            huge_pages=huge_pages,
            plasma_store_socket_name=plasma_store_socket_name,
            redis_password=redis_password)
        object_store_addresses.append(object_store_address)
        time.sleep(0.1)

    # Start any raylets that do not exist yet.
    for i in range(len(raylet_socket_names), num_local_schedulers):
        raylet_stdout_file, raylet_stderr_file = new_raylet_log_file(
            i, redirect_output=redirect_worker_output)
        address_info["raylet_socket_names"].append(
            start_raylet(
                redis_address,
                node_ip_address,
                raylet_socket_name or get_raylet_socket_name(),
                object_store_addresses[i],
                worker_path,
                object_manager_port=object_manager_ports[i],
                node_manager_port=node_manager_ports[i],
                resources=resources[i],
                num_workers=workers_per_local_scheduler[i],
                stdout_file=raylet_stdout_file,
                stderr_file=raylet_stderr_file,
                cleanup=cleanup,
                redis_password=redis_password,
                config=config))

    # Try to start the web UI.
    if include_webui:
        ui_stdout_file, ui_stderr_file = new_webui_log_file()
        address_info["webui_url"] = start_ui(
            redis_address,
            stdout_file=ui_stdout_file,
            stderr_file=ui_stderr_file,
            cleanup=cleanup)
    else:
        address_info["webui_url"] = ""
    # Return the addresses of the relevant processes.
    return address_info


def start_ray_node(node_ip_address,
                   redis_address,
                   object_manager_ports=None,
                   node_manager_ports=None,
                   num_workers=None,
                   num_local_schedulers=1,
                   object_store_memory=None,
                   redis_password=None,
                   worker_path=None,
                   cleanup=True,
                   redirect_worker_output=False,
                   redirect_output=False,
                   resources=None,
                   plasma_directory=None,
                   huge_pages=False,
                   plasma_store_socket_name=None,
                   raylet_socket_name=None,
                   temp_dir=None,
                   _internal_config=None):
    """Start the Ray processes for a single node.

    This assumes that the Ray processes on some master node have already been
    started.

    Args:
        node_ip_address (str): The IP address of this node.
        redis_address (str): The address of the Redis server.
        object_manager_ports (list): A list of the ports to use for the object
            managers. There should be one per object manager being started on
            this node (typically just one).
        node_manager_ports (list): A list of the ports to use for the node
            managers. There should be one per node manager being started on
            this node (typically just one).
        num_workers (int): The number of workers to start.
        num_local_schedulers (int): The number of local schedulers to start.
            This is also the number of plasma stores and plasma managers to
            start.
        object_store_memory (int): The maximum amount of memory (in bytes) to
            let the plasma store use.
        redis_password (str): Prevents external clients without the password
            from connecting to Redis if provided.
        worker_path (str): The path of the source code that will be run by the
            worker.
        cleanup (bool): If cleanup is true, then the processes started here
            will be killed by services.cleanup() when the Python process that
            called this method exits.
        redirect_worker_output: True if the stdout and stderr of worker
            processes should be redirected to files.
        redirect_output (bool): True if stdout and stderr for non-worker
            processes should be redirected to files and false otherwise.
        resources: A dictionary mapping resource name to the available quantity
            of that resource.
        plasma_directory: A directory where the Plasma memory mapped files will
            be created.
        huge_pages: Boolean flag indicating whether to start the Object
            Store with hugetlbfs support. Requires plasma_directory.
        plasma_store_socket_name (str): If provided, it will specify the socket
            name used by the plasma store.
        raylet_socket_name (str): If provided, it will specify the socket path
            used by the raylet process.
        temp_dir (str): If provided, it will specify the root temporary
            directory for the Ray process.
        _internal_config (str): JSON configuration for overriding
            RayConfig defaults. For testing purposes ONLY.

    Returns:
        A dictionary of the address information for the processes that were
            started.
    """
    address_info = {
        "redis_address": redis_address,
    }
    return start_ray_processes(
        address_info=address_info,
        object_manager_ports=object_manager_ports,
        node_manager_ports=node_manager_ports,
        node_ip_address=node_ip_address,
        num_workers=num_workers,
        num_local_schedulers=num_local_schedulers,
        object_store_memory=object_store_memory,
        redis_password=redis_password,
        worker_path=worker_path,
        include_log_monitor=True,
        cleanup=cleanup,
        redirect_worker_output=redirect_worker_output,
        redirect_output=redirect_output,
        resources=resources,
        plasma_directory=plasma_directory,
        huge_pages=huge_pages,
        plasma_store_socket_name=plasma_store_socket_name,
        raylet_socket_name=raylet_socket_name,
        temp_dir=temp_dir,
        _internal_config=_internal_config)


def start_ray_head(address_info=None,
                   object_manager_ports=None,
                   node_manager_ports=None,
                   node_ip_address="127.0.0.1",
                   redis_port=None,
                   redis_shard_ports=None,
                   num_workers=None,
                   num_local_schedulers=1,
                   object_store_memory=None,
                   worker_path=None,
                   cleanup=True,
                   redirect_worker_output=False,
                   redirect_output=False,
                   start_workers_from_local_scheduler=True,
                   resources=None,
                   num_redis_shards=None,
                   redis_max_clients=None,
                   redis_password=None,
                   include_webui=True,
                   plasma_directory=None,
                   huge_pages=False,
                   autoscaling_config=None,
                   plasma_store_socket_name=None,
                   raylet_socket_name=None,
                   temp_dir=None,
                   _internal_config=None):
    """Start Ray in local mode.

    Args:
        address_info (dict): A dictionary with address information for
            processes that have already been started. If provided, address_info
            will be modified to include processes that are newly started.
        object_manager_ports (list): A list of the ports to use for the object
            managers. There should be one per object manager being started on
            this node (typically just one).
        node_manager_ports (list): A list of the ports to use for the node
            managers. There should be one per node manager being started on
            this node (typically just one).
        node_ip_address (str): The IP address of this node.
        redis_port (int): The port that the primary Redis shard should listen
            to. If None, then a random port will be chosen. If the key
            "redis_address" is in address_info, then this argument will be
            ignored.
        redis_shard_ports: A list of the ports to use for the non-primary Redis
            shards.
        num_workers (int): The number of workers to start.
        num_local_schedulers (int): The total number of local schedulers
            required. This is also the total number of object stores required.
            This method will start new instances of local schedulers and object
            stores until there are at least num_local_schedulers existing
            instances of each, including ones already registered with the given
            address_info.
        object_store_memory: The amount of memory (in bytes) to start the
            object store with.
        worker_path (str): The path of the source code that will be run by the
            worker.
        cleanup (bool): If cleanup is true, then the processes started here
            will be killed by services.cleanup() when the Python process that
            called this method exits.
        redirect_worker_output: True if the stdout and stderr of worker
            processes should be redirected to files.
        redirect_output (bool): True if stdout and stderr for non-worker
            processes should be redirected to files and false otherwise.
        start_workers_from_local_scheduler (bool): If this flag is True, then
            start the initial workers from the local scheduler. Else, start
            them from Python.
        resources: A dictionary mapping resource name to the available quantity
            of that resource.
        num_redis_shards: The number of Redis shards to start in addition to
            the primary Redis shard.
        redis_max_clients: If provided, attempt to configure Redis with this
            maxclients number.
        redis_password (str): Prevents external clients without the password
            from connecting to Redis if provided.
        include_webui: True if the UI should be started and false otherwise.
        plasma_directory: A directory where the Plasma memory mapped files will
            be created.
        huge_pages: Boolean flag indicating whether to start the Object
            Store with hugetlbfs support. Requires plasma_directory.
        autoscaling_config: path to autoscaling config file.
        plasma_store_socket_name (str): If provided, it will specify the socket
            name used by the plasma store.
        raylet_socket_name (str): If provided, it will specify the socket path
            used by the raylet process.
        temp_dir (str): If provided, it will specify the root temporary
            directory for the Ray process.
        _internal_config (str): JSON configuration for overriding
            RayConfig defaults. For testing purposes ONLY.

    Returns:
        A dictionary of the address information for the processes that were
            started.
    """
    num_redis_shards = 1 if num_redis_shards is None else num_redis_shards
    return start_ray_processes(
        address_info=address_info,
        object_manager_ports=object_manager_ports,
        node_manager_ports=node_manager_ports,
        node_ip_address=node_ip_address,
        redis_port=redis_port,
        redis_shard_ports=redis_shard_ports,
        num_workers=num_workers,
        num_local_schedulers=num_local_schedulers,
        object_store_memory=object_store_memory,
        worker_path=worker_path,
        cleanup=cleanup,
        redirect_worker_output=redirect_worker_output,
        redirect_output=redirect_output,
        include_log_monitor=True,
        include_webui=include_webui,
        start_workers_from_local_scheduler=start_workers_from_local_scheduler,
        resources=resources,
        num_redis_shards=num_redis_shards,
        redis_max_clients=redis_max_clients,
        redis_password=redis_password,
        plasma_directory=plasma_directory,
        huge_pages=huge_pages,
        autoscaling_config=autoscaling_config,
        plasma_store_socket_name=plasma_store_socket_name,
        raylet_socket_name=raylet_socket_name,
        temp_dir=temp_dir,
        _internal_config=_internal_config)
