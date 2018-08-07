from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import binascii
import logging
import json
import os
import random
import resource
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import OrderedDict, namedtuple
from datetime import datetime

import psutil
import redis

import pyarrow
# Ray modules
import ray.ray_constants
import ray.global_scheduler as global_scheduler
import ray.local_scheduler
import ray.plasma

logger = logging.getLogger("ray")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stderr)
ch.setLevel(logging.INFO)
logger.addHandler(ch)

PROCESS_TYPE_MONITOR = "monitor"
PROCESS_TYPE_LOG_MONITOR = "log_monitor"
PROCESS_TYPE_WORKER = "worker"
PROCESS_TYPE_RAYLET = "raylet"
PROCESS_TYPE_LOCAL_SCHEDULER = "local_scheduler"
PROCESS_TYPE_PLASMA_MANAGER = "plasma_manager"
PROCESS_TYPE_PLASMA_STORE = "plasma_store"
PROCESS_TYPE_GLOBAL_SCHEDULER = "global_scheduler"
PROCESS_TYPE_REDIS_SERVER = "redis_server"
PROCESS_TYPE_WEB_UI = "web_ui"

# This is a dictionary tracking all of the processes of different types that
# have been started by this services module. Note that the order of the keys is
# important because it determines the order in which these processes will be
# terminated when Ray exits, and certain orders will cause errors to be logged
# to the screen.
all_processes = OrderedDict(
    [(PROCESS_TYPE_MONITOR, []), (PROCESS_TYPE_LOG_MONITOR, []),
     (PROCESS_TYPE_WORKER, []), (PROCESS_TYPE_RAYLET, []),
     (PROCESS_TYPE_LOCAL_SCHEDULER, []), (PROCESS_TYPE_PLASMA_MANAGER, []),
     (PROCESS_TYPE_PLASMA_STORE, []), (PROCESS_TYPE_GLOBAL_SCHEDULER, []),
     (PROCESS_TYPE_REDIS_SERVER, []), (PROCESS_TYPE_WEB_UI, [])], )

# True if processes are run in the valgrind profiler.
RUN_RAYLET_PROFILER = False
RUN_LOCAL_SCHEDULER_PROFILER = False
RUN_PLASMA_MANAGER_PROFILER = False
RUN_PLASMA_STORE_PROFILER = False

# Location of the redis server and module.
REDIS_EXECUTABLE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "core/src/common/thirdparty/redis/src/redis-server")
REDIS_MODULE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "core/src/common/redis_module/libray_redis_module.so")

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

# ObjectStoreAddress tuples contain all information necessary to connect to an
# object store. The fields are:
# - name: The socket name for the object store
# - manager_name: The socket name for the object store manager
# - manager_port: The Internet port that the object store manager listens on
ObjectStoreAddress = namedtuple("ObjectStoreAddress",
                                ["name", "manager_name", "manager_port"])


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


def random_name():
    return str(random.randint(0, 99999999))


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
    if any([
            RUN_RAYLET_PROFILER, RUN_LOCAL_SCHEDULER_PROFILER,
            RUN_PLASMA_MANAGER_PROFILER, RUN_PLASMA_STORE_PROFILER
    ]):
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


def all_processes_alive(exclude=[]):
    """Check if all of the processes are still alive.

    Args:
        exclude: Don't check the processes whose types are in this list.
    """
    for process_type, processes in all_processes.items():
        # Note that p.poll() returns the exit code that the process exited
        # with, so an exit code of None indicates that the process is still
        # alive.
        processes_alive = [p.poll() is None for p in processes]
        if (not all(processes_alive) and process_type not in exclude):
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

    return node_ip_address


def record_log_files_in_redis(redis_address, node_ip_address, log_files):
    """Record in Redis that a new log file has been created.

    This is used so that each log monitor can check Redis and figure out which
    log files it is reponsible for monitoring.

    Args:
        redis_address: The address of the redis server.
        node_ip_address: The IP address of the node that the log file exists
            on.
        log_files: A list of file handles for the log files. If one of the file
            handles is None, we ignore it.
    """
    for log_file in log_files:
        if log_file is not None:
            redis_ip_address, redis_port = redis_address.split(":")
            redis_client = redis.StrictRedis(
                host=redis_ip_address, port=redis_port)
            # The name of the key storing the list of log filenames for this IP
            # address.
            log_file_list_key = "LOG_FILENAMES:{}".format(node_ip_address)
            redis_client.rpush(log_file_list_key, log_file.name)


def create_redis_client(redis_address):
    """Create a Redis client.

    Args:
        The IP address and port of the Redis server.

    Returns:
        A Redis client.
    """
    redis_ip_address, redis_port = redis_address.split(":")
    # For this command to work, some other client (on the same machine
    # as Redis) must have run "CONFIG SET protected-mode no".
    return redis.StrictRedis(host=redis_ip_address, port=int(redis_port))


def wait_for_redis_to_start(redis_ip_address, redis_port, num_retries=5):
    """Wait for a Redis server to be available.

    This is accomplished by creating a Redis client and sending a random
    command to the server until the command gets through.

    Args:
        redis_ip_address (str): The IP address of the redis server.
        redis_port (int): The port of the redis server.
        num_retries (int): The number of times to try connecting with redis.
            The client will sleep for one second between attempts.

    Raises:
        Exception: An exception is raised if we could not connect with Redis.
    """
    redis_client = redis.StrictRedis(host=redis_ip_address, port=redis_port)
    # Wait for the Redis server to start.
    counter = 0
    while counter < num_retries:
        try:
            # Run some random command and see if it worked.
            logger.info(
                "Waiting for redis server at {}:{} to respond...".format(
                    redis_ip_address, redis_port))
            redis_client.client_list()
        except redis.ConnectionError as e:
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
    return (ray_version, python_version, pyarrow_version)


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
                use_raylet=False,
                redirect_output=False,
                redirect_worker_output=False,
                cleanup=True,
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
        use_raylet: True if the new raylet code path should be used. This is
            not supported yet.
        redirect_output (bool): True if output should be redirected to a file
            and false otherwise.
        redirect_worker_output (bool): True if worker output should be
            redirected to a file and false otherwise. Workers will have access
            to this value when they start up.
        cleanup (bool): True if using Ray in local mode. If cleanup is true,
            then all Redis processes started by this method will be killed by
            services.cleanup() when the Python process that imported services
            exits.
        use_credis: If True, additionally load the chain-replicated libraries
            into the redis servers.  Defaults to None, which means its value is
            set by the presence of "RAY_USE_NEW_GCS" in os.environ.

    Returns:
        A tuple of the address for the primary Redis shard and a list of
            addresses for the remaining shards.
    """
    redis_stdout_file, redis_stderr_file = new_log_files(
        "redis", redirect_output)

    if redis_shard_ports is None:
        redis_shard_ports = num_redis_shards * [None]
    elif len(redis_shard_ports) != num_redis_shards:
        raise Exception("The number of Redis shard ports does not match the "
                        "number of Redis shards.")

    if use_credis is None:
        use_credis = ("RAY_USE_NEW_GCS" in os.environ)
    if not use_credis:
        assigned_port, _ = _start_redis_instance(
            node_ip_address=node_ip_address,
            port=port,
            redis_max_clients=redis_max_clients,
            stdout_file=redis_stdout_file,
            stderr_file=redis_stderr_file,
            cleanup=cleanup)
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
            modules=[CREDIS_MASTER_MODULE, REDIS_MODULE])
    if port is not None:
        assert assigned_port == port
    port = assigned_port
    redis_address = address(node_ip_address, port)

    redis_client = redis.StrictRedis(host=node_ip_address, port=port)

    # Store whether we're using the raylet code path or not.
    redis_client.set("UseRaylet", 1 if use_raylet else 0)

    # Register the number of Redis shards in the primary shard, so that clients
    # know how many redis shards to expect under RedisShards.
    primary_redis_client = redis.StrictRedis(host=node_ip_address, port=port)
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
        redis_stdout_file, redis_stderr_file = new_log_files(
            "redis-{}".format(i), redirect_output)
        if not use_credis:
            redis_shard_port, _ = _start_redis_instance(
                node_ip_address=node_ip_address,
                port=redis_shard_ports[i],
                redis_max_clients=redis_max_clients,
                stdout_file=redis_stdout_file,
                stderr_file=redis_stderr_file,
                cleanup=cleanup)
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
            host=node_ip_address, port=redis_shard_port)
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
        command = [executable, "--port",
                   str(port), "--loglevel", "warning"] + load_module_args
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
    redis_client = redis.StrictRedis(host="127.0.0.1", port=port)
    # Wait for the Redis server to start.
    wait_for_redis_to_start("127.0.0.1", port)
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
        address(node_ip_address, port), node_ip_address,
        [stdout_file, stderr_file])
    return port, p


def start_log_monitor(redis_address,
                      node_ip_address,
                      stdout_file=None,
                      stderr_file=None,
                      cleanup=cleanup):
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
    """
    log_monitor_filepath = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "log_monitor.py")
    p = subprocess.Popen(
        [
            sys.executable, "-u", log_monitor_filepath, "--redis-address",
            redis_address, "--node-ip-address", node_ip_address
        ],
        stdout=stdout_file,
        stderr=stderr_file)
    if cleanup:
        all_processes[PROCESS_TYPE_LOG_MONITOR].append(p)
    record_log_files_in_redis(redis_address, node_ip_address,
                              [stdout_file, stderr_file])


def start_global_scheduler(redis_address,
                           node_ip_address,
                           stdout_file=None,
                           stderr_file=None,
                           cleanup=True):
    """Start a global scheduler process.

    Args:
        redis_address (str): The address of the Redis instance.
        node_ip_address: The IP address of the node that this scheduler will
            run on.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        cleanup (bool): True if using Ray in local mode. If cleanup is true,
            then this process will be killed by services.cleanup() when the
            Python process that imported services exits.
    """
    p = global_scheduler.start_global_scheduler(
        redis_address,
        node_ip_address,
        stdout_file=stdout_file,
        stderr_file=stderr_file)
    if cleanup:
        all_processes[PROCESS_TYPE_GLOBAL_SCHEDULER].append(p)
    record_log_files_in_redis(redis_address, node_ip_address,
                              [stdout_file, stderr_file])


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
    new_env = os.environ.copy()
    notebook_filepath = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "WebUI.ipynb")
    # We copy the notebook file so that the original doesn't get modified by
    # the user.
    random_ui_id = random.randint(0, 100000)
    new_notebook_filepath = "/tmp/raylogs/ray_ui{}.ipynb".format(random_ui_id)
    new_notebook_directory = os.path.dirname(new_notebook_filepath)
    shutil.copy(notebook_filepath, new_notebook_filepath)
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
    token = ray.utils.decode(binascii.hexlify(os.urandom(24)))
    command = [
        "jupyter", "notebook", "--no-browser", "--port={}".format(port),
        "--NotebookApp.iopub_data_rate_limit=10000000000",
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
        webui_url = ("http://localhost:{}/notebooks/ray_ui{}.ipynb?token={}"
                     .format(port, random_ui_id, token))
        logger.info("\n" + "=" * 70)
        logger.info("View the web UI at {}".format(webui_url))
        logger.info("=" * 70 + "\n")
        return webui_url


def check_and_update_resources(resources, use_raylet):
    """Sanity check a resource dictionary and add sensible defaults.

    Args:
        resources: A dictionary mapping resource names to resource quantities.
        use_raylet: True if we are using the raylet code path and false
            otherwise.

    Returns:
        A new resource dictionary.
    """
    if resources is None:
        resources = {}
    resources = resources.copy()
    if "CPU" not in resources:
        # By default, use the number of hardware execution threads for the
        # number of cores.
        resources["CPU"] = psutil.cpu_count()

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

        if (use_raylet and
                resource_quantity > ray.ray_constants.MAX_RESOURCE_QUANTITY):
            raise ValueError("Resource quantities must be at most {}.".format(
                ray.ray_constants.MAX_RESOURCE_QUANTITY))

    return resources


def start_local_scheduler(redis_address,
                          node_ip_address,
                          plasma_store_name,
                          plasma_manager_name,
                          worker_path,
                          plasma_address=None,
                          stdout_file=None,
                          stderr_file=None,
                          cleanup=True,
                          resources=None,
                          num_workers=0):
    """Start a local scheduler process.

    Args:
        redis_address (str): The address of the Redis instance.
        node_ip_address (str): The IP address of the node that this local
            scheduler is running on.
        plasma_store_name (str): The name of the plasma store socket to connect
            to.
        plasma_manager_name (str): The name of the plasma manager socket to
            connect to.
        worker_path (str): The path of the script to use when the local
            scheduler starts up new workers.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        cleanup (bool): True if using Ray in local mode. If cleanup is true,
            then this process will be killed by serices.cleanup() when the
            Python process that imported services exits.
        resources: A dictionary mapping the name of a resource to the available
            quantity of that resource.
        num_workers (int): The number of workers that the local scheduler
            should start.

    Return:
        The name of the local scheduler socket.
    """
    resources = check_and_update_resources(resources, False)

    logger.info("Starting local scheduler with the following resources: {}."
                .format(resources))
    local_scheduler_name, p = ray.local_scheduler.start_local_scheduler(
        plasma_store_name,
        plasma_manager_name,
        worker_path=worker_path,
        node_ip_address=node_ip_address,
        redis_address=redis_address,
        plasma_address=plasma_address,
        use_profiler=RUN_LOCAL_SCHEDULER_PROFILER,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        static_resources=resources,
        num_workers=num_workers)
    if cleanup:
        all_processes[PROCESS_TYPE_LOCAL_SCHEDULER].append(p)
    record_log_files_in_redis(redis_address, node_ip_address,
                              [stdout_file, stderr_file])
    return local_scheduler_name


def start_raylet(redis_address,
                 node_ip_address,
                 plasma_store_name,
                 worker_path,
                 resources=None,
                 num_workers=0,
                 use_valgrind=False,
                 use_profiler=False,
                 stdout_file=None,
                 stderr_file=None,
                 cleanup=True):
    """Start a raylet, which is a combined local scheduler and object manager.

    Args:
        redis_address (str): The address of the Redis instance.
        node_ip_address (str): The IP address of the node that this local
            scheduler is running on.
        plasma_store_name (str): The name of the plasma store socket to connect
            to.
        worker_path (str): The path of the script to use when the local
            scheduler starts up new workers.
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

    Returns:
        The raylet socket name.
    """
    if use_valgrind and use_profiler:
        raise Exception("Cannot use valgrind and profiler at the same time.")

    static_resources = check_and_update_resources(resources, True)

    # Format the resource argument in a form like 'CPU,1.0,GPU,0,Custom,3'.
    resource_argument = ",".join([
        "{},{}".format(resource_name, resource_value)
        for resource_name, resource_value in zip(static_resources.keys(),
                                                 static_resources.values())
    ])

    gcs_ip_address, gcs_port = redis_address.split(":")
    raylet_name = "/tmp/raylet{}".format(random_name())

    # Create the command that the Raylet will use to start workers.
    start_worker_command = ("{} {} "
                            "--node-ip-address={} "
                            "--object-store-name={} "
                            "--raylet-name={} "
                            "--redis-address={}".format(
                                sys.executable, worker_path, node_ip_address,
                                plasma_store_name, raylet_name, redis_address))

    command = [
        RAYLET_EXECUTABLE,
        raylet_name,
        plasma_store_name,
        node_ip_address,
        gcs_ip_address,
        gcs_port,
        str(num_workers),
        start_worker_command,
        resource_argument,
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
    record_log_files_in_redis(redis_address, node_ip_address,
                              [stdout_file, stderr_file])

    return raylet_name


def start_objstore(node_ip_address,
                   redis_address,
                   object_manager_port=None,
                   store_stdout_file=None,
                   store_stderr_file=None,
                   manager_stdout_file=None,
                   manager_stderr_file=None,
                   objstore_memory=None,
                   cleanup=True,
                   plasma_directory=None,
                   huge_pages=False,
                   use_raylet=False):
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
        manager_stdout_file: A file handle opened for writing to redirect
            stdout to. If no redirection should happen, then this should be
            None.
        manager_stderr_file: A file handle opened for writing to redirect
            stderr to. If no redirection should happen, then this should be
            None.
        objstore_memory: The amount of memory (in bytes) to start the object
            store with.
        cleanup (bool): True if using Ray in local mode. If cleanup is true,
            then this process will be killed by serices.cleanup() when the
            Python process that imported services exits.
        plasma_directory: A directory where the Plasma memory mapped files will
            be created.
        huge_pages: Boolean flag indicating whether to start the Object
            Store with hugetlbfs support. Requires plasma_directory.
        use_raylet: True if the new raylet code path should be used. This is
            not supported yet.

    Return:
        A tuple of the Plasma store socket name, the Plasma manager socket
            name, and the plasma manager port.
    """
    if objstore_memory is None:
        # Compute a fraction of the system memory for the Plasma store to use.
        system_memory = psutil.virtual_memory().total
        if sys.platform == "linux" or sys.platform == "linux2":
            # On linux we use /dev/shm, its size is half the size of the
            # physical memory. To not overflow it, we set the plasma memory
            # limit to 0.4 times the size of the physical memory.
            objstore_memory = int(system_memory * 0.4)
            # Compare the requested memory size to the memory available in
            # /dev/shm.
            shm_fd = os.open("/dev/shm", os.O_RDONLY)
            try:
                shm_fs_stats = os.fstatvfs(shm_fd)
                # The value shm_fs_stats.f_bsize is the block size and the
                # value shm_fs_stats.f_bavail is the number of available
                # blocks.
                shm_avail = shm_fs_stats.f_bsize * shm_fs_stats.f_bavail
                if objstore_memory > shm_avail:
                    logger.warning(
                        "Warning: Reducing object store memory because "
                        "/dev/shm has only {} bytes available. You may be "
                        "able to free up space by deleting files in "
                        "/dev/shm. If you are inside a Docker container, "
                        "you may need to pass an argument with the flag "
                        "'--shm-size' to 'docker run'.".format(shm_avail))
                    objstore_memory = int(shm_avail * 0.8)
            finally:
                os.close(shm_fd)
        else:
            objstore_memory = int(system_memory * 0.8)
    # Start the Plasma store.
    plasma_store_name, p1 = ray.plasma.start_plasma_store(
        plasma_store_memory=objstore_memory,
        use_profiler=RUN_PLASMA_STORE_PROFILER,
        stdout_file=store_stdout_file,
        stderr_file=store_stderr_file,
        plasma_directory=plasma_directory,
        huge_pages=huge_pages)
    # Start the plasma manager.
    if not use_raylet:
        if object_manager_port is not None:
            (plasma_manager_name, p2,
             plasma_manager_port) = ray.plasma.start_plasma_manager(
                 plasma_store_name,
                 redis_address,
                 plasma_manager_port=object_manager_port,
                 node_ip_address=node_ip_address,
                 num_retries=1,
                 run_profiler=RUN_PLASMA_MANAGER_PROFILER,
                 stdout_file=manager_stdout_file,
                 stderr_file=manager_stderr_file)
            assert plasma_manager_port == object_manager_port
        else:
            (plasma_manager_name, p2,
             plasma_manager_port) = ray.plasma.start_plasma_manager(
                 plasma_store_name,
                 redis_address,
                 node_ip_address=node_ip_address,
                 run_profiler=RUN_PLASMA_MANAGER_PROFILER,
                 stdout_file=manager_stdout_file,
                 stderr_file=manager_stderr_file)
    else:
        plasma_manager_port = None
        plasma_manager_name = None

    if cleanup:
        all_processes[PROCESS_TYPE_PLASMA_STORE].append(p1)
    record_log_files_in_redis(redis_address, node_ip_address,
                              [store_stdout_file, store_stderr_file])
    if not use_raylet:
        if cleanup:
            all_processes[PROCESS_TYPE_PLASMA_MANAGER].append(p2)
        record_log_files_in_redis(redis_address, node_ip_address,
                                  [manager_stdout_file, manager_stderr_file])

    return ObjectStoreAddress(plasma_store_name, plasma_manager_name,
                              plasma_manager_port)


def start_worker(node_ip_address,
                 object_store_name,
                 object_store_manager_name,
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
        object_store_manager_name (str): The name of the object store manager.
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
        "--object-store-manager-name=" + object_store_manager_name,
        "--local-scheduler-name=" + local_scheduler_name,
        "--redis-address=" + str(redis_address)
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
                  autoscaling_config=None):
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
    """
    monitor_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "monitor.py")
    command = [
        sys.executable, "-u", monitor_path,
        "--redis-address=" + str(redis_address)
    ]
    if autoscaling_config:
        command.append("--autoscaling-config=" + str(autoscaling_config))
    p = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
    if cleanup:
        all_processes[PROCESS_TYPE_MONITOR].append(p)
    record_log_files_in_redis(redis_address, node_ip_address,
                              [stdout_file, stderr_file])


def start_raylet_monitor(redis_address,
                         stdout_file=None,
                         stderr_file=None,
                         cleanup=True):
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
    """
    gcs_ip_address, gcs_port = redis_address.split(":")
    command = [RAYLET_MONITOR_EXECUTABLE, gcs_ip_address, gcs_port]
    p = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
    if cleanup:
        all_processes[PROCESS_TYPE_MONITOR].append(p)


def start_ray_processes(address_info=None,
                        node_ip_address="127.0.0.1",
                        redis_port=None,
                        redis_shard_ports=None,
                        num_workers=None,
                        num_local_schedulers=1,
                        object_store_memory=None,
                        num_redis_shards=1,
                        redis_max_clients=None,
                        worker_path=None,
                        cleanup=True,
                        redirect_worker_output=False,
                        redirect_output=False,
                        include_global_scheduler=False,
                        include_log_monitor=False,
                        include_webui=False,
                        start_workers_from_local_scheduler=True,
                        resources=None,
                        plasma_directory=None,
                        huge_pages=False,
                        autoscaling_config=None,
                        use_raylet=False):
    """Helper method to start Ray processes.

    Args:
        address_info (dict): A dictionary with address information for
            processes that have already been started. If provided, address_info
            will be modified to include processes that are newly started.
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
        worker_path (str): The path of the source code that will be run by the
            worker.
        cleanup (bool): If cleanup is true, then the processes started here
            will be killed by services.cleanup() when the Python process that
            called this method exits.
        redirect_worker_output: True if the stdout and stderr of worker
            processes should be redirected to files.
        redirect_output (bool): True if stdout and stderr for non-worker
            processes should be redirected to files and false otherwise.
        include_global_scheduler (bool): If include_global_scheduler is True,
            then start a global scheduler process.
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
        use_raylet: True if the new raylet code path should be used. This is
            not supported yet.

    Returns:
        A dictionary of the address information for the processes that were
            started.
    """
    logger.info(
        "Process STDOUT and STDERR is being redirected to /tmp/raylogs/.")

    if resources is None:
        resources = {}
    if not isinstance(resources, list):
        resources = num_local_schedulers * [resources]

    if num_workers is not None:
        workers_per_local_scheduler = num_local_schedulers * [num_workers]
    else:
        workers_per_local_scheduler = []
        for resource_dict in resources:
            cpus = resource_dict.get("CPU")
            workers_per_local_scheduler.append(cpus if cpus is not None else
                                               psutil.cpu_count())

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
            use_raylet=use_raylet,
            redirect_output=True,
            redirect_worker_output=redirect_worker_output,
            cleanup=cleanup)
        address_info["redis_address"] = redis_address
        time.sleep(0.1)

        # Start monitoring the processes.
        monitor_stdout_file, monitor_stderr_file = new_log_files(
            "monitor", redirect_output)
        start_monitor(
            redis_address,
            node_ip_address,
            stdout_file=monitor_stdout_file,
            stderr_file=monitor_stderr_file,
            cleanup=cleanup,
            autoscaling_config=autoscaling_config)
        if use_raylet:
            start_raylet_monitor(
                redis_address,
                stdout_file=monitor_stdout_file,
                stderr_file=monitor_stderr_file,
                cleanup=cleanup)
    if redis_shards == []:
        # Get redis shards from primary redis instance.
        redis_ip_address, redis_port = redis_address.split(":")
        redis_client = redis.StrictRedis(
            host=redis_ip_address, port=redis_port)
        redis_shards = redis_client.lrange("RedisShards", start=0, end=-1)
        redis_shards = [ray.utils.decode(shard) for shard in redis_shards]
        address_info["redis_shards"] = redis_shards

    # Start the log monitor, if necessary.
    if include_log_monitor:
        log_monitor_stdout_file, log_monitor_stderr_file = new_log_files(
            "log_monitor", redirect_output=True)
        start_log_monitor(
            redis_address,
            node_ip_address,
            stdout_file=log_monitor_stdout_file,
            stderr_file=log_monitor_stderr_file,
            cleanup=cleanup)

    # Start the global scheduler, if necessary.
    if include_global_scheduler and not use_raylet:
        global_scheduler_stdout_file, global_scheduler_stderr_file = (
            new_log_files("global_scheduler", redirect_output))
        start_global_scheduler(
            redis_address,
            node_ip_address,
            stdout_file=global_scheduler_stdout_file,
            stderr_file=global_scheduler_stderr_file,
            cleanup=cleanup)

    # Initialize with existing services.
    if "object_store_addresses" not in address_info:
        address_info["object_store_addresses"] = []
    object_store_addresses = address_info["object_store_addresses"]
    if "local_scheduler_socket_names" not in address_info:
        address_info["local_scheduler_socket_names"] = []
    local_scheduler_socket_names = address_info["local_scheduler_socket_names"]
    if "raylet_socket_names" not in address_info:
        address_info["raylet_socket_names"] = []
    raylet_socket_names = address_info["raylet_socket_names"]

    # Get the ports to use for the object managers if any are provided.
    object_manager_ports = (address_info["object_manager_ports"] if
                            "object_manager_ports" in address_info else None)
    if not isinstance(object_manager_ports, list):
        object_manager_ports = num_local_schedulers * [object_manager_ports]
    assert len(object_manager_ports) == num_local_schedulers

    # Start any object stores that do not yet exist.
    for i in range(num_local_schedulers - len(object_store_addresses)):
        # Start Plasma.
        plasma_store_stdout_file, plasma_store_stderr_file = new_log_files(
            "plasma_store_{}".format(i), redirect_output)
        plasma_manager_stdout_file, plasma_manager_stderr_file = new_log_files(
            "plasma_manager_{}".format(i), redirect_output)
        object_store_address = start_objstore(
            node_ip_address,
            redis_address,
            object_manager_port=object_manager_ports[i],
            store_stdout_file=plasma_store_stdout_file,
            store_stderr_file=plasma_store_stderr_file,
            manager_stdout_file=plasma_manager_stdout_file,
            manager_stderr_file=plasma_manager_stderr_file,
            objstore_memory=object_store_memory,
            cleanup=cleanup,
            plasma_directory=plasma_directory,
            huge_pages=huge_pages,
            use_raylet=use_raylet)
        object_store_addresses.append(object_store_address)
        time.sleep(0.1)

    if not use_raylet:
        # Start any local schedulers that do not yet exist.
        for i in range(
                len(local_scheduler_socket_names), num_local_schedulers):
            # Connect the local scheduler to the object store at the same
            # index.
            object_store_address = object_store_addresses[i]
            plasma_address = "{}:{}".format(node_ip_address,
                                            object_store_address.manager_port)
            # Determine how many workers this local scheduler should start.
            if start_workers_from_local_scheduler:
                num_local_scheduler_workers = workers_per_local_scheduler[i]
                workers_per_local_scheduler[i] = 0
            else:
                # If we're starting the workers from Python, the local
                # scheduler should not start any workers.
                num_local_scheduler_workers = 0
            # Start the local scheduler. Note that if we do not wish to
            # redirect the worker output, then we cannot redirect the local
            # scheduler output.
            local_scheduler_stdout_file, local_scheduler_stderr_file = (
                new_log_files(
                    "local_scheduler_{}".format(i),
                    redirect_output=redirect_worker_output))
            local_scheduler_name = start_local_scheduler(
                redis_address,
                node_ip_address,
                object_store_address.name,
                object_store_address.manager_name,
                worker_path,
                plasma_address=plasma_address,
                stdout_file=local_scheduler_stdout_file,
                stderr_file=local_scheduler_stderr_file,
                cleanup=cleanup,
                resources=resources[i],
                num_workers=num_local_scheduler_workers)
            local_scheduler_socket_names.append(local_scheduler_name)

        # Make sure that we have exactly num_local_schedulers instances of
        # object stores and local schedulers.
        assert len(object_store_addresses) == num_local_schedulers
        assert len(local_scheduler_socket_names) == num_local_schedulers

    else:
        # Start any raylets that do not exist yet.
        for i in range(len(raylet_socket_names), num_local_schedulers):
            raylet_stdout_file, raylet_stderr_file = new_log_files(
                "raylet_{}".format(i), redirect_output=redirect_worker_output)
            address_info["raylet_socket_names"].append(
                start_raylet(
                    redis_address,
                    node_ip_address,
                    object_store_addresses[i].name,
                    worker_path,
                    resources=resources[i],
                    num_workers=workers_per_local_scheduler[i],
                    stdout_file=raylet_stdout_file,
                    stderr_file=raylet_stderr_file,
                    cleanup=cleanup))

    if not use_raylet:
        # Start any workers that the local scheduler has not already started.
        for i, num_local_scheduler_workers in enumerate(
                workers_per_local_scheduler):
            object_store_address = object_store_addresses[i]
            local_scheduler_name = local_scheduler_socket_names[i]
            for j in range(num_local_scheduler_workers):
                worker_stdout_file, worker_stderr_file = new_log_files(
                    "worker_{}_{}".format(i, j), redirect_output)
                start_worker(
                    node_ip_address,
                    object_store_address.name,
                    object_store_address.manager_name,
                    local_scheduler_name,
                    redis_address,
                    worker_path,
                    stdout_file=worker_stdout_file,
                    stderr_file=worker_stderr_file,
                    cleanup=cleanup)
                workers_per_local_scheduler[i] -= 1

        # Make sure that we've started all the workers.
        assert (sum(workers_per_local_scheduler) == 0)

    # Try to start the web UI.
    if include_webui:
        ui_stdout_file, ui_stderr_file = new_log_files(
            "webui", redirect_output=True)
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
                   num_workers=0,
                   num_local_schedulers=1,
                   object_store_memory=None,
                   worker_path=None,
                   cleanup=True,
                   redirect_worker_output=False,
                   redirect_output=False,
                   resources=None,
                   plasma_directory=None,
                   huge_pages=False,
                   use_raylet=False):
    """Start the Ray processes for a single node.

    This assumes that the Ray processes on some master node have already been
    started.

    Args:
        node_ip_address (str): The IP address of this node.
        redis_address (str): The address of the Redis server.
        object_manager_ports (list): A list of the ports to use for the object
            managers. There should be one per object manager being started on
            this node (typically just one).
        num_workers (int): The number of workers to start.
        num_local_schedulers (int): The number of local schedulers to start.
            This is also the number of plasma stores and plasma managers to
            start.
        object_store_memory (int): The maximum amount of memory (in bytes) to
            let the plasma store use.
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
        use_raylet: True if the new raylet code path should be used. This is
            not supported yet.

    Returns:
        A dictionary of the address information for the processes that were
            started.
    """
    address_info = {
        "redis_address": redis_address,
        "object_manager_ports": object_manager_ports
    }
    return start_ray_processes(
        address_info=address_info,
        node_ip_address=node_ip_address,
        num_workers=num_workers,
        num_local_schedulers=num_local_schedulers,
        object_store_memory=object_store_memory,
        worker_path=worker_path,
        include_log_monitor=True,
        cleanup=cleanup,
        redirect_worker_output=redirect_worker_output,
        redirect_output=redirect_output,
        resources=resources,
        plasma_directory=plasma_directory,
        huge_pages=huge_pages,
        use_raylet=use_raylet)


def start_ray_head(address_info=None,
                   node_ip_address="127.0.0.1",
                   redis_port=None,
                   redis_shard_ports=None,
                   num_workers=0,
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
                   include_webui=True,
                   plasma_directory=None,
                   huge_pages=False,
                   autoscaling_config=None,
                   use_raylet=False):
    """Start Ray in local mode.

    Args:
        address_info (dict): A dictionary with address information for
            processes that have already been started. If provided, address_info
            will be modified to include processes that are newly started.
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
        include_webui: True if the UI should be started and false otherwise.
        plasma_directory: A directory where the Plasma memory mapped files will
            be created.
        huge_pages: Boolean flag indicating whether to start the Object
            Store with hugetlbfs support. Requires plasma_directory.
        autoscaling_config: path to autoscaling config file.
        use_raylet: True if the new raylet code path should be used. This is
            not supported yet.

    Returns:
        A dictionary of the address information for the processes that were
            started.
    """
    num_redis_shards = 1 if num_redis_shards is None else num_redis_shards
    return start_ray_processes(
        address_info=address_info,
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
        include_global_scheduler=True,
        include_log_monitor=True,
        include_webui=include_webui,
        start_workers_from_local_scheduler=start_workers_from_local_scheduler,
        resources=resources,
        num_redis_shards=num_redis_shards,
        redis_max_clients=redis_max_clients,
        plasma_directory=plasma_directory,
        huge_pages=huge_pages,
        autoscaling_config=autoscaling_config,
        use_raylet=use_raylet)


def try_to_create_directory(directory_path):
    """Attempt to create a directory that is globally readable/writable.

    Args:
        directory_path: The path of the directory to create.
    """
    if not os.path.exists(directory_path):
        try:
            os.makedirs(directory_path)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise e
            logger.warning(
                "Attempted to create '{}', but the directory already "
                "exists.".format(directory_path))
        # Change the log directory permissions so others can use it. This is
        # important when multiple people are using the same machine.
        os.chmod(directory_path, 0o0777)


def new_log_files(name, redirect_output):
    """Generate partially randomized filenames for log files.

    Args:
        name (str): descriptive string for this log file.
        redirect_output (bool): True if files should be generated for logging
            stdout and stderr and false if stdout and stderr should not be
            redirected.

    Returns:
        If redirect_output is true, this will return a tuple of two
            filehandles. The first is for redirecting stdout and the second is
            for redirecting stderr. If redirect_output is false, this will
            return a tuple of two None objects.
    """
    if not redirect_output:
        return None, None

    # Create a directory to be used for process log files.
    logs_dir = "/tmp/raylogs"
    try_to_create_directory(logs_dir)
    # Create another directory that will be used by some of the RL algorithms.
    try_to_create_directory("/tmp/ray")

    log_id = random.randint(0, 10000)
    date_str = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
    log_stdout = "{}/{}-{}-{:05d}.out".format(logs_dir, name, date_str, log_id)
    log_stderr = "{}/{}-{}-{:05d}.err".format(logs_dir, name, date_str, log_id)
    # Line-buffer the output (mode 1)
    log_stdout_file = open(log_stdout, "a", buffering=1)
    log_stderr_file = open(log_stderr, "a", buffering=1)
    return log_stdout_file, log_stderr_file
