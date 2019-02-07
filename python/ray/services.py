from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import binascii
import collections
import json
import logging
import multiprocessing
import os
import random
import resource
import socket
import subprocess
import sys
import time
import redis

import pyarrow
# Ray modules
import ray
import ray.ray_constants as ray_constants

from ray.tempfile_services import (
    get_gdb_init_path,
    get_ipython_notebook_path,
    get_logs_dir_path,
    get_temp_root,
    new_redis_log_file,
)

# True if processes are run in the valgrind profiler.
RUN_RAYLET_PROFILER = False
RUN_PLASMA_STORE_PROFILER = False

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

# Location of the raylet executables.
RAYLET_MONITOR_EXECUTABLE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "core/src/ray/raylet/raylet_monitor")
RAYLET_EXECUTABLE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "core/src/ray/raylet/raylet")

DEFAULT_JAVA_WORKER_OPTIONS = "-classpath {}".format(
    os.path.join(
        os.path.abspath(os.path.dirname(__file__)), "../../../build/java/*"))

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)

ProcessInfo = collections.namedtuple("ProcessInfo", [
    "process", "stdout_file", "stderr_file", "use_valgrind", "use_gdb",
    "use_valgrind_profiler", "use_perftools_profiler", "use_tmux"
])


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


def include_java_from_redis(redis_client):
    """This is used for query include_java bool from redis.

    Args:
        redis_client (StrictRedis): The redis client to GCS.

    Returns:
        True if this cluster backend enables Java worker.
    """
    return redis_client.get("INCLUDE_JAVA") == b"1"


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


def start_ray_process(command,
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
    # Detect which flags are set through environment variables.
    valgrind_env_var = "RAY_{}_VALGRIND".format(process_type.upper())
    if os.environ.get(valgrind_env_var) == "1":
        logger.info("Detected environment variable '%s'.", valgrind_env_var)
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
        [use_gdb, use_valgrind, use_valgrind_profiler, use_perftools_profiler
         ]) > 1:
        raise ValueError(
            "At most one of the 'use_gdb', 'use_valgrind', "
            "'use_valgrind_profiler', and 'use_perftools_profiler' flags can "
            "be used at a time.")
    if env_updates is None:
        env_updates = {}
    if not isinstance(env_updates, dict):
        raise ValueError("The 'env_updates' argument must be a dictionary.")

    modified_env = os.environ.copy()
    modified_env.update(env_updates)

    if use_gdb:
        if not use_tmux:
            raise ValueError(
                "If 'use_gdb' is true, then 'use_tmux' must be true as well.")
        gdb_init_path = get_gdb_init_path(process_type)
        ray_process_path = command[0]
        ray_process_args = command[1:]
        run_args = " ".join(["'{}'".format(arg) for arg in ray_process_args])
        with open(gdb_init_path, "w") as gdb_init_file:
            gdb_init_file.write("run {}".format(run_args))
        command = ["gdb", ray_process_path, "-x", gdb_init_path]

    if use_valgrind:
        command = [
            "valgrind", "--track-origins=yes", "--leak-check=full",
            "--show-leak-kinds=all", "--leak-check-heuristics=stdstring",
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
        command = ["tmux", "new-session", "-d", "{}".format(" ".join(command))]

    process = subprocess.Popen(
        command,
        env=modified_env,
        cwd=cwd,
        stdout=stdout_file,
        stderr=stderr_file)

    return ProcessInfo(
        process=process,
        stdout_file=stdout_file.name if stdout_file is not None else None,
        stderr_file=stderr_file.name if stderr_file is not None else None,
        use_valgrind=use_valgrind,
        use_gdb=use_gdb,
        use_valgrind_profiler=use_valgrind_profiler,
        use_perftools_profiler=use_perftools_profiler,
        use_tmux=use_tmux)


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
                password=None,
                use_credis=None,
                redis_max_memory=None,
                include_java=False):
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
        password (str): Prevents external clients without the password
            from connecting to Redis if provided.
        use_credis: If True, additionally load the chain-replicated libraries
            into the redis servers.  Defaults to None, which means its value is
            set by the presence of "RAY_USE_NEW_GCS" in os.environ.
        redis_max_memory: The max amount of memory (in bytes) to allow each
            redis shard to use. Once the limit is exceeded, redis will start
            LRU eviction of entries. This only applies to the sharded redis
            tables (task, object, and profile tables). By default, this is
            capped at 10GB but can be set higher.
        include_java (bool): If True, the raylet backend can also support
            Java worker.

    Returns:
        A tuple of the address for the primary Redis shard, a list of
            addresses for the remaining shards, and the processes that were
            started.
    """
    redis_stdout_file, redis_stderr_file = new_redis_log_file(redirect_output)

    if redis_shard_ports is None:
        redis_shard_ports = num_redis_shards * [None]
    elif len(redis_shard_ports) != num_redis_shards:
        raise Exception("The number of Redis shard ports does not match the "
                        "number of Redis shards.")

    processes = []

    if use_credis is None:
        use_credis = ("RAY_USE_NEW_GCS" in os.environ)
    if use_credis:
        if password is not None:
            # TODO(pschafhalter) remove this once credis supports
            # authenticating Redis ports
            raise Exception("Setting the `redis_password` argument is not "
                            "supported in credis. To run Ray with "
                            "password-protected Redis ports, ensure that "
                            "the environment variable `RAY_USE_NEW_GCS=off`.")
        assert num_redis_shards == 1, (
            "For now, RAY_USE_NEW_GCS supports 1 shard, and credis "
            "supports 1-node chain for that shard only.")

    if use_credis:
        redis_executable = CREDIS_EXECUTABLE
        # TODO(suquark): We need credis here because some symbols need to be
        # imported from credis dynamically through dlopen when Ray is built
        # with RAY_USE_NEW_GCS=on. We should remove them later for the primary
        # shard.
        # See src/ray/gcs/redis_module/ray_redis_module.cc
        redis_modules = [CREDIS_MASTER_MODULE, REDIS_MODULE]
    else:
        redis_executable = REDIS_EXECUTABLE
        redis_modules = [REDIS_MODULE]

    # Start the primary Redis shard.
    port, p = _start_redis_instance(
        redis_executable,
        modules=redis_modules,
        port=port,
        password=password,
        redis_max_clients=redis_max_clients,
        # Below we use None to indicate no limit on the memory of the
        # primary Redis shard.
        redis_max_memory=None,
        stdout_file=redis_stdout_file,
        stderr_file=redis_stderr_file)
    processes.append(p)
    redis_address = address(node_ip_address, port)

    # Record the log files in Redis.
    record_log_files_in_redis(
        redis_address,
        node_ip_address, [redis_stdout_file, redis_stderr_file],
        password=password)

    # Register the number of Redis shards in the primary shard, so that clients
    # know how many redis shards to expect under RedisShards.
    primary_redis_client = redis.StrictRedis(
        host=node_ip_address, port=port, password=password)
    primary_redis_client.set("NumRedisShards", str(num_redis_shards))

    # Put the redirect_worker_output bool in the Redis shard so that workers
    # can access it and know whether or not to redirect their output.
    primary_redis_client.set("RedirectOutput", 1
                             if redirect_worker_output else 0)

    # put the include_java bool to primary redis-server, so that other nodes
    # can access it and know whether or not to enable cross-languages.
    primary_redis_client.set("INCLUDE_JAVA", 1 if include_java else 0)

    # Store version information in the primary Redis shard.
    _put_version_info_in_redis(primary_redis_client)

    # Cap the memory of the other redis shards if no limit is provided.
    redis_max_memory = (redis_max_memory if redis_max_memory is not None else
                        ray_constants.DEFAULT_REDIS_MAX_MEMORY_BYTES)
    if redis_max_memory < ray_constants.REDIS_MINIMUM_MEMORY_BYTES:
        raise ValueError("Attempting to cap Redis memory usage at {} bytes, "
                         "but the minimum allowed is {} bytes.".format(
                             redis_max_memory,
                             ray_constants.REDIS_MINIMUM_MEMORY_BYTES))

    # Start other Redis shards. Each Redis shard logs to a separate file,
    # prefixed by "redis-<shard number>".
    redis_shards = []
    for i in range(num_redis_shards):
        redis_stdout_file, redis_stderr_file = new_redis_log_file(
            redirect_output, shard_number=i)

        if use_credis:
            redis_executable = CREDIS_EXECUTABLE
            # It is important to load the credis module BEFORE the ray module,
            # as the latter contains an extern declaration that the former
            # supplies.
            redis_modules = [CREDIS_MEMBER_MODULE, REDIS_MODULE]
        else:
            redis_executable = REDIS_EXECUTABLE
            redis_modules = [REDIS_MODULE]

        redis_shard_port, p = _start_redis_instance(
            redis_executable,
            modules=redis_modules,
            port=redis_shard_ports[i],
            password=password,
            redis_max_clients=redis_max_clients,
            redis_max_memory=redis_max_memory,
            stdout_file=redis_stdout_file,
            stderr_file=redis_stderr_file)
        processes.append(p)

        shard_address = address(node_ip_address, redis_shard_port)
        redis_shards.append(shard_address)
        # Store redis shard information in the primary redis shard.
        primary_redis_client.rpush("RedisShards", shard_address)

        record_log_files_in_redis(
            redis_address,
            node_ip_address, [redis_stdout_file, redis_stderr_file],
            password=password)

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
        # [CREDIS_MEMBER_MODULE, REDIS_MODULE], and they will be initialized by
        # execute_command("MEMBER.CONNECT_TO_MASTER", node_ip_address, port)
        #
        # Currently we have num_redis_shards == 1, so only one chain will be
        # created, and the chain only contains master.

        # TODO(suquark): Currently, this is not correct because we are
        # using the master replica as the primary shard. This should be
        # fixed later. I had tried to fix it but failed because of heartbeat
        # issues.
        primary_client = redis.StrictRedis(
            host=node_ip_address, port=port, password=password)
        shard_client = redis.StrictRedis(
            host=node_ip_address, port=redis_shard_port, password=password)
        primary_client.execute_command("MASTER.ADD", node_ip_address,
                                       redis_shard_port)
        shard_client.execute_command("MEMBER.CONNECT_TO_MASTER",
                                     node_ip_address, port)

    return redis_address, redis_shards, processes


def _start_redis_instance(executable,
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
        process_info = start_ray_process(
            command,
            ray_constants.PROCESS_TYPE_REDIS_SERVER,
            stdout_file=stdout_file,
            stderr_file=stderr_file)
        time.sleep(0.1)
        # Check if Redis successfully started (or at least if it the executable
        # did not exit within 0.1 seconds).
        if process_info.process.poll() is None:
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
    return port, process_info


def start_log_monitor(redis_address,
                      node_ip_address,
                      stdout_file=None,
                      stderr_file=None,
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
        redis_password (str): The password of the redis server.

    Returns:
        ProcessInfo for the process that was started.
    """
    log_monitor_filepath = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "log_monitor.py")
    command = [
        sys.executable, "-u", log_monitor_filepath, "--redis-address",
        redis_address, "--node-ip-address", node_ip_address
    ]
    if redis_password:
        command += ["--redis-password", redis_password]
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_LOG_MONITOR,
        stdout_file=stdout_file,
        stderr_file=stderr_file)
    record_log_files_in_redis(
        redis_address,
        node_ip_address, [stdout_file, stderr_file],
        password=redis_password)
    return process_info


def start_ui(redis_address, stdout_file=None, stderr_file=None):
    """Start a UI process.

    Args:
        redis_address: The address of the primary Redis shard.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.

    Returns:
        A tuple of the web UI url and ProcessInfo for the process that was
            started.
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

    notebook_name = get_ipython_notebook_path()
    new_notebook_directory = os.path.dirname(notebook_name)
    # We generate the token used for authentication ourselves to avoid
    # querying the jupyter server.
    token = ray.utils.decode(binascii.hexlify(os.urandom(24)))
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
        process_info = start_ray_process(
            command,
            ray_constants.PROCESS_TYPE_WEB_UI,
            env_updates={"REDIS_ADDRESS": redis_address},
            cwd=new_notebook_directory,
            stdout_file=stdout_file,
            stderr_file=stderr_file)
    except Exception:
        logger.warning("Failed to start the UI, you may need to run "
                       "'pip install jupyter'.")
    else:
        webui_url = ("http://localhost:{}/notebooks/{}?token={}".format(
            port, os.path.basename(notebook_name), token))
        print("\n" + "=" * 70)
        print("View the web UI at {}".format(webui_url))
        print("=" * 70 + "\n")
        return webui_url, process_info
    return None, None


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

        if resource_quantity > ray_constants.MAX_RESOURCE_QUANTITY:
            raise ValueError("Resource quantities must be at most {}.".format(
                ray_constants.MAX_RESOURCE_QUANTITY))

    return resources


def start_raylet(redis_address,
                 node_ip_address,
                 raylet_name,
                 plasma_store_name,
                 worker_path,
                 num_cpus=None,
                 num_gpus=None,
                 resources=None,
                 object_manager_port=None,
                 node_manager_port=None,
                 redis_password=None,
                 use_valgrind=False,
                 use_profiler=False,
                 stdout_file=None,
                 stderr_file=None,
                 config=None,
                 include_java=False,
                 java_worker_options=None):
    """Start a raylet, which is a combined local scheduler and object manager.

    Args:
        redis_address (str): The address of the primary Redis server.
        node_ip_address (str): The IP address of this node.
        raylet_name (str): The name of the raylet socket to create.
        plasma_store_name (str): The name of the plasma store socket to connect
             to.
        worker_path (str): The path of the Python file that new worker
            processes will execute.
        num_cpus: The CPUs allocated for this raylet.
        num_gpus: The GPUs allocated for this raylet.
        resources: The custom resources allocated for this raylet.
        object_manager_port: The port to use for the object manager. If this is
            None, then the object manager will choose its own port.
        node_manager_port: The port to use for the node manager. If this is
            None, then the node manager will choose its own port.
        redis_password: The password to use when connecting to Redis.
        use_valgrind (bool): True if the raylet should be started inside
            of valgrind. If this is True, use_profiler must be False.
        use_profiler (bool): True if the raylet should be started inside
            a profiler. If this is True, use_valgrind must be False.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        config (dict|None): Optional Raylet configuration that will
            override defaults in RayConfig.
        include_java (bool): If True, the raylet backend can also support
            Java worker.
        java_worker_options (str): The command options for Java worker.
    Returns:
        ProcessInfo for the process that was started.
    """
    config = config or {}
    config_str = ",".join(["{},{}".format(*kv) for kv in config.items()])

    if use_valgrind and use_profiler:
        raise Exception("Cannot use valgrind and profiler at the same time.")

    num_initial_workers = (num_cpus if num_cpus is not None else
                           multiprocessing.cpu_count())

    static_resources = check_and_update_resources(num_cpus, num_gpus,
                                                  resources)

    # Limit the number of workers that can be started in parallel by the
    # raylet. However, make sure it is at least 1.
    maximum_startup_concurrency = max(
        1, min(multiprocessing.cpu_count(), static_resources["CPU"]))

    # Format the resource argument in a form like 'CPU,1.0,GPU,0,Custom,3'.
    resource_argument = ",".join(
        ["{},{}".format(*kv) for kv in static_resources.items()])

    gcs_ip_address, gcs_port = redis_address.split(":")

    if include_java is True:
        java_worker_options = (java_worker_options
                               or DEFAULT_JAVA_WORKER_OPTIONS)
        java_worker_command = build_java_worker_command(
            java_worker_options, redis_address, plasma_store_name, raylet_name)
    else:
        java_worker_command = ""

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
        str(num_initial_workers),
        str(maximum_startup_concurrency),
        resource_argument,
        config_str,
        start_worker_command,
        java_worker_command,
        redis_password or "",
        get_temp_root(),
    ]
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_RAYLET,
        use_valgrind=use_valgrind,
        use_gdb=False,
        use_valgrind_profiler=use_profiler,
        use_perftools_profiler=("RAYLET_PERFTOOLS_PATH" in os.environ),
        stdout_file=stdout_file,
        stderr_file=stderr_file)
    record_log_files_in_redis(
        redis_address,
        node_ip_address, [stdout_file, stderr_file],
        password=redis_password)

    return process_info


def build_java_worker_command(java_worker_options, redis_address,
                              plasma_store_name, raylet_name):
    """This method assembles the command used to start a Java worker.

    Args:
        java_worker_options (str): The command options for Java worker.
        redis_address (str): Redis address of GCS.
        plasma_store_name (str): The name of the plasma store socket to connect
           to.
        raylet_name (str): The name of the raylet socket to create.

    Returns:
        The command string for starting Java worker.
    """
    assert java_worker_options is not None

    command = "java {} ".format(java_worker_options)
    if redis_address is not None:
        command += "-Dray.redis.address={} ".format(redis_address)

    if plasma_store_name is not None:
        command += (
            "-Dray.object-store.socket-name={} ".format(plasma_store_name))

    if raylet_name is not None:
        command += "-Dray.raylet.socket-name={} ".format(raylet_name)

    command += "-Dray.home={} ".format(RAY_HOME)
    command += "-Dray.log-dir={} ".format(get_logs_dir_path())
    command += "org.ray.runtime.runner.worker.DefaultWorker"

    return command


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

    return object_store_memory, plasma_directory


def _start_plasma_store(plasma_store_memory,
                        use_valgrind=False,
                        use_profiler=False,
                        stdout_file=None,
                        stderr_file=None,
                        plasma_directory=None,
                        huge_pages=False,
                        socket_name=None):
    """Start a plasma store process.

    Args:
        plasma_store_memory (int): The amount of memory in bytes to start the
            plasma store with.
        use_valgrind (bool): True if the plasma store should be started inside
            of valgrind. If this is True, use_profiler must be False.
        use_profiler (bool): True if the plasma store should be started inside
            a profiler. If this is True, use_valgrind must be False.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        plasma_directory: A directory where the Plasma memory mapped files will
            be created.
        huge_pages: a boolean flag indicating whether to start the
            Object Store with hugetlbfs support. Requires plasma_directory.
        socket_name (str): If provided, it will specify the socket
            name used by the plasma store.

    Return:
        A tuple of the name of the plasma store socket and ProcessInfo for the
            plasma store process.
    """
    if use_valgrind and use_profiler:
        raise Exception("Cannot use valgrind and profiler at the same time.")

    if huge_pages and not (sys.platform == "linux"
                           or sys.platform == "linux2"):
        raise Exception("The huge_pages argument is only supported on "
                        "Linux.")

    if huge_pages and plasma_directory is None:
        raise Exception("If huge_pages is True, then the "
                        "plasma_directory argument must be provided.")

    if not isinstance(plasma_store_memory, int):
        raise Exception("plasma_store_memory should be an integer.")

    command = [
        PLASMA_STORE_EXECUTABLE, "-s", socket_name, "-m",
        str(plasma_store_memory)
    ]
    if plasma_directory is not None:
        command += ["-d", plasma_directory]
    if huge_pages:
        command += ["-h"]
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_PLASMA_STORE,
        use_valgrind=use_valgrind,
        use_valgrind_profiler=use_profiler,
        stdout_file=stdout_file,
        stderr_file=stderr_file)
    return process_info


def start_plasma_store(node_ip_address,
                       redis_address,
                       stdout_file=None,
                       stderr_file=None,
                       object_store_memory=None,
                       plasma_directory=None,
                       huge_pages=False,
                       plasma_store_socket_name=None,
                       redis_password=None):
    """This method starts an object store process.

    Args:
        node_ip_address (str): The IP address of the node running the object
            store.
        redis_address (str): The address of the Redis instance to connect to.
        stdout_file: A file handle opened for writing to redirect stdout
            to. If no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr
            to. If no redirection should happen, then this should be None.
        object_store_memory: The amount of memory (in bytes) to start the
            object store with.
        plasma_directory: A directory where the Plasma memory mapped files will
            be created.
        huge_pages: Boolean flag indicating whether to start the Object
            Store with hugetlbfs support. Requires plasma_directory.
        redis_password (str): The password of the redis server.

    Returns:
        ProcessInfo for the process that was started.
    """
    object_store_memory, plasma_directory = determine_plasma_store_config(
        object_store_memory, plasma_directory, huge_pages)

    if object_store_memory < ray_constants.OBJECT_STORE_MINIMUM_MEMORY_BYTES:
        raise ValueError("Attempting to cap object store memory usage at {} "
                         "bytes, but the minimum allowed is {} bytes.".format(
                             object_store_memory,
                             ray_constants.OBJECT_STORE_MINIMUM_MEMORY_BYTES))

    # Print the object store memory using two decimal places.
    object_store_memory_str = (object_store_memory / 10**7) / 10**2
    logger.info("Starting the Plasma object store with {} GB memory "
                "using {}.".format(object_store_memory_str, plasma_directory))
    # Start the Plasma store.
    process_info = _start_plasma_store(
        object_store_memory,
        use_profiler=RUN_PLASMA_STORE_PROFILER,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        plasma_directory=plasma_directory,
        huge_pages=huge_pages,
        socket_name=plasma_store_socket_name)

    record_log_files_in_redis(
        redis_address,
        node_ip_address, [stdout_file, stderr_file],
        password=redis_password)

    return process_info


def start_worker(node_ip_address,
                 object_store_name,
                 raylet_name,
                 redis_address,
                 worker_path,
                 stdout_file=None,
                 stderr_file=None):
    """This method starts a worker process.

    Args:
        node_ip_address (str): The IP address of the node that this worker is
            running on.
        object_store_name (str): The socket name of the object store.
        raylet_name (str): The socket name of the raylet server.
        redis_address (str): The address that the Redis server is listening on.
        worker_path (str): The path of the source code which the worker process
            will run.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.

    Returns:
        ProcessInfo for the process that was started.
    """
    command = [
        sys.executable, "-u", worker_path,
        "--node-ip-address=" + node_ip_address,
        "--object-store-name=" + object_store_name,
        "--raylet-name=" + raylet_name,
        "--redis-address=" + str(redis_address),
        "--temp-dir=" + get_temp_root()
    ]
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_WORKER,
        stdout_file=stdout_file,
        stderr_file=stderr_file)
    record_log_files_in_redis(redis_address, node_ip_address,
                              [stdout_file, stderr_file])
    return process_info


def start_monitor(redis_address,
                  node_ip_address,
                  stdout_file=None,
                  stderr_file=None,
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
        autoscaling_config: path to autoscaling config file.
        redis_password (str): The password of the redis server.

    Returns:
        ProcessInfo for the process that was started.
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
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_MONITOR,
        stdout_file=stdout_file,
        stderr_file=stderr_file)
    record_log_files_in_redis(
        redis_address,
        node_ip_address, [stdout_file, stderr_file],
        password=redis_password)
    return process_info


def start_raylet_monitor(redis_address,
                         stdout_file=None,
                         stderr_file=None,
                         redis_password=None,
                         config=None):
    """Run a process to monitor the other processes.

    Args:
        redis_address (str): The address that the Redis server is listening on.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        redis_password (str): The password of the redis server.
        config (dict|None): Optional configuration that will
            override defaults in RayConfig.

    Returns:
        ProcessInfo for the process that was started.
    """
    gcs_ip_address, gcs_port = redis_address.split(":")
    redis_password = redis_password or ""
    config = config or {}
    config_str = ",".join(["{},{}".format(*kv) for kv in config.items()])
    command = [RAYLET_MONITOR_EXECUTABLE, gcs_ip_address, gcs_port, config_str]
    if redis_password:
        command += [redis_password]
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_RAYLET_MONITOR,
        stdout_file=stdout_file,
        stderr_file=stderr_file)
    return process_info
