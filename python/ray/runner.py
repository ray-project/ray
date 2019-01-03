import logging
import multiprocessing
import os
import random
import resource
import signal
import subprocess
import sys
import threading
import time

import redis

import ray.ray_constants as ray_constants
import ray.utils

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray configures it by default automatically
# using logging.basicConfig in its entry/init points.
logger = logging.getLogger(__name__)

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

PLASMA_STORE_EXECUTABLE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "core/src/plasma/plasma_store_server")

RUN_PLASMA_STORE_PROFILER = False
# True if processes are run in the valgrind profiler.
RUN_RAYLET_PROFILER = False

PLASMA_WAIT_TIMEOUT = 2**30

DEFAULT_PLASMA_STORE_MEMORY = 10**9

RAYLET_EXECUTABLE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "core/src/ray/raylet/raylet")


# Location of the raylet executables.
RAYLET_MONITOR_EXECUTABLE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "core/src/ray/raylet/raylet_monitor")

DEFAULT_WORKER_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),  "workers/default_worker.py")


def new_port():
    return random.randint(10000, 65535)


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


def _start_redis_instance(port=None,
                          redis_max_clients=None,
                          num_retries=20,
                          stdout_file=None,
                          stderr_file=None,
                          password=None,
                          executable=REDIS_EXECUTABLE,
                          modules=None,
                          redis_max_memory=None):
    """Start a single Redis server.

    Args:
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
        executable (str): Full path tho the redis-server executable.
        modules (list of str): A list of pathnames, pointing to the redis
            module(s) that will be loaded in this redis server.  If None, load
            the default Ray redis module.
        redis_max_memory: The max amount of memory (in bytes) to allow redis
            to use, or None for no limit. Once the limit is exceeded, redis
            will start LRU eviction of entries.

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
            break
        port = new_port()
        counter += 1
    else:
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
    return port, p


def start_redis_instance(port=None,
                         redis_max_clients=None,
                         password=None,
                         redis_max_memory=None,
                         use_credis=None,
                         stdout_file=None,
                         stderr_file=None):
    """Start a redis server instance.

    Args:
        port (int): If provided, the primary Redis shard will be started on
            this port.
        redis_max_clients: If this is provided, Ray will attempt to configure
            Redis with this maxclients number.
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
    Returns:
        A tuple of the port used by Redis and a handle to the process that was
            started. If a port is passed in, then the returned port value is
            the same.
    """
    if use_credis:
        if password is not None:
            # TODO(pschafhalter) remove this once credis supports
            # authenticating Redis ports
            raise Exception("Setting the `redis_password` argument is not "
                            "supported in credis. To run Ray with "
                            "password-protected Redis ports, ensure that "
                            "the environment variable `RAY_USE_NEW_GCS=off`.")

        # It is important to load the credis module BEFORE the ray module,
        # as the latter contains an extern declaration that the former
        # supplies.
        modules = [CREDIS_MASTER_MODULE, REDIS_MODULE]
        executable = CREDIS_EXECUTABLE
    else:
        modules = [REDIS_MODULE]
        executable = REDIS_EXECUTABLE

    return _start_redis_instance(
        port=port,
        redis_max_clients=redis_max_clients,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        executable=executable,
        modules=modules,
        password=password,
        redis_max_memory=redis_max_memory)


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
    return p


def start_monitor(redis_address,
                  redis_password=None,
                  autoscaling_config=None,
                  stdout_file=None,
                  stderr_file=None):
    """Run a process to monitor the other processes.

    Args:
        redis_address (str): The address that the Redis server is listening on.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
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
    return p


def start_raylet_monitor(redis_address,
                         redis_password=None,
                         config=None,
                         stdout_file=None,
                         stderr_file=None):
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
    """
    gcs_ip_address, gcs_port = redis_address.split(":")
    redis_password = redis_password or ""
    config = config or {}
    config_str = ",".join(["{},{}".format(*kv) for kv in config.items()])
    command = [RAYLET_MONITOR_EXECUTABLE, gcs_ip_address, gcs_port, config_str]
    if redis_password:
        command += [redis_password]
    p = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
    return p


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
                    "argument with the flag '--shm-size' to 'docker run'."
                        .format(shm_avail))
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
        raise Exception("The file {} does not exist or is not a directory."
                        .format(plasma_directory))

    return object_store_memory, plasma_directory


def start_plasma_store(object_store_memory=None,
                       plasma_directory=None,
                       huge_pages=False,
                       plasma_store_socket_name=None,
                       use_profiler=RUN_PLASMA_STORE_PROFILER,
                       use_valgrind=False,
                       stdout_file=None,
                       stderr_file=None):
    """This method starts an object store process.

    Args:
        object_store_memory: The amount of memory (in bytes) to start the
            object store with.
        plasma_directory: A directory where the Plasma memory mapped files will
            be created.
        huge_pages: Boolean flag indicating whether to start the Object
            Store with hugetlbfs support. Requires plasma_directory.
        plasma_store_socket_name (str): If provided, it will specify the socket
            name used by the plasma store.
        stdout_file: A file handle opened for writing to redirect stdout
            to. If no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr
            to. If no redirection should happen, then this should be None.
        use_valgrind (bool): True if the plasma store should be started inside
            of valgrind. If this is True, use_profiler must be False.
        use_profiler (bool): True if the plasma store should be started inside
            a profiler. If this is True, use_valgrind must be False.

    Return:
        The Plasma store socket name.
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

    if use_valgrind and use_profiler:
        raise Exception("Cannot use valgrind and profiler at the same time.")

    if huge_pages and not (sys.platform == "linux"
                           or sys.platform == "linux2"):
        raise Exception("The huge_pages argument is only supported on Linux.")

    if huge_pages and plasma_directory is None:
        raise Exception("If huge_pages is True, then the "
                        "plasma_directory argument must be provided.")

    if not isinstance(object_store_memory, int):
        raise Exception("plasma_store_memory should be an integer.")

    # Start the Plasma store.
    command = [
        PLASMA_STORE_EXECUTABLE, "-s", plasma_store_socket_name, "-m",
        str(object_store_memory)
    ]
    if plasma_directory is not None:
        command += ["-d", plasma_directory]
    if huge_pages:
        command += ["-h"]
    if use_valgrind:
        pid = subprocess.Popen(
            [
                "valgrind", "--track-origins=yes", "--leak-check=full",
                "--show-leak-kinds=all", "--leak-check-heuristics=stdstring",
                "--error-exitcode=1"
            ] + command,
            stdout=stdout_file,
            stderr=stderr_file)
        time.sleep(1.0)
    elif use_profiler:
        pid = subprocess.Popen(
            ["valgrind", "--tool=callgrind"] + command,
            stdout=stdout_file,
            stderr=stderr_file)
        time.sleep(1.0)
    else:
        pid = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
        time.sleep(0.1)
    return plasma_store_socket_name, pid


def start_raylet(node_ip_address,
                 redis_address,
                 plasma_store_name,
                 raylet_socket_name,
                 static_resources,
                 start_worker_command,
                 temp_dir,
                 num_workers=0,
                 object_manager_port=None,
                 node_manager_port=None,
                 redis_password=None,
                 use_valgrind=False,
                 use_profiler=False,
                 stdout_file=None,
                 stderr_file=None,
                 config=None):
    """Start a raylet, which is a combined local scheduler and object manager.

    Args:
        plasma_store_name (str): The name of the plasma store socket to connect
             to.
        raylet_socket_name (str): The name of the raylet socket to create.
        num_workers (int): The number of workers to start.
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

    Returns:
        The raylet socket name.
    """
    config = config or {}
    config_str = ",".join(["{},{}".format(*kv) for kv in config.items()])

    if use_valgrind and use_profiler:
        raise Exception("Cannot use valgrind and profiler at the same time.")

    # Limit the number of workers that can be started in parallel by the
    # raylet. However, make sure it is at least 1.
    maximum_startup_concurrency = max(
        1, min(multiprocessing.cpu_count(), static_resources["CPU"]))

    # Format the resource argument in a form like 'CPU,1.0,GPU,0,Custom,3'.
    resource_argument = ",".join(
        ["{},{}".format(*kv) for kv in static_resources.items()])

    gcs_ip_address, gcs_port = redis_address.split(":")
    command = [
        RAYLET_EXECUTABLE,
        raylet_socket_name,
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
        temp_dir,
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
    elif "RAYLET_PERFTOOLS_PATH" in os.environ:
        modified_env = os.environ.copy()
        modified_env["LD_PRELOAD"] = os.environ["RAYLET_PERFTOOLS_PATH"]
        modified_env["CPUPROFILE"] = os.environ["RAYLET_PERFTOOLS_LOGFILE"]
        pid = subprocess.Popen(
            command, stdout=stdout_file, stderr=stderr_file, env=modified_env)
    else:
        pid = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)

    return raylet_socket_name, pid


def start_ui(port,
             new_notebook_directory,
             webui_url,
             token,
             redis_address,
             stdout_file=None,
             stderr_file=None):
    """Start a UI process.

    Args:
        redis_address: The address of the primary Redis shard.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
    """
    new_env = os.environ.copy()
    new_env["REDIS_ADDRESS"] = redis_address

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
        return None, None
    else:
        logger.info("\n" + "=" * 70)
        logger.info("View the web UI at {}".format(webui_url))
        logger.info("=" * 70 + "\n")
        return webui_url, ui_process


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