import base64
import collections
import errno
import io
import json
import logging
import multiprocessing
import os
from pathlib import Path
import mmap
import random
import shutil
import signal
import socket
import subprocess
import sys
import time
from typing import Optional, List
import uuid

# Ray modules
import ray
import ray.ray_constants as ray_constants
from ray._raylet import GcsClientOptions
from ray._private.gcs_utils import GcsClient, use_gcs_for_bootstrap
import redis
from ray.core.generated.common_pb2 import Language

# Import psutil and colorama after ray so the packaged version is used.
import colorama
import psutil

resource = None
if sys.platform != "win32":
    import resource

EXE_SUFFIX = ".exe" if sys.platform == "win32" else ""

# True if processes are run in the valgrind profiler.
RUN_RAYLET_PROFILER = False

# Location of the redis server.
RAY_HOME = os.path.join(os.path.dirname(os.path.dirname(__file__)), "../..")
RAY_PATH = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
RAY_PRIVATE_DIR = "_private"
AUTOSCALER_PRIVATE_DIR = "autoscaler/_private"
REDIS_EXECUTABLE = os.path.join(
    RAY_PATH, "core/src/ray/thirdparty/redis/src/redis-server" + EXE_SUFFIX
)

# Location of the raylet executables.
RAYLET_EXECUTABLE = os.path.join(RAY_PATH, "core/src/ray/raylet/raylet" + EXE_SUFFIX)
GCS_SERVER_EXECUTABLE = os.path.join(
    RAY_PATH, "core/src/ray/gcs/gcs_server" + EXE_SUFFIX
)

# Location of the cpp default worker executables.
DEFAULT_WORKER_EXECUTABLE = os.path.join(RAY_PATH, "cpp/default_worker" + EXE_SUFFIX)

# Location of the native libraries.
DEFAULT_NATIVE_LIBRARY_PATH = os.path.join(RAY_PATH, "cpp/lib")

DASHBOARD_DEPENDENCY_ERROR_MESSAGE = (
    "Not all Ray Dashboard dependencies were "
    "found. To use the dashboard please "
    "install Ray using `pip install "
    "ray[default]`."
)

RAY_JEMALLOC_LIB_PATH = "RAY_JEMALLOC_LIB_PATH"
RAY_JEMALLOC_CONF = "RAY_JEMALLOC_CONF"
RAY_JEMALLOC_PROFILE = "RAY_JEMALLOC_PROFILE"

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)

ProcessInfo = collections.namedtuple(
    "ProcessInfo",
    [
        "process",
        "stdout_file",
        "stderr_file",
        "use_valgrind",
        "use_gdb",
        "use_valgrind_profiler",
        "use_perftools_profiler",
        "use_tmux",
    ],
)


def _get_gcs_client_options(redis_address, redis_password, gcs_server_address):
    if not use_gcs_for_bootstrap():
        redis_ip_address, redis_port = redis_address.split(":")
        wait_for_redis_to_start(redis_ip_address, redis_port, redis_password)
        return GcsClientOptions.from_redis_address(redis_address, redis_password)
    else:
        return GcsClientOptions.from_gcs_address(gcs_server_address)


def serialize_config(config):
    return base64.b64encode(json.dumps(config).encode("utf-8")).decode("utf-8")


def propagate_jemalloc_env_var(
    *,
    jemalloc_path: str,
    jemalloc_conf: str,
    jemalloc_comps: List[str],
    process_type: str,
):
    """Read the jemalloc memory profiling related
    env var and return the dictionary that translates
    them to proper jemalloc related env vars.

    For example, if users specify `RAY_JEMALLOC_LIB_PATH`,
    it is translated into `LD_PRELOAD` which is needed to
    run Jemalloc as a shared library.

    Params:
        jemalloc_path (str): The path to the jemalloc shared library.
        jemalloc_conf (str): `,` separated string of jemalloc config.
        jemalloc_comps List(str): The list of Ray components
            that we will profile.
        process_type (str): The process type that needs jemalloc
            env var for memory profiling. If it doesn't match one of
            jemalloc_comps, the function will return an empty dict.

    Returns:
        dictionary of {env_var: value}
            that are needed to jemalloc profiling. The caller can
            call `dict.update(return_value_of_this_func)` to
            update the dict of env vars. If the process_type doesn't
            match jemalloc_comps, it will return an empty dict.
    """
    assert isinstance(jemalloc_comps, list)
    assert process_type is not None
    process_type = process_type.lower()
    if not jemalloc_path or process_type not in jemalloc_comps:
        return {}

    env_vars = {
        "LD_PRELOAD": jemalloc_path,
    }
    if jemalloc_conf:
        env_vars.update({"MALLOC_CONF": jemalloc_conf})
    return env_vars


class ConsolePopen(subprocess.Popen):
    if sys.platform == "win32":

        def terminate(self):
            if isinstance(self.stdin, io.IOBase):
                self.stdin.close()
            if self._use_signals:
                self.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                super(ConsolePopen, self).terminate()

        def __init__(self, *args, **kwargs):
            # CREATE_NEW_PROCESS_GROUP is used to send Ctrl+C on Windows:
            # https://docs.python.org/3/library/subprocess.html#subprocess.Popen.send_signal
            new_pgroup = subprocess.CREATE_NEW_PROCESS_GROUP
            flags_to_add = 0
            if ray._private.utils.detect_fate_sharing_support():
                # If we don't have kernel-mode fate-sharing, then don't do this
                # because our children need to be in out process group for
                # the process reaper to properly terminate them.
                flags_to_add = new_pgroup
            flags_key = "creationflags"
            if flags_to_add:
                kwargs[flags_key] = (kwargs.get(flags_key) or 0) | flags_to_add
            self._use_signals = kwargs[flags_key] & new_pgroup
            super(ConsolePopen, self).__init__(*args, **kwargs)


def address(ip_address, port):
    return ip_address + ":" + str(port)


def new_port(lower_bound=10000, upper_bound=65535, denylist=None):
    if not denylist:
        denylist = set()
    port = random.randint(lower_bound, upper_bound)
    retry = 0
    while port in denylist:
        if retry > 100:
            break
        port = random.randint(lower_bound, upper_bound)
        retry += 1
    if retry > 100:
        raise ValueError(
            "Failed to find a new port from the range "
            f"{lower_bound}-{upper_bound}. Denylist: {denylist}"
        )
    return port


def _find_address_from_flag(flag: str):
    """
    Attempts to find all valid Ray addresses on this node, specified by the
    flag.

    Params:
        flag: `--redis-address` or `--gcs-address`
    Returns:
        Set of detected addresses.
    """
    # Using Redis address `--redis-address` as an example:
    # Currently, this extracts the deprecated --redis-address from the command
    # that launched the raylet running on this node, if any. Anyone looking to
    # edit this function should be warned that these commands look like, for
    # example:
    # /usr/local/lib/python3.8/dist-packages/ray/core/src/ray/raylet/raylet
    # --redis_address=123.456.78.910 --node_ip_address=123.456.78.910
    # --raylet_socket_name=... --store_socket_name=... --object_manager_port=0
    # --min_worker_port=10000 --max_worker_port=19999
    # --node_manager_port=58578 --redis_port=6379
    # --maximum_startup_concurrency=8
    # --static_resource_list=node:123.456.78.910,1.0,object_store_memory,66
    # --config_list=plasma_store_as_thread,True
    # --python_worker_command=/usr/bin/python
    #     /usr/local/lib/python3.8/dist-packages/ray/workers/default_worker.py
    #     --redis-address=123.456.78.910:6379
    #     --node-ip-address=123.456.78.910 --node-manager-port=58578
    #     --object-store-name=... --raylet-name=...
    #     --temp-dir=/tmp/ray
    #     --metrics-agent-port=41856 --redis-password=[MASKED]
    #     --java_worker_command= --cpp_worker_command=
    #     --redis_password=[MASKED] --temp_dir=/tmp/ray --session_dir=...
    #     --metrics-agent-port=41856 --metrics_export_port=64229
    #     --agent_command=/usr/bin/python
    #     -u /usr/local/lib/python3.8/dist-packages/ray/dashboard/agent.py
    #         --redis-address=123.456.78.910:6379 --metrics-export-port=64229
    #         --dashboard-agent-port=41856 --node-manager-port=58578
    #         --object-store-name=... --raylet-name=... --temp-dir=/tmp/ray
    #         --log-dir=/tmp/ray/session_2020-11-08_14-29-07_199128_278000/logs
    #         --redis-password=[MASKED] --object_store_memory=5037192806
    #         --plasma_directory=/tmp
    # Longer arguments are elided with ... but all arguments from this instance
    # are included, to provide a sense of what is in these.
    # Indeed, we had to pull --redis-address to the front of each call to make
    # this readable.
    # As you can see, this is very long and complex, which is why we can't
    # simply extract all the the arguments using regular expressions and
    # present a dict as if we never lost track of these arguments, for
    # example. Picking out --redis-address below looks like it might grab the
    # wrong thing, but double-checking that we're finding the correct process
    # by checking that the contents look like we expect would probably be prone
    # to choking in unexpected ways.
    # Notice that --redis-address appears twice. This is not a copy-paste
    # error; this is the reason why the for loop below attempts to pick out
    # every appearance of --redis-address.

    # The --redis-address here is what is now called the --address, but it
    # appears in the default_worker.py and agent.py calls as --redis-address.
    pids = psutil.pids()
    addresses = set()
    for pid in pids:
        try:
            proc = psutil.Process(pid)
            # HACK: Workaround for UNIX idiosyncrasy
            # Normally, cmdline() is supposed to return the argument list.
            # But it in some cases (such as when setproctitle is called),
            # an arbitrary string resembling a command-line is stored in
            # the first argument.
            # Explanation: https://unix.stackexchange.com/a/432681
            # More info: https://github.com/giampaolo/psutil/issues/1179
            cmdline = proc.cmdline()
            # NOTE(kfstorm): To support Windows, we can't use
            # `os.path.basename(cmdline[0]) == "raylet"` here.
            if len(cmdline) > 0 and "raylet" in os.path.basename(cmdline[0]):
                for arglist in cmdline:
                    # Given we're merely seeking --redis-address, we just split
                    # every argument on spaces for now.
                    for arg in arglist.split(" "):
                        # TODO(ekl): Find a robust solution for locating Redis.
                        if arg.startswith(flag):
                            proc_addr = arg.split("=")[1]
                            # TODO(mwtian): remove this workaround after Ray
                            # no longer sets --redis-address to None.
                            if proc_addr != "" and proc_addr != "None":
                                addresses.add(proc_addr)
        except psutil.AccessDenied:
            pass
        except psutil.NoSuchProcess:
            pass
    return addresses


def find_redis_address():
    return _find_address_from_flag("--redis-address")


def find_gcs_address():
    return _find_address_from_flag("--gcs-address")


def find_bootstrap_address():
    if use_gcs_for_bootstrap():
        return find_gcs_address()
    else:
        return find_redis_address()


def _find_redis_address_or_die():
    """Finds one Redis address unambiguously, or raise an error.

    Callers outside of this module should use
    get_ray_address_from_environment() or canonicalize_bootstrap_address()
    """
    redis_addresses = find_redis_address()
    if len(redis_addresses) > 1:
        raise ConnectionError(
            f"Found multiple active Ray instances: {redis_addresses}. "
            "Please specify the one to connect to by setting `address`."
        )
        sys.exit(1)
    elif not redis_addresses:
        raise ConnectionError(
            "Could not find any running Ray instance. "
            "Please specify the one to connect to by setting `address`."
        )
    return redis_addresses.pop()


def _find_gcs_address_or_die():
    """Find one GCS address unambiguously, or raise an error.

    Callers outside of this module should use get_ray_address_to_use_or_die()
    """
    gcs_addresses = _find_address_from_flag("--gcs-address")
    if len(gcs_addresses) > 1:
        raise ConnectionError(
            f"Found multiple active Ray instances: {gcs_addresses}. "
            "Please specify the one to connect to by setting `--address` flag "
            "or `RAY_ADDRESS` environment variable."
        )
        sys.exit(1)
    elif not gcs_addresses:
        raise ConnectionError(
            "Could not find any running Ray instance. "
            "Please specify the one to connect to by setting `--address` flag "
            "or `RAY_ADDRESS` environment variable."
        )
    return gcs_addresses.pop()


def get_ray_address_from_environment():
    """
    Attempts to find the address of Ray cluster to use, first from
    RAY_ADDRESS environment variable, then from the local Raylet.

    Returns:
        A string to pass into `ray.init(address=...)`, e.g. ip:port, `auto`.
    """
    addr = os.environ.get(ray_constants.RAY_ADDRESS_ENVIRONMENT_VARIABLE)
    if addr is None or addr == "auto":
        if use_gcs_for_bootstrap():
            addr = _find_gcs_address_or_die()
        else:
            addr = _find_redis_address_or_die()
    return addr


def wait_for_node(
    redis_address,
    gcs_address,
    node_plasma_store_socket_name,
    redis_password=None,
    timeout=30,
):
    """Wait until this node has appeared in the client table.

    Args:
        redis_address (str): The redis address.
        gcs_address (str): The gcs address
        node_plasma_store_socket_name (str): The
            plasma_store_socket_name for the given node which we wait for.
        redis_password (str): the redis password.
        timeout: The amount of time in seconds to wait before raising an
            exception.

    Raises:
        TimeoutError: An exception is raised if the timeout expires before
            the node appears in the client table.
    """
    if use_gcs_for_bootstrap():
        gcs_options = GcsClientOptions.from_gcs_address(gcs_address)
    else:
        redis_ip_address, redis_port = redis_address.split(":")
        wait_for_redis_to_start(redis_ip_address, redis_port, redis_password)
        gcs_options = GcsClientOptions.from_redis_address(redis_address, redis_password)
    global_state = ray.state.GlobalState()
    global_state._initialize_global_state(gcs_options)
    start_time = time.time()
    while time.time() - start_time < timeout:
        clients = global_state.node_table()
        object_store_socket_names = [
            client["ObjectStoreSocketName"] for client in clients
        ]
        if node_plasma_store_socket_name in object_store_socket_names:
            return
        else:
            time.sleep(0.1)
    raise TimeoutError("Timed out while waiting for node to startup.")


def get_node_to_connect_for_driver(
    redis_address, gcs_address, node_ip_address, redis_password=None
):
    # Get node table from global state accessor.
    global_state = ray.state.GlobalState()
    gcs_options = _get_gcs_client_options(redis_address, redis_password, gcs_address)
    global_state._initialize_global_state(gcs_options)
    return global_state.get_node_to_connect_for_driver(node_ip_address)


def get_webui_url_from_internal_kv():
    assert ray.experimental.internal_kv._internal_kv_initialized()
    webui_url = ray.experimental.internal_kv._internal_kv_get(
        "webui:url", namespace=ray_constants.KV_NAMESPACE_DASHBOARD
    )
    return ray._private.utils.decode(webui_url) if webui_url is not None else None


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
        raise RuntimeError(
            "This process is not in a position to determine "
            "whether all processes are alive or not."
        )
    return ray.worker._global_node.remaining_processes_alive()


def canonicalize_bootstrap_address(addr: str):
    """Canonicalizes Ray cluster bootstrap address to host:port.
    Reads address from the environment if needed.

    This function should be used to process user supplied Ray cluster address,
    via ray.init() or `--address` flags, before using the address to connect.

    Returns:
        Ray cluster address string in <host:port> format.
    """
    if addr is None or addr == "auto":
        addr = get_ray_address_from_environment()
    try:
        bootstrap_address = resolve_ip_for_localhost(addr)
    except Exception:
        logger.exception(f"Failed to convert {addr} to host:port")
        raise
    return bootstrap_address


def extract_ip_port(bootstrap_address: str):
    if ":" not in bootstrap_address:
        raise ValueError(
            f"Malformed address {bootstrap_address}. " f"Expected '<host>:<port>'."
        )
    ip, _, port = bootstrap_address.rpartition(":")
    try:
        port = int(port)
    except ValueError:
        raise ValueError(f"Malformed address port {port}. Must be an integer.")
    if port < 1024 or port > 65535:
        raise ValueError(
            f"Invalid address port {port}. Must be between 1024 "
            "and 65535 (inclusive)."
        )
    return ip, port


def resolve_ip_for_localhost(address: str):
    """Convert to a remotely reachable IP if the address is "localhost"
            or "127.0.0.1". Otherwise do nothing.

    Args:
        address: This can be either a string containing a hostname (or an IP
            address) and a port or it can be just an IP address.

    Returns:
        The same address but with the local host replaced by remotely
            reachable IP.
    """
    if not address:
        raise ValueError(f"Malformed address: {address}")
    address_parts = address.split(":")
    # Make sure localhost isn't resolved to the loopback ip
    if address_parts[0] == "127.0.0.1" or address_parts[0] == "localhost":
        ip_address = get_node_ip_address()
        return ":".join([ip_address] + address_parts[1:])
    else:
        return address


def node_ip_address_from_perspective(address):
    """IP address by which the local node can be reached *from* the `address`.

    Args:
        address (str): The IP address and port of any known live service on the
            network you care about.

    Returns:
        The IP address by which the local node can be reached from the address.
    """
    ip_address, port = address.split(":")
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # This command will raise an exception if there is no internet
        # connection.
        s.connect((ip_address, int(port)))
        node_ip_address = s.getsockname()[0]
    except OSError as e:
        node_ip_address = "127.0.0.1"
        # [Errno 101] Network is unreachable
        if e.errno == errno.ENETUNREACH:
            try:
                # try get node ip address from host name
                host_name = socket.getfqdn(socket.gethostname())
                node_ip_address = socket.gethostbyname(host_name)
            except Exception:
                pass
    finally:
        s.close()

    return node_ip_address


def get_node_ip_address(address="8.8.8.8:53"):
    if ray.worker._global_node is not None:
        return ray.worker._global_node.node_ip_address
    if sys.platform == "darwin" or sys.platform == "win32":
        # Due to the mac osx/windows firewall,
        # we use loopback ip as the ip address
        # to prevent security popups.
        return "127.0.0.1"
    return node_ip_address_from_perspective(address)


def create_redis_client(redis_address, password=None):
    """Create a Redis client.

    Args:
        The IP address, port, and password of the Redis server.

    Returns:
        A Redis client.
    """
    if not hasattr(create_redis_client, "instances"):
        create_redis_client.instances = {}

    for _ in range(ray_constants.START_REDIS_WAIT_RETRIES):
        cli = create_redis_client.instances.get(redis_address)
        if cli is None:
            redis_ip_address, redis_port = extract_ip_port(
                canonicalize_bootstrap_address(redis_address)
            )
            cli = redis.StrictRedis(
                host=redis_ip_address, port=int(redis_port), password=password
            )
            create_redis_client.instances[redis_address] = cli
        try:
            cli.ping()
            return cli
        except Exception:
            create_redis_client.instances.pop(redis_address)
            time.sleep(2)

    raise RuntimeError(f"Unable to connect to Redis at {redis_address}")


def start_ray_process(
    command,
    process_type,
    fate_share,
    env_updates=None,
    cwd=None,
    use_valgrind=False,
    use_gdb=False,
    use_valgrind_profiler=False,
    use_perftools_profiler=False,
    use_tmux=False,
    stdout_file=None,
    stderr_file=None,
    pipe_stdin=False,
):
    """Start one of the Ray processes.

    TODO(rkn): We need to figure out how these commands interact. For example,
    it may only make sense to start a process in gdb if we also start it in
    tmux. Similarly, certain combinations probably don't make sense, like
    simultaneously running the process in valgrind and the profiler.

    Args:
        command (List[str]): The command to use to start the Ray process.
        process_type (str): The type of the process that is being started
            (e.g., "raylet").
        fate_share: If true, the child will be killed if its parent (us) dies.
            True must only be passed after detection of this functionality.
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
        pipe_stdin: If true, subprocess.PIPE will be passed to the process as
            stdin.

    Returns:
        Information about the process that was started including a handle to
            the process that was started.
    """
    # Detect which flags are set through environment variables.
    valgrind_env_var = f"RAY_{process_type.upper()}_VALGRIND"
    if os.environ.get(valgrind_env_var) == "1":
        logger.info("Detected environment variable '%s'.", valgrind_env_var)
        use_valgrind = True
    valgrind_profiler_env_var = f"RAY_{process_type.upper()}_VALGRIND_PROFILER"
    if os.environ.get(valgrind_profiler_env_var) == "1":
        logger.info("Detected environment variable '%s'.", valgrind_profiler_env_var)
        use_valgrind_profiler = True
    perftools_profiler_env_var = f"RAY_{process_type.upper()}" "_PERFTOOLS_PROFILER"
    if os.environ.get(perftools_profiler_env_var) == "1":
        logger.info("Detected environment variable '%s'.", perftools_profiler_env_var)
        use_perftools_profiler = True
    tmux_env_var = f"RAY_{process_type.upper()}_TMUX"
    if os.environ.get(tmux_env_var) == "1":
        logger.info("Detected environment variable '%s'.", tmux_env_var)
        use_tmux = True
    gdb_env_var = f"RAY_{process_type.upper()}_GDB"
    if os.environ.get(gdb_env_var) == "1":
        logger.info("Detected environment variable '%s'.", gdb_env_var)
        use_gdb = True
    # Jemalloc memory profiling.
    jemalloc_lib_path = os.environ.get(RAY_JEMALLOC_LIB_PATH)
    jemalloc_conf = os.environ.get(RAY_JEMALLOC_CONF)
    jemalloc_comps = os.environ.get(RAY_JEMALLOC_PROFILE)
    jemalloc_comps = [] if not jemalloc_comps else jemalloc_comps.split(",")
    jemalloc_env_vars = propagate_jemalloc_env_var(
        jemalloc_path=jemalloc_lib_path,
        jemalloc_conf=jemalloc_conf,
        jemalloc_comps=jemalloc_comps,
        process_type=process_type,
    )
    use_jemalloc_mem_profiler = len(jemalloc_env_vars) > 0

    if (
        sum(
            [
                use_gdb,
                use_valgrind,
                use_valgrind_profiler,
                use_perftools_profiler,
                use_jemalloc_mem_profiler,
            ]
        )
        > 1
    ):
        raise ValueError(
            "At most one of the 'use_gdb', 'use_valgrind', "
            "'use_valgrind_profiler', 'use_perftools_profiler', "
            "and 'use_jemalloc_mem_profiler' flags can "
            "be used at a time."
        )
    if env_updates is None:
        env_updates = {}
    if not isinstance(env_updates, dict):
        raise ValueError("The 'env_updates' argument must be a dictionary.")

    modified_env = os.environ.copy()
    modified_env.update(env_updates)

    if use_gdb:
        if not use_tmux:
            raise ValueError(
                "If 'use_gdb' is true, then 'use_tmux' must be true as well."
            )

        # TODO(suquark): Any better temp file creation here?
        gdb_init_path = os.path.join(
            ray._private.utils.get_ray_temp_dir(),
            f"gdb_init_{process_type}_{time.time()}",
        )
        ray_process_path = command[0]
        ray_process_args = command[1:]
        run_args = " ".join(["'{}'".format(arg) for arg in ray_process_args])
        with open(gdb_init_path, "w") as gdb_init_file:
            gdb_init_file.write(f"run {run_args}")
        command = ["gdb", ray_process_path, "-x", gdb_init_path]

    if use_valgrind:
        command = [
            "valgrind",
            "--track-origins=yes",
            "--leak-check=full",
            "--show-leak-kinds=all",
            "--leak-check-heuristics=stdstring",
            "--error-exitcode=1",
        ] + command

    if use_valgrind_profiler:
        command = ["valgrind", "--tool=callgrind"] + command

    if use_perftools_profiler:
        modified_env["LD_PRELOAD"] = os.environ["PERFTOOLS_PATH"]
        modified_env["CPUPROFILE"] = os.environ["PERFTOOLS_LOGFILE"]

    if use_jemalloc_mem_profiler:
        logger.info(
            f"Jemalloc profiling will be used for {process_type}. "
            f"env vars: {jemalloc_env_vars}"
        )
        modified_env.update(jemalloc_env_vars)

    if use_tmux:
        # The command has to be created exactly as below to ensure that it
        # works on all versions of tmux. (Tested with tmux 1.8-5, travis'
        # version, and tmux 2.1)
        command = ["tmux", "new-session", "-d", f"{' '.join(command)}"]

    if fate_share:
        assert ray._private.utils.detect_fate_sharing_support(), (
            "kernel-level fate-sharing must only be specified if "
            "detect_fate_sharing_support() has returned True"
        )

    def preexec_fn():
        import signal

        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
        if fate_share and sys.platform.startswith("linux"):
            ray._private.utils.set_kill_on_parent_death_linux()

    win32_fate_sharing = fate_share and sys.platform == "win32"
    # With Windows fate-sharing, we need special care:
    # The process must be added to the job before it is allowed to execute.
    # Otherwise, there's a race condition: the process might spawn children
    # before the process itself is assigned to the job.
    # After that point, its children will not be added to the job anymore.
    CREATE_SUSPENDED = 0x00000004  # from Windows headers
    if sys.platform == "win32":
        # CreateProcess, which underlies Popen, is limited to
        # 32,767 characters, including the Unicode terminating null
        # character
        total_chrs = sum([len(x) for x in command])
        if total_chrs > 31766:
            raise ValueError(
                f"command is limited to a total of 31767 characters, "
                f"got {total_chrs}"
            )

    process = ConsolePopen(
        command,
        env=modified_env,
        cwd=cwd,
        stdout=stdout_file,
        stderr=stderr_file,
        stdin=subprocess.PIPE if pipe_stdin else None,
        preexec_fn=preexec_fn if sys.platform != "win32" else None,
        creationflags=CREATE_SUSPENDED if win32_fate_sharing else 0,
    )

    if win32_fate_sharing:
        try:
            ray._private.utils.set_kill_child_on_death_win32(process)
            psutil.Process(process.pid).resume()
        except (psutil.Error, OSError):
            process.kill()
            raise

    def _get_stream_name(stream):
        if stream is not None:
            try:
                return stream.name
            except AttributeError:
                return str(stream)
        return None

    return ProcessInfo(
        process=process,
        stdout_file=_get_stream_name(stdout_file),
        stderr_file=_get_stream_name(stderr_file),
        use_valgrind=use_valgrind,
        use_gdb=use_gdb,
        use_valgrind_profiler=use_valgrind_profiler,
        use_perftools_profiler=use_perftools_profiler,
        use_tmux=use_tmux,
    )


def wait_for_redis_to_start(redis_ip_address, redis_port, password=None):
    """Wait for a Redis server to be available.

    This is accomplished by creating a Redis client and sending a random
    command to the server until the command gets through.

    Args:
        redis_ip_address (str): The IP address of the redis server.
        redis_port (int): The port of the redis server.
        password (str): The password of the redis server.

    Raises:
        Exception: An exception is raised if we could not connect with Redis.
    """
    redis_client = redis.StrictRedis(
        host=redis_ip_address, port=redis_port, password=password
    )
    # Wait for the Redis server to start.
    num_retries = ray_constants.START_REDIS_WAIT_RETRIES
    delay = 0.001
    for i in range(num_retries):
        try:
            # Run some random command and see if it worked.
            logger.debug(
                "Waiting for redis server at {}:{} to respond...".format(
                    redis_ip_address, redis_port
                )
            )
            redis_client.client_list()
        # If the Redis service is delayed getting set up for any reason, we may
        # get a redis.ConnectionError: Error 111 connecting to host:port.
        # Connection refused.
        # Unfortunately, redis.ConnectionError is also the base class of
        # redis.AuthenticationError. We *don't* want to obscure a
        # redis.AuthenticationError, because that indicates the user provided a
        # bad password. Thus a double except clause to ensure a
        # redis.AuthenticationError isn't trapped here.
        except redis.AuthenticationError as authEx:
            raise RuntimeError(
                "Unable to connect to Redis at {}:{}.".format(
                    redis_ip_address, redis_port
                )
            ) from authEx
        except redis.ConnectionError as connEx:
            if i >= num_retries - 1:
                raise RuntimeError(
                    f"Unable to connect to Redis at {redis_ip_address}:"
                    f"{redis_port} after {num_retries} retries. Check that "
                    f"{redis_ip_address}:{redis_port} is reachable from this "
                    "machine. If it is not, your firewall may be blocking "
                    "this port. If the problem is a flaky connection, try "
                    "setting the environment variable "
                    "`RAY_START_REDIS_WAIT_RETRIES` to increase the number of"
                    " attempts to ping the Redis server."
                ) from connEx
            # Wait a little bit.
            time.sleep(delay)
            delay *= 2
        else:
            break
    else:
        raise RuntimeError(
            f"Unable to connect to Redis (after {num_retries} retries). "
            "If the Redis instance is on a different machine, check that "
            "your firewall and relevant Ray ports are configured properly. "
            "You can also set the environment variable "
            "`RAY_START_REDIS_WAIT_RETRIES` to increase the number of "
            "attempts to ping the Redis server."
        )


def start_reaper(fate_share=None):
    """Start the reaper process.

    This is a lightweight process that simply
    waits for its parent process to die and then terminates its own
    process group. This allows us to ensure that ray processes are always
    terminated properly so long as that process itself isn't SIGKILLed.

    Returns:
        ProcessInfo for the process that was started.
    """
    # Make ourselves a process group leader so that the reaper can clean
    # up other ray processes without killing the process group of the
    # process that started us.
    try:
        if sys.platform != "win32":
            os.setpgrp()
    except OSError as e:
        errcode = e.errno
        if errcode == errno.EPERM and os.getpgrp() == os.getpid():
            # Nothing to do; we're already a session leader.
            pass
        else:
            logger.warning(
                "setpgrp failed, processes may not be "
                "cleaned up properly: {}.".format(e)
            )
            # Don't start the reaper in this case as it could result in killing
            # other user processes.
            return None

    reaper_filepath = os.path.join(RAY_PATH, RAY_PRIVATE_DIR, "ray_process_reaper.py")
    command = [sys.executable, "-u", reaper_filepath]
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_REAPER,
        pipe_stdin=True,
        fate_share=fate_share,
    )
    return process_info


def start_redis(
    node_ip_address,
    redirect_files,
    resource_spec,
    session_dir_path,
    port=None,
    redis_shard_ports=None,
    num_redis_shards=1,
    redis_max_clients=None,
    password=None,
    fate_share=None,
    external_addresses=None,
    port_denylist=None,
):
    """Start the Redis global state store.

    Args:
        node_ip_address: The IP address of the current node. This is only used
            for recording the log filenames in Redis.
        redirect_files: The list of (stdout, stderr) file pairs.
        resource_spec (ResourceSpec): Resources for the node.
        session_dir_path (str): Path to the session directory of
            this Ray cluster.
        port (int): If provided, the primary Redis shard will be started on
            this port.
        redis_shard_ports: A list of the ports to use for the non-primary Redis
            shards.
        num_redis_shards (int): If provided, the number of Redis shards to
            start, in addition to the primary one. The default value is one
            shard.
        redis_max_clients: If this is provided, Ray will attempt to configure
            Redis with this maxclients number.
        password (str): Prevents external clients without the password
            from connecting to Redis if provided.
        port_denylist (set): A set of denylist ports that shouldn't
            be used when allocating a new port.

    Returns:
        A tuple of the address for the primary Redis shard, a list of
            addresses for the remaining shards, and the processes that were
            started.
    """
    processes = []

    if external_addresses is not None:
        primary_redis_address = external_addresses[0]
        [primary_redis_ip, port] = primary_redis_address.split(":")
        port = int(port)
        redis_address = address(primary_redis_ip, port)
        primary_redis_client = create_redis_client(
            "%s:%s" % (primary_redis_ip, port), password=password
        )
        # Deleting the key to avoid duplicated rpush.
        primary_redis_client.delete("RedisShards")
    else:
        if len(redirect_files) != 1 + num_redis_shards:
            raise ValueError(
                "The number of redirect file pairs should be equal "
                "to the number of redis shards (including the "
                "primary shard) we will start."
            )
        if redis_shard_ports is None:
            redis_shard_ports = num_redis_shards * [None]
        elif len(redis_shard_ports) != num_redis_shards:
            raise RuntimeError(
                "The number of Redis shard ports does not match "
                "the number of Redis shards."
            )
        redis_executable = REDIS_EXECUTABLE

        redis_stdout_file, redis_stderr_file = redirect_files[0]
        # If no port is given, fallback to default Redis port for the primary
        # shard.
        if port is None:
            port = ray_constants.DEFAULT_PORT
            num_retries = 20
        else:
            num_retries = 1
        # Start the primary Redis shard.
        port, p = _start_redis_instance(
            redis_executable,
            session_dir_path,
            port=port,
            password=password,
            redis_max_clients=redis_max_clients,
            num_retries=num_retries,
            # Below we use None to indicate no limit on the memory of the
            # primary Redis shard.
            redis_max_memory=None,
            stdout_file=redis_stdout_file,
            stderr_file=redis_stderr_file,
            fate_share=fate_share,
            port_denylist=port_denylist,
            listen_to_localhost_only=(node_ip_address == "127.0.0.1"),
        )
        processes.append(p)
        redis_address = address(node_ip_address, port)
        primary_redis_client = redis.StrictRedis(
            host=node_ip_address, port=port, password=password
        )

    # Register the number of Redis shards in the primary shard, so that clients
    # know how many redis shards to expect under RedisShards.
    primary_redis_client.set("NumRedisShards", str(num_redis_shards))

    # Calculate the redis memory.
    assert resource_spec.resolved()
    redis_max_memory = resource_spec.redis_max_memory

    # Start other Redis shards. Each Redis shard logs to a separate file,
    # prefixed by "redis-<shard number>".
    redis_shards = []
    # If Redis shard ports are not provided, start the port range of the
    # other Redis shards at a high, random port.
    last_shard_port = new_port(denylist=port_denylist) - 1
    for i in range(num_redis_shards):
        if external_addresses is not None:
            shard_address = external_addresses[i + 1]
        else:
            redis_stdout_file, redis_stderr_file = redirect_files[i + 1]
            redis_executable = REDIS_EXECUTABLE
            redis_shard_port = redis_shard_ports[i]
            # If no shard port is given, try to start this shard's Redis
            # instance on the port right after the last shard's port.
            if redis_shard_port is None:
                redis_shard_port = last_shard_port + 1
                num_retries = 20
            else:
                num_retries = 1

            redis_shard_port, p = _start_redis_instance(
                redis_executable,
                session_dir_path,
                port=redis_shard_port,
                password=password,
                redis_max_clients=redis_max_clients,
                num_retries=num_retries,
                redis_max_memory=redis_max_memory,
                stdout_file=redis_stdout_file,
                stderr_file=redis_stderr_file,
                fate_share=fate_share,
                port_denylist=port_denylist,
                listen_to_localhost_only=(node_ip_address == "127.0.0.1"),
            )
            processes.append(p)

            shard_address = address(node_ip_address, redis_shard_port)
            last_shard_port = redis_shard_port

        redis_shards.append(shard_address)
        # Store redis shard information in the primary redis shard.
        primary_redis_client.rpush("RedisShards", shard_address)

    return redis_address, redis_shards, processes


def _start_redis_instance(
    executable,
    session_dir_path,
    port,
    redis_max_clients=None,
    num_retries=20,
    stdout_file=None,
    stderr_file=None,
    password=None,
    redis_max_memory=None,
    fate_share=None,
    port_denylist=None,
    listen_to_localhost_only=False,
):
    """Start a single Redis server.

    Notes:
        We will initially try to start the Redis instance at the given port,
        and then try at most `num_retries - 1` times to start the Redis
        instance at successive random ports.

    Args:
        executable (str): Full path of the redis-server executable.
        session_dir_path (str): Path to the session directory of
            this Ray cluster.
        port (int): Try to start a Redis server at this port.
        redis_max_clients: If this is provided, Ray will attempt to configure
            Redis with this maxclients number.
        num_retries (int): The number of times to attempt to start Redis at
            successive ports.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        password (str): Prevents external clients without the password
            from connecting to Redis if provided.
        redis_max_memory: The max amount of memory (in bytes) to allow redis
            to use, or None for no limit. Once the limit is exceeded, redis
            will start LRU eviction of entries.
        port_denylist (set): A set of denylist ports that shouldn't
            be used when allocating a new port.
        listen_to_localhost_only (bool): Redis server only listens to
            localhost (127.0.0.1) if it's true,
            otherwise it listens to all network interfaces.

    Returns:
        A tuple of the port used by Redis and ProcessInfo for the process that
            was started. If a port is passed in, then the returned port value
            is the same.

    Raises:
        Exception: An exception is raised if Redis could not be started.
    """
    assert os.path.isfile(executable)
    counter = 0

    while counter < num_retries:
        # Construct the command to start the Redis server.
        command = [executable]
        if password:
            if " " in password:
                raise ValueError("Spaces not permitted in redis password.")
            command += ["--requirepass", password]
        command += ["--port", str(port), "--loglevel", "warning"]
        if listen_to_localhost_only:
            command += ["--bind", "127.0.0.1"]
        pidfile = os.path.join(session_dir_path, "redis-" + uuid.uuid4().hex + ".pid")
        command += ["--pidfile", pidfile]
        process_info = start_ray_process(
            command,
            ray_constants.PROCESS_TYPE_REDIS_SERVER,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            fate_share=fate_share,
        )
        try:
            wait_for_redis_to_start("127.0.0.1", port, password=password)
        except (redis.exceptions.ResponseError, RuntimeError):
            # Connected to redis with the wrong password, or exceeded
            # the number of retries. This means we got the wrong redis
            # or there is some error in starting up redis.
            # Try the next port by looping again.
            pass
        else:
            r = redis.StrictRedis(host="127.0.0.1", port=port, password=password)
            # Check if Redis successfully started and we connected
            # to the right server.
            if r.config_get("pidfile")["pidfile"] == pidfile:
                break
        port = new_port(denylist=port_denylist)
        counter += 1
    if counter == num_retries:
        raise RuntimeError(
            "Couldn't start Redis. "
            "Check log files: {} {}".format(
                stdout_file.name if stdout_file is not None else "<stdout>",
                stderr_file.name if stdout_file is not None else "<stderr>",
            )
        )

    # Create a Redis client just for configuring Redis.
    redis_client = redis.StrictRedis(host="127.0.0.1", port=port, password=password)
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
        logger.debug(
            "Starting Redis shard with {} GB max memory.".format(
                round(redis_max_memory / 1e9, 2)
            )
        )

    # If redis_max_clients is provided, attempt to raise the number of maximum
    # number of Redis clients.
    if redis_max_clients is not None:
        redis_client.config_set("maxclients", str(redis_max_clients))
    elif resource is not None:
        # If redis_max_clients is not provided, determine the current ulimit.
        # We will use this to attempt to raise the maximum number of Redis
        # clients.
        current_max_clients = int(redis_client.config_get("maxclients")["maxclients"])
        # The below command should be the same as doing ulimit -n.
        ulimit_n = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        # The quantity redis_client_buffer appears to be the required buffer
        # between the maximum number of redis clients and ulimit -n. That is,
        # if ulimit -n returns 10000, then we can set maxclients to
        # 10000 - redis_client_buffer.
        redis_client_buffer = 32
        if current_max_clients < ulimit_n - redis_client_buffer:
            redis_client.config_set("maxclients", ulimit_n - redis_client_buffer)

    # Increase the hard and soft limits for the redis client pubsub buffer to
    # 128MB. This is a hack to make it less likely for pubsub messages to be
    # dropped and for pubsub connections to therefore be killed.
    cur_config = redis_client.config_get("client-output-buffer-limit")[
        "client-output-buffer-limit"
    ]
    cur_config_list = cur_config.split()
    assert len(cur_config_list) == 12
    cur_config_list[8:] = ["pubsub", "134217728", "134217728", "60"]
    redis_client.config_set("client-output-buffer-limit", " ".join(cur_config_list))
    # Put a time stamp in Redis to indicate when it was started.
    redis_client.set("redis_start_time", time.time())
    return port, process_info


def start_log_monitor(
    redis_address,
    gcs_address,
    logs_dir,
    redis_password=None,
    fate_share=None,
    max_bytes=0,
    backup_count=0,
    redirect_logging=True,
):
    """Start a log monitor process.

    Args:
        redis_address (str): The address of the Redis instance.
        logs_dir (str): The directory of logging files.
        redis_password (str): The password of the redis server.
        max_bytes (int): Log rotation parameter. Corresponding to
            RotatingFileHandler's maxBytes.
        backup_count (int): Log rotation parameter. Corresponding to
            RotatingFileHandler's backupCount.
        redirect_logging (bool): Whether we should redirect logging to
            the provided log directory.

    Returns:
        ProcessInfo for the process that was started.
    """
    log_monitor_filepath = os.path.join(RAY_PATH, RAY_PRIVATE_DIR, "log_monitor.py")

    command = [
        sys.executable,
        "-u",
        log_monitor_filepath,
        f"--redis-address={redis_address}",
        f"--logs-dir={logs_dir}",
        f"--logging-rotate-bytes={max_bytes}",
        f"--logging-rotate-backup-count={backup_count}",
        f"--gcs-address={gcs_address}",
    ]
    if redirect_logging:
        # Avoid hanging due to fd inheritance.
        stdout_file = subprocess.DEVNULL
        stderr_file = subprocess.DEVNULL
    else:
        # If not redirecting logging to files, unset log filename.
        # This will cause log records to go to stderr.
        command.append("--logging-filename=")
        # Use stderr log format with the component name as a message prefix.
        logging_format = ray_constants.LOGGER_FORMAT_STDERR.format(
            component=ray_constants.PROCESS_TYPE_LOG_MONITOR
        )
        command.append(f"--logging-format={logging_format}")
        # Inherit stdout/stderr streams.
        stdout_file = None
        stderr_file = None
    if redis_password:
        command.append(f"--redis-password={redis_password}")
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_LOG_MONITOR,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=fate_share,
    )
    return process_info


def start_dashboard(
    require_dashboard,
    host,
    redis_address,
    gcs_address,
    temp_dir,
    logdir,
    port=None,
    redis_password=None,
    fate_share=None,
    max_bytes=0,
    backup_count=0,
    redirect_logging=True,
):
    """Start a dashboard process.

    Args:
        require_dashboard (bool): If true, this will raise an exception if we
            fail to start the dashboard. Otherwise it will print a warning if
            we fail to start the dashboard.
        host (str): The host to bind the dashboard web server to.
        redis_address (str): The address of the Redis instance.
        gcs_address (str): The gcs address the dashboard should connect to
        temp_dir (str): The temporary directory used for log files and
            information for this Ray session.
        logdir (str): The log directory used to generate dashboard log.
        port (str): The port to bind the dashboard web server to.
            Defaults to 8265.
        redis_password (str): The password of the redis server.
        max_bytes (int): Log rotation parameter. Corresponding to
            RotatingFileHandler's maxBytes.
        backup_count (int): Log rotation parameter. Corresponding to
            RotatingFileHandler's backupCount.
        redirect_logging (bool): Whether we should redirect logging to
            the provided log directory.

    Returns:
        ProcessInfo for the process that was started.
    """
    try:
        # Make sure port is available.
        if port is None:
            port_retries = 50
            port = ray_constants.DEFAULT_DASHBOARD_PORT
        else:
            port_retries = 0
            port_test_socket = socket.socket()
            port_test_socket.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_REUSEADDR,
                1,
            )
            try:
                port_test_socket.bind((host, port))
                port_test_socket.close()
            except socket.error as e:
                # 10013 on windows is a bit more broad than just
                # "address in use": it can also indicate "permission denied".
                # TODO: improve the error message?
                if e.errno in {48, 98, 10013}:  # address already in use.
                    raise ValueError(
                        f"Failed to bind to {host}:{port} because it's "
                        "already occupied. You can use `ray start "
                        "--dashboard-port ...` or `ray.init(dashboard_port=..."
                        ")` to select a different port."
                    )
                else:
                    raise e

        # Make sure the process can start.
        minimal = not ray._private.utils.check_dashboard_dependencies_installed()

        # Start the dashboard process.
        dashboard_dir = "dashboard"
        dashboard_filepath = os.path.join(RAY_PATH, dashboard_dir, "dashboard.py")

        command = [
            sys.executable,
            "-u",
            dashboard_filepath,
            f"--host={host}",
            f"--port={port}",
            f"--port-retries={port_retries}",
            f"--redis-address={redis_address}",
            f"--temp-dir={temp_dir}",
            f"--log-dir={logdir}",
            f"--logging-rotate-bytes={max_bytes}",
            f"--logging-rotate-backup-count={backup_count}",
            f"--gcs-address={gcs_address}",
        ]

        if redirect_logging:
            # Avoid hanging due to fd inheritance.
            stdout_file = subprocess.DEVNULL
            stderr_file = subprocess.DEVNULL
        else:
            # If not redirecting logging to files, unset log filename.
            # This will cause log records to go to stderr.
            command.append("--logging-filename=")
            # Use stderr log format with the component name as a message prefix.
            logging_format = ray_constants.LOGGER_FORMAT_STDERR.format(
                component=ray_constants.PROCESS_TYPE_DASHBOARD
            )
            command.append(f"--logging-format={logging_format}")
            # Inherit stdout/stderr streams.
            stdout_file = None
            stderr_file = None
        if minimal:
            command.append("--minimal")

        if redis_password is not None:
            command.append(f"--redis-password={redis_password}")
        process_info = start_ray_process(
            command,
            ray_constants.PROCESS_TYPE_DASHBOARD,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            fate_share=fate_share,
        )

        # Retrieve the dashboard url
        if not use_gcs_for_bootstrap():
            redis_client = ray._private.services.create_redis_client(
                redis_address, redis_password
            )
            gcs_client = GcsClient.create_from_redis(redis_client)
        else:
            gcs_client = GcsClient(address=gcs_address)
        ray.experimental.internal_kv._initialize_internal_kv(gcs_client)
        dashboard_url = None
        dashboard_returncode = None
        for _ in range(200):
            dashboard_url = ray.experimental.internal_kv._internal_kv_get(
                ray_constants.DASHBOARD_ADDRESS,
                namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
            )
            if dashboard_url is not None:
                dashboard_url = dashboard_url.decode("utf-8")
                break
            dashboard_returncode = process_info.process.poll()
            if dashboard_returncode is not None:
                break
            # This is often on the critical path of ray.init() and ray start,
            # so we need to poll often.
            time.sleep(0.1)
        if dashboard_url is None:
            returncode_str = (
                f", return code {dashboard_returncode}"
                if dashboard_returncode is not None
                else ""
            )
            err_msg = "Failed to start the dashboard" + returncode_str
            if logdir:
                dashboard_log = os.path.join(logdir, "dashboard.log")
                # Read last n lines of dashboard log. The log file may be large.
                n = 10
                lines = []
                try:
                    with open(dashboard_log, "rb") as f:
                        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                            end = mm.size()
                            for _ in range(n):
                                sep = mm.rfind(b"\n", 0, end - 1)
                                if sep == -1:
                                    break
                                lines.append(mm[sep + 1 : end].decode("utf-8"))
                                end = sep
                    lines.append(f" The last {n} lines of {dashboard_log}:")
                except Exception as e:
                    raise Exception(err_msg + f"\nFailed to read dashboard log: {e}")

                last_log_str = "\n" + "\n".join(reversed(lines[-n:]))
                raise Exception(err_msg + last_log_str)
            else:
                raise Exception(err_msg)

        if not minimal:
            logger.info(
                "View the Ray dashboard at %s%shttp://%s%s%s",
                colorama.Style.BRIGHT,
                colorama.Fore.GREEN,
                dashboard_url,
                colorama.Fore.RESET,
                colorama.Style.NORMAL,
            )
        else:
            # If it is the minimal installation, the web url (dashboard url)
            # shouldn't be configured because it doesn't start a server.
            dashboard_url = ""

        return dashboard_url, process_info
    except Exception as e:
        if require_dashboard:
            raise e from e
        else:
            logger.error(f"Failed to start the dashboard: {e}")
            logger.exception(e)
            return None, None


def start_gcs_server(
    redis_address,
    log_dir,
    stdout_file=None,
    stderr_file=None,
    redis_password=None,
    config=None,
    fate_share=None,
    gcs_server_port=None,
    metrics_agent_port=None,
    node_ip_address=None,
):
    """Start a gcs server.
    Args:
        redis_address (str): The address that the Redis server is listening on.
        log_dir (str): The path of the dir where log files are created.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        redis_password (str): The password of the redis server.
        config (dict|None): Optional configuration that will
            override defaults in RayConfig.
        gcs_server_port (int): Port number of the gcs server.
        metrics_agent_port(int): The port where metrics agent is bound to.
        node_ip_address(str): IP Address of a node where gcs server starts.
    Returns:
        ProcessInfo for the process that was started.
    """
    assert gcs_server_port > 0

    command = [
        GCS_SERVER_EXECUTABLE,
        f"--log_dir={log_dir}",
        f"--config_list={serialize_config(config)}",
        f"--gcs_server_port={gcs_server_port}",
        f"--metrics-agent-port={metrics_agent_port}",
        f"--node-ip-address={node_ip_address}",
    ]
    if redis_address:
        redis_ip_address, redis_port = redis_address.rsplit(":")
        command += [
            f"--redis_address={redis_ip_address}",
            f"--redis_port={redis_port}",
        ]
    if redis_password:
        command += [f"--redis_password={redis_password}"]
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_GCS_SERVER,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=fate_share,
    )
    return process_info


def start_raylet(
    redis_address,
    gcs_address,
    node_ip_address,
    node_manager_port,
    raylet_name,
    plasma_store_name,
    worker_path,
    setup_worker_path,
    temp_dir,
    session_dir,
    resource_dir,
    log_dir,
    resource_spec,
    plasma_directory,
    object_store_memory,
    min_worker_port=None,
    max_worker_port=None,
    worker_port_list=None,
    object_manager_port=None,
    redis_password=None,
    metrics_agent_port=None,
    metrics_export_port=None,
    dashboard_agent_listen_port=None,
    use_valgrind=False,
    use_profiler=False,
    stdout_file=None,
    stderr_file=None,
    config=None,
    huge_pages=False,
    fate_share=None,
    socket_to_use=None,
    start_initial_python_workers_for_first_job=False,
    max_bytes=0,
    backup_count=0,
    ray_debugger_external=False,
    env_updates=None,
):
    """Start a raylet, which is a combined local scheduler and object manager.

    Args:
        redis_address (str): The address of the primary Redis server.
        gcs_address (str): The address of GCS server.
        node_ip_address (str): The IP address of this node.
        node_manager_port(int): The port to use for the node manager. If it's
            0, a random port will be used.
        raylet_name (str): The name of the raylet socket to create.
        plasma_store_name (str): The name of the plasma store socket to connect
             to.
        worker_path (str): The path of the Python file that new worker
            processes will execute.
        setup_worker_path (str): The path of the Python file that will set up
            the environment for the worker process.
        temp_dir (str): The path of the temporary directory Ray will use.
        session_dir (str): The path of this session.
        resource_dir(str): The path of resource of this session .
        log_dir (str): The path of the dir where log files are created.
        resource_spec (ResourceSpec): Resources for this raylet.
        object_manager_port: The port to use for the object manager. If this is
            None, then the object manager will choose its own port.
        min_worker_port (int): The lowest port number that workers will bind
            on. If not set, random ports will be chosen.
        max_worker_port (int): The highest port number that workers will bind
            on. If set, min_worker_port must also be set.
        redis_password: The password to use when connecting to Redis.
        metrics_agent_port(int): The port where metrics agent is bound to.
        metrics_export_port(int): The port at which metrics are exposed to.
        use_valgrind (bool): True if the raylet should be started inside
            of valgrind. If this is True, use_profiler must be False.
        use_profiler (bool): True if the raylet should be started inside
            a profiler. If this is True, use_valgrind must be False.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        tracing_startup_hook: Tracing startup hook.
        config (dict|None): Optional Raylet configuration that will
            override defaults in RayConfig.
        max_bytes (int): Log rotation parameter. Corresponding to
            RotatingFileHandler's maxBytes.
        backup_count (int): Log rotation parameter. Corresponding to
            RotatingFileHandler's backupCount.
        ray_debugger_external (bool): True if the Ray debugger should be made
            available externally to this node.
        env_updates (dict): Environment variable overrides.

    Returns:
        ProcessInfo for the process that was started.
    """
    assert node_manager_port is not None and type(node_manager_port) == int

    if use_valgrind and use_profiler:
        raise ValueError("Cannot use valgrind and profiler at the same time.")

    assert resource_spec.resolved()
    static_resources = resource_spec.to_resource_dict()

    # Limit the number of workers that can be started in parallel by the
    # raylet. However, make sure it is at least 1.
    num_cpus_static = static_resources.get("CPU", 0)
    maximum_startup_concurrency = max(
        1, min(multiprocessing.cpu_count(), num_cpus_static)
    )

    # Format the resource argument in a form like 'CPU,1.0,GPU,0,Custom,3'.
    resource_argument = ",".join(
        ["{},{}".format(*kv) for kv in static_resources.items()]
    )

    has_java_command = False
    if shutil.which("java") is not None:
        has_java_command = True

    ray_java_installed = False
    try:
        jars_dir = get_ray_jars_dir()
        if os.path.exists(jars_dir):
            ray_java_installed = True
    except Exception:
        pass

    include_java = has_java_command and ray_java_installed
    if include_java is True:
        java_worker_command = build_java_worker_command(
            gcs_address if use_gcs_for_bootstrap() else redis_address,
            plasma_store_name,
            raylet_name,
            redis_password,
            session_dir,
            node_ip_address,
            setup_worker_path,
        )
    else:
        java_worker_command = []

    if os.path.exists(DEFAULT_WORKER_EXECUTABLE):
        cpp_worker_command = build_cpp_worker_command(
            "",
            gcs_address if use_gcs_for_bootstrap() else redis_address,
            plasma_store_name,
            raylet_name,
            redis_password,
            session_dir,
            log_dir,
            node_ip_address,
        )
    else:
        cpp_worker_command = []

    # Create the command that the Raylet will use to start workers.
    # TODO(architkulkarni): Pipe in setup worker args separately instead of
    # inserting them into start_worker_command and later erasing them if
    # needed.
    start_worker_command = [
        sys.executable,
        setup_worker_path,
        worker_path,
        f"--node-ip-address={node_ip_address}",
        "--node-manager-port=RAY_NODE_MANAGER_PORT_PLACEHOLDER",
        f"--object-store-name={plasma_store_name}",
        f"--raylet-name={raylet_name}",
        f"--redis-address={redis_address}",
        f"--temp-dir={temp_dir}",
        f"--metrics-agent-port={metrics_agent_port}",
        f"--logging-rotate-bytes={max_bytes}",
        f"--logging-rotate-backup-count={backup_count}",
        f"--gcs-address={gcs_address}",
        "RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER",
    ]

    if redis_password:
        start_worker_command += [f"--redis-password={redis_password}"]

    # If the object manager port is None, then use 0 to cause the object
    # manager to choose its own port.
    if object_manager_port is None:
        object_manager_port = 0

    if min_worker_port is None:
        min_worker_port = 0

    if max_worker_port is None:
        max_worker_port = 0

    agent_command = [
        sys.executable,
        "-u",
        os.path.join(RAY_PATH, "dashboard", "agent.py"),
        f"--node-ip-address={node_ip_address}",
        f"--redis-address={redis_address}",
        f"--metrics-export-port={metrics_export_port}",
        f"--dashboard-agent-port={metrics_agent_port}",
        f"--listen-port={dashboard_agent_listen_port}",
        "--node-manager-port=RAY_NODE_MANAGER_PORT_PLACEHOLDER",
        f"--object-store-name={plasma_store_name}",
        f"--raylet-name={raylet_name}",
        f"--temp-dir={temp_dir}",
        f"--session-dir={session_dir}",
        f"--runtime-env-dir={resource_dir}",
        f"--log-dir={log_dir}",
        f"--logging-rotate-bytes={max_bytes}",
        f"--logging-rotate-backup-count={backup_count}",
        f"--gcs-address={gcs_address}",
    ]
    if stdout_file is None and stderr_file is None:
        # If not redirecting logging to files, unset log filename.
        # This will cause log records to go to stderr.
        agent_command.append("--logging-filename=")
        # Use stderr log format with the component name as a message prefix.
        logging_format = ray_constants.LOGGER_FORMAT_STDERR.format(
            component=ray_constants.PROCESS_TYPE_DASHBOARD_AGENT
        )
        agent_command.append(f"--logging-format={logging_format}")

    if redis_password is not None and len(redis_password) != 0:
        agent_command.append("--redis-password={}".format(redis_password))

    if not ray._private.utils.check_dashboard_dependencies_installed():
        # If dependencies are not installed, it is the minimally packaged
        # ray. We should restrict the features within dashboard agent
        # that requires additional dependencies to be downloaded.
        agent_command.append("--minimal")

    command = [
        RAYLET_EXECUTABLE,
        f"--raylet_socket_name={raylet_name}",
        f"--store_socket_name={plasma_store_name}",
        f"--object_manager_port={object_manager_port}",
        f"--min_worker_port={min_worker_port}",
        f"--max_worker_port={max_worker_port}",
        f"--node_manager_port={node_manager_port}",
        f"--node_ip_address={node_ip_address}",
        f"--maximum_startup_concurrency={maximum_startup_concurrency}",
        f"--static_resource_list={resource_argument}",
        f"--python_worker_command={subprocess.list2cmdline(start_worker_command)}",  # noqa
        f"--java_worker_command={subprocess.list2cmdline(java_worker_command)}",  # noqa
        f"--cpp_worker_command={subprocess.list2cmdline(cpp_worker_command)}",  # noqa
        f"--native_library_path={DEFAULT_NATIVE_LIBRARY_PATH}",
        f"--redis_password={redis_password or ''}",
        f"--temp_dir={temp_dir}",
        f"--session_dir={session_dir}",
        f"--log_dir={log_dir}",
        f"--resource_dir={resource_dir}",
        f"--metrics-agent-port={metrics_agent_port}",
        f"--metrics_export_port={metrics_export_port}",
        f"--object_store_memory={object_store_memory}",
        f"--plasma_directory={plasma_directory}",
        f"--ray-debugger-external={1 if ray_debugger_external else 0}",
    ]
    if use_gcs_for_bootstrap():
        command.append(f"--gcs-address={gcs_address}")
    else:
        # TODO (iycheng): remove redis_ip_address after redis removal
        redis_ip_address, redis_port = redis_address.split(":")
        command.extend(
            [f"--redis_address={redis_ip_address}", f"--redis_port={redis_port}"]
        )

    if worker_port_list is not None:
        command.append(f"--worker_port_list={worker_port_list}")
    if start_initial_python_workers_for_first_job:
        command.append(
            "--num_initial_python_workers_for_first_job={}".format(
                resource_spec.num_cpus
            )
        )
    command.append("--agent_command={}".format(subprocess.list2cmdline(agent_command)))
    if huge_pages:
        command.append("--huge_pages")
    if socket_to_use:
        socket_to_use.close()
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_RAYLET,
        use_valgrind=use_valgrind,
        use_gdb=False,
        use_valgrind_profiler=use_profiler,
        use_perftools_profiler=("RAYLET_PERFTOOLS_PATH" in os.environ),
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=fate_share,
        env_updates=env_updates,
    )

    return process_info


def get_ray_jars_dir():
    """Return a directory where all ray-related jars and
    their dependencies locate."""
    current_dir = RAY_PATH
    jars_dir = os.path.abspath(os.path.join(current_dir, "jars"))
    if not os.path.exists(jars_dir):
        raise RuntimeError(
            "Ray jars is not packaged into ray. "
            "Please build ray with java enabled "
            "(set env var RAY_INSTALL_JAVA=1)"
        )
    return os.path.abspath(os.path.join(current_dir, "jars"))


def build_java_worker_command(
    bootstrap_address,
    plasma_store_name,
    raylet_name,
    redis_password,
    session_dir,
    node_ip_address,
    setup_worker_path,
):
    """This method assembles the command used to start a Java worker.

    Args:
        bootstrap_address (str): Bootstrap address of ray cluster.
        plasma_store_name (str): The name of the plasma store socket to connect
           to.
        raylet_name (str): The name of the raylet socket to create.
        redis_password (str): The password of connect to redis.
        session_dir (str): The path of this session.
        node_ip_address (str): The ip address for this node.
        setup_worker_path (str): The path of the Python file that will set up
            the environment for the worker process.
    Returns:
        The command string for starting Java worker.
    """
    pairs = []
    if bootstrap_address is not None:
        pairs.append(("ray.address", bootstrap_address))
    pairs.append(("ray.raylet.node-manager-port", "RAY_NODE_MANAGER_PORT_PLACEHOLDER"))

    if plasma_store_name is not None:
        pairs.append(("ray.object-store.socket-name", plasma_store_name))

    if raylet_name is not None:
        pairs.append(("ray.raylet.socket-name", raylet_name))

    if redis_password is not None:
        pairs.append(("ray.redis.password", redis_password))

    if node_ip_address is not None:
        pairs.append(("ray.node-ip", node_ip_address))

    pairs.append(("ray.home", RAY_HOME))
    pairs.append(("ray.logging.dir", os.path.join(session_dir, "logs")))
    pairs.append(("ray.session-dir", session_dir))
    command = (
        [sys.executable]
        + [setup_worker_path]
        + ["java"]
        + ["-D{}={}".format(*pair) for pair in pairs]
    )

    # Add ray jars path to java classpath
    ray_jars = os.path.join(get_ray_jars_dir(), "*")
    command += ["-cp", ray_jars]

    command += ["RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER"]
    command += ["io.ray.runtime.runner.worker.DefaultWorker"]

    return command


def build_cpp_worker_command(
    cpp_worker_options,
    bootstrap_address,
    plasma_store_name,
    raylet_name,
    redis_password,
    session_dir,
    log_dir,
    node_ip_address,
):
    """This method assembles the command used to start a CPP worker.

    Args:
        cpp_worker_options (list): The command options for CPP worker.
        bootstrap_address (str): The bootstrap address of the cluster.
        plasma_store_name (str): The name of the plasma store socket to connect
           to.
        raylet_name (str): The name of the raylet socket to create.
        redis_password (str): The password of connect to redis.
        session_dir (str): The path of this session.
        log_dir (str): The path of logs.
        node_ip_address (str): The ip address for this node.
    Returns:
        The command string for starting CPP worker.
    """

    command = [
        DEFAULT_WORKER_EXECUTABLE,
        f"--ray_plasma_store_socket_name={plasma_store_name}",
        f"--ray_raylet_socket_name={raylet_name}",
        "--ray_node_manager_port=RAY_NODE_MANAGER_PORT_PLACEHOLDER",
        f"--ray_address={bootstrap_address}",
        f"--ray_redis_password={redis_password}",
        f"--ray_session_dir={session_dir}",
        f"--ray_logs_dir={log_dir}",
        f"--ray_node_ip_address={node_ip_address}",
        "RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER",
    ]

    return command


def determine_plasma_store_config(
    object_store_memory, plasma_directory=None, huge_pages=False
):
    """Figure out how to configure the plasma object store.

    This will determine which directory to use for the plasma store. On Linux,
    we will try to use /dev/shm unless the shared memory file system is too
    small, in which case we will fall back to /tmp. If any of the object store
    memory or plasma directory parameters are specified by the user, then those
    values will be preserved.

    Args:
        object_store_memory (int): The object store memory to use.
        plasma_directory (str): The user-specified plasma directory parameter.
        huge_pages (bool): The user-specified huge pages parameter.

    Returns:
        The plasma directory to use. If it is specified by the user, then that
            value will be preserved.
    """
    if not isinstance(object_store_memory, int):
        object_store_memory = int(object_store_memory)

    if huge_pages and not (sys.platform == "linux" or sys.platform == "linux2"):
        raise ValueError("The huge_pages argument is only supported on " "Linux.")

    system_memory = ray._private.utils.get_system_memory()

    # Determine which directory to use. By default, use /tmp on MacOS and
    # /dev/shm on Linux, unless the shared-memory file system is too small,
    # in which case we default to /tmp on Linux.
    if plasma_directory is None:
        if sys.platform == "linux" or sys.platform == "linux2":
            shm_avail = ray._private.utils.get_shared_memory_bytes()
            # Compare the requested memory size to the memory available in
            # /dev/shm.
            if shm_avail > object_store_memory:
                plasma_directory = "/dev/shm"
            elif (
                not os.environ.get("RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE")
                and object_store_memory > ray_constants.REQUIRE_SHM_SIZE_THRESHOLD
            ):
                raise ValueError(
                    "The configured object store size ({} GB) exceeds "
                    "/dev/shm size ({} GB). This will harm performance. "
                    "Consider deleting files in /dev/shm or increasing its "
                    "size with "
                    "--shm-size in Docker. To ignore this warning, "
                    "set RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1.".format(
                        object_store_memory / 1e9, shm_avail / 1e9
                    )
                )
            else:
                plasma_directory = ray._private.utils.get_user_temp_dir()
                logger.warning(
                    "WARNING: The object store is using {} instead of "
                    "/dev/shm because /dev/shm has only {} bytes available. "
                    "This will harm performance! You may be able to free up "
                    "space by deleting files in /dev/shm. If you are inside a "
                    "Docker container, you can increase /dev/shm size by "
                    "passing '--shm-size={:.2f}gb' to 'docker run' (or add it "
                    "to the run_options list in a Ray cluster config). Make "
                    "sure to set this to more than 30% of available RAM.".format(
                        ray._private.utils.get_user_temp_dir(),
                        shm_avail,
                        object_store_memory * (1.1) / (2 ** 30),
                    )
                )
        else:
            plasma_directory = ray._private.utils.get_user_temp_dir()

        # Do some sanity checks.
        if object_store_memory > system_memory:
            raise ValueError(
                "The requested object store memory size is greater "
                "than the total available memory."
            )
    else:
        plasma_directory = os.path.abspath(plasma_directory)
        logger.info(
            "object_store_memory is not verified when " "plasma_directory is set."
        )

    if not os.path.isdir(plasma_directory):
        raise ValueError(
            f"The file {plasma_directory} does not " "exist or is not a directory."
        )

    if huge_pages and plasma_directory is None:
        raise ValueError(
            "If huge_pages is True, then the "
            "plasma_directory argument must be provided."
        )

    if object_store_memory < ray_constants.OBJECT_STORE_MINIMUM_MEMORY_BYTES:
        raise ValueError(
            "Attempting to cap object store memory usage at {} "
            "bytes, but the minimum allowed is {} bytes.".format(
                object_store_memory, ray_constants.OBJECT_STORE_MINIMUM_MEMORY_BYTES
            )
        )

    if (
        sys.platform == "darwin"
        and object_store_memory > ray_constants.MAC_DEGRADED_PERF_MMAP_SIZE_LIMIT
        and os.environ.get("RAY_ENABLE_MAC_LARGE_OBJECT_STORE") != "1"
    ):
        raise ValueError(
            "The configured object store size ({:.4}GiB) exceeds "
            "the optimal size on Mac ({:.4}GiB). "
            "This will harm performance! There is a known issue where "
            "Ray's performance degrades with object store size greater"
            " than {:.4}GB on a Mac."
            "To reduce the object store capacity, specify"
            "`object_store_memory` when calling ray.init() or ray start."
            "To ignore this warning, "
            "set RAY_ENABLE_MAC_LARGE_OBJECT_STORE=1.".format(
                object_store_memory / 2 ** 30,
                ray_constants.MAC_DEGRADED_PERF_MMAP_SIZE_LIMIT / 2 ** 30,
                ray_constants.MAC_DEGRADED_PERF_MMAP_SIZE_LIMIT / 2 ** 30,
            )
        )

    # Print the object store memory using two decimal places.
    logger.debug(
        "Determine to start the Plasma object store with {} GB memory "
        "using {}.".format(round(object_store_memory / 10 ** 9, 2), plasma_directory)
    )

    return plasma_directory, object_store_memory


def start_worker(
    node_ip_address,
    object_store_name,
    raylet_name,
    redis_address,
    worker_path,
    temp_dir,
    raylet_ip_address=None,
    stdout_file=None,
    stderr_file=None,
    fate_share=None,
):
    """This method starts a worker process.

    Args:
        node_ip_address (str): The IP address of the node that this worker is
            running on.
        object_store_name (str): The socket name of the object store.
        raylet_name (str): The socket name of the raylet server.
        redis_address (str): The address that the Redis server is listening on.
        worker_path (str): The path of the source code which the worker process
            will run.
        temp_dir (str): The path of the temp dir.
        raylet_ip_address (str): The IP address of the worker's raylet. If not
            provided, it defaults to the node_ip_address.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.

    Returns:
        ProcessInfo for the process that was started.
    """
    command = [
        sys.executable,
        "-u",
        worker_path,
        "--node-ip-address=" + node_ip_address,
        "--object-store-name=" + object_store_name,
        "--raylet-name=" + raylet_name,
        "--redis-address=" + str(redis_address),
        "--temp-dir=" + temp_dir,
    ]
    if raylet_ip_address is not None:
        command.append("--raylet-ip-address=" + raylet_ip_address)
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_WORKER,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=fate_share,
    )
    return process_info


def start_monitor(
    redis_address,
    gcs_address,
    logs_dir,
    stdout_file=None,
    stderr_file=None,
    autoscaling_config=None,
    redis_password=None,
    fate_share=None,
    max_bytes=0,
    backup_count=0,
    monitor_ip=None,
):
    """Run a process to monitor the other processes.

    Args:
        redis_address (str): The address that the Redis server is listening on.
        gcs_address (str): The address of GCS server.
        logs_dir(str): The path to the log directory.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        autoscaling_config: path to autoscaling config file.
        redis_password (str): The password of the redis server.
        max_bytes (int): Log rotation parameter. Corresponding to
            RotatingFileHandler's maxBytes.
        backup_count (int): Log rotation parameter. Corresponding to
            RotatingFileHandler's backupCount.
        monitor_ip (str): IP address of the machine that the monitor will be
            run on. Can be excluded, but required for autoscaler metrics.
    Returns:
        ProcessInfo for the process that was started.
    """
    monitor_path = os.path.join(RAY_PATH, AUTOSCALER_PRIVATE_DIR, "monitor.py")

    command = [
        sys.executable,
        "-u",
        monitor_path,
        f"--logs-dir={logs_dir}",
        f"--redis-address={redis_address}",
        f"--logging-rotate-bytes={max_bytes}",
        f"--logging-rotate-backup-count={backup_count}",
        f"--gcs-address={gcs_address}",
    ]

    if stdout_file is None and stderr_file is None:
        # If not redirecting logging to files, unset log filename.
        # This will cause log records to go to stderr.
        command.append("--logging-filename=")
        # Use stderr log format with the component name as a message prefix.
        logging_format = ray_constants.LOGGER_FORMAT_STDERR.format(
            component=ray_constants.PROCESS_TYPE_MONITOR
        )
        command.append(f"--logging-format={logging_format}")
    if autoscaling_config:
        command.append("--autoscaling-config=" + str(autoscaling_config))
    if redis_password:
        command.append("--redis-password=" + redis_password)
    if monitor_ip:
        command.append("--monitor-ip=" + monitor_ip)
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_MONITOR,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=fate_share,
    )
    return process_info


def start_ray_client_server(
    address,
    ray_client_server_ip,
    ray_client_server_port,
    stdout_file=None,
    stderr_file=None,
    redis_password=None,
    fate_share=None,
    metrics_agent_port=None,
    server_type: str = "proxy",
    serialized_runtime_env_context: Optional[str] = None,
):
    """Run the server process of the Ray client.

    Args:
        address: The address of the cluster.
        ray_client_server_ip: Host IP the Ray client server listens on.
        ray_client_server_port (int): Port the Ray client server listens on.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        redis_password (str): The password of the redis server.
        server_type (str): Whether to start the proxy version of Ray Client.
        serialized_runtime_env_context (str|None): If specified, the serialized
            runtime_env_context to start the client server in.

    Returns:
        ProcessInfo for the process that was started.
    """
    root_ray_dir = Path(__file__).resolve().parents[1]
    setup_worker_path = os.path.join(
        root_ray_dir, "workers", ray_constants.SETUP_WORKER_FILENAME
    )

    ray_client_server_host = (
        "127.0.0.1" if ray_client_server_ip == "127.0.0.1" else "0.0.0.0"
    )
    command = [
        sys.executable,
        setup_worker_path,
        "-m",
        "ray.util.client.server",
        f"--address={address}",
        f"--host={ray_client_server_host}",
        f"--port={ray_client_server_port}",
        f"--mode={server_type}",
        f"--language={Language.Name(Language.PYTHON)}",
    ]
    if redis_password:
        command.append(f"--redis-password={redis_password}")
    if serialized_runtime_env_context:
        command.append(
            f"--serialized-runtime-env-context={serialized_runtime_env_context}"  # noqa: E501
        )
    if metrics_agent_port:
        command.append(f"--metrics-agent-port={metrics_agent_port}")
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_RAY_CLIENT_SERVER,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=fate_share,
    )
    return process_info
