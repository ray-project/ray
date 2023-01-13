import base64
import collections
import errno
import io
import json
import logging
import mmap
import multiprocessing
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional, IO, AnyStr

# Import psutil after ray so the packaged version is used.
import psutil

# Ray modules
import ray
import ray._private.ray_constants as ray_constants
from ray._private.gcs_utils import GcsClient
from ray._raylet import GcsClientOptions
from ray.core.generated.common_pb2 import Language

resource = None
if sys.platform != "win32":
    _timeout = 30
else:
    _timeout = 60

EXE_SUFFIX = ".exe" if sys.platform == "win32" else ""

# True if processes are run in the valgrind profiler.
RUN_RAYLET_PROFILER = False

# Location of the redis server.
RAY_HOME = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "..")
RAY_PATH = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
RAY_PRIVATE_DIR = "_private"
AUTOSCALER_PRIVATE_DIR = os.path.join("autoscaler", "_private")

# Location of the raylet executables.
RAYLET_EXECUTABLE = os.path.join(
    RAY_PATH, "core", "src", "ray", "raylet", "raylet" + EXE_SUFFIX
)
GCS_SERVER_EXECUTABLE = os.path.join(
    RAY_PATH, "core", "src", "ray", "gcs", "gcs_server" + EXE_SUFFIX
)

# Location of the cpp default worker executables.
DEFAULT_WORKER_EXECUTABLE = os.path.join(RAY_PATH, "cpp", "default_worker" + EXE_SUFFIX)

# Location of the native libraries.
DEFAULT_NATIVE_LIBRARY_PATH = os.path.join(RAY_PATH, "cpp", "lib")

DASHBOARD_DEPENDENCY_ERROR_MESSAGE = (
    "Not all Ray Dashboard dependencies were "
    "found. To use the dashboard please "
    "install Ray using `pip install "
    "ray[default]`."
)

RAY_JEMALLOC_LIB_PATH = "RAY_JEMALLOC_LIB_PATH"
RAY_JEMALLOC_CONF = "RAY_JEMALLOC_CONF"
RAY_JEMALLOC_PROFILE = "RAY_JEMALLOC_PROFILE"

# Comma separated name of components that will run memory profiler.
# Ray uses `memray` to memory profile internal components.
# The name of the component must be one of ray_constants.PROCESS_TYPE*.
RAY_MEMRAY_PROFILE_COMPONENT_ENV = "RAY_INTERNAL_MEM_PROFILE_COMPONENTS"
# Options to specify for `memray run` command. See
# `memray run --help` for more details.
# Example:
# RAY_INTERNAL_MEM_PROFILE_OPTIONS="--live,--live-port,3456,-q,"
# -> `memray run --live --live-port 3456 -q`
RAY_MEMRAY_PROFILE_OPTIONS_ENV = "RAY_INTERNAL_MEM_PROFILE_OPTIONS"

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


def _build_python_executable_command_memory_profileable(
    component: str, session_dir: str, unbuffered: bool = True
):
    """Build the Python executable command.

    It runs a memory profiler if env var is configured.

    Args:
        component: Name of the component. It must be one of
            ray_constants.PROCESS_TYPE*.
        session_dir: The directory name of the Ray session.
        unbuffered: If true, Python executable is started with unbuffered option.
            e.g., `-u`.
            It means the logs are flushed immediately (good when there's a failure),
            but writing to a log file can be slower.
    """
    command = [
        sys.executable,
    ]
    if unbuffered:
        command.append("-u")
    components_to_memory_profile = os.getenv(RAY_MEMRAY_PROFILE_COMPONENT_ENV, "")
    if not components_to_memory_profile:
        return command

    components_to_memory_profile = set(components_to_memory_profile.split(","))
    try:
        import memray  # noqa: F401
    except ImportError:
        raise ImportError(
            "Memray is required to memory profiler on components "
            f"{components_to_memory_profile}. Run `pip install memray`."
        )
    if component in components_to_memory_profile:
        session_dir = Path(session_dir)
        session_name = session_dir.name
        profile_dir = session_dir / "profile"
        profile_dir.mkdir(exist_ok=True)
        output_file_path = profile_dir / f"{session_name}_memory_{component}.bin"
        options = os.getenv(RAY_MEMRAY_PROFILE_OPTIONS_ENV, None)
        options = options.split(",") if options else []
        command.extend(["-m", "memray", "run", "-o", str(output_file_path), *options])

    return command


def _get_gcs_client_options(gcs_server_address):
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
        jemalloc_path: The path to the jemalloc shared library.
        jemalloc_conf: `,` separated string of jemalloc config.
        jemalloc_comps: The list of Ray components
            that we will profile.
        process_type: The process type that needs jemalloc
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


def find_gcs_addresses():
    """Finds any local GCS processes based on grepping ps."""
    return _find_address_from_flag("--gcs-address")


def find_bootstrap_address(temp_dir: Optional[str]):
    """Finds the latest Ray cluster address to connect to, if any. This is the
    GCS address connected to by the last successful `ray start`."""
    return ray._private.utils.read_ray_address(temp_dir)


def get_ray_address_from_environment(addr: str, temp_dir: Optional[str]):
    """Attempts to find the address of Ray cluster to use, in this order:

    1. Use RAY_ADDRESS if defined and nonempty.
    2. If no address is provided or the provided address is "auto", use the
    address in /tmp/ray/ray_current_cluster if available. This will error if
    the specified address is None and there is no address found. For "auto",
    we will fallback to connecting to any detected Ray cluster (legacy).
    3. Otherwise, use the provided address.

    Returns:
        A string to pass into `ray.init(address=...)`, e.g. ip:port, `auto`.
    """
    env_addr = os.environ.get(ray_constants.RAY_ADDRESS_ENVIRONMENT_VARIABLE)
    if env_addr is not None and env_addr != "":
        addr = env_addr

    if addr is not None and addr != "auto":
        return addr
    # We should try to automatically find an active local instance.
    gcs_addrs = find_gcs_addresses()
    bootstrap_addr = find_bootstrap_address(temp_dir)

    if len(gcs_addrs) > 1 and bootstrap_addr is not None:
        logger.warning(
            f"Found multiple active Ray instances: {gcs_addrs}. "
            f"Connecting to latest cluster at {bootstrap_addr}. "
            "You can override this by setting the `--address` flag "
            "or `RAY_ADDRESS` environment variable."
        )
    elif len(gcs_addrs) > 0 and addr == "auto":
        # Preserve legacy "auto" behavior of connecting to any cluster, even if not
        # started with ray start. However if addr is None, we will raise an error.
        bootstrap_addr = list(gcs_addrs).pop()

    if bootstrap_addr is None:
        if addr is None:
            # Caller should start a new instance.
            return None
        else:
            raise ConnectionError(
                "Could not find any running Ray instance. "
                "Please specify the one to connect to by setting `--address` flag "
                "or `RAY_ADDRESS` environment variable."
            )

    return bootstrap_addr


def wait_for_node(
    gcs_address: str,
    node_plasma_store_socket_name: str,
    timeout: int = _timeout,
):
    """Wait until this node has appeared in the client table.

    Args:
        gcs_address: The gcs address
        node_plasma_store_socket_name: The
            plasma_store_socket_name for the given node which we wait for.
        timeout: The amount of time in seconds to wait before raising an
            exception.

    Raises:
        TimeoutError: An exception is raised if the timeout expires before
            the node appears in the client table.
    """
    gcs_options = GcsClientOptions.from_gcs_address(gcs_address)
    global_state = ray._private.state.GlobalState()
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


def get_node_to_connect_for_driver(gcs_address, node_ip_address):
    # Get node table from global state accessor.
    global_state = ray._private.state.GlobalState()
    gcs_options = _get_gcs_client_options(gcs_address)
    global_state._initialize_global_state(gcs_options)
    return global_state.get_node_to_connect_for_driver(node_ip_address)


def get_webui_url_from_internal_kv():
    assert ray.experimental.internal_kv._internal_kv_initialized()
    webui_url = ray.experimental.internal_kv._internal_kv_get(
        "webui:url", namespace=ray_constants.KV_NAMESPACE_DASHBOARD
    )
    return ray._private.utils.decode(webui_url) if webui_url is not None else None


def get_storage_uri_from_internal_kv():
    assert ray.experimental.internal_kv._internal_kv_initialized()
    storage_uri = ray.experimental.internal_kv._internal_kv_get(
        "storage", namespace=ray_constants.KV_NAMESPACE_SESSION
    )
    return ray._private.utils.decode(storage_uri) if storage_uri is not None else None


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
    if ray._private.worker._global_node is None:
        raise RuntimeError(
            "This process is not in a position to determine "
            "whether all processes are alive or not."
        )
    return ray._private.worker._global_node.remaining_processes_alive()


def canonicalize_bootstrap_address(
    addr: str, temp_dir: Optional[str] = None
) -> Optional[str]:
    """Canonicalizes Ray cluster bootstrap address to host:port.
    Reads address from the environment if needed.

    This function should be used to process user supplied Ray cluster address,
    via ray.init() or `--address` flags, before using the address to connect.

    Returns:
        Ray cluster address string in <host:port> format or None if the caller
        should start a local Ray instance.
    """
    if addr is None or addr == "auto":
        addr = get_ray_address_from_environment(addr, temp_dir)
    if addr is None or addr == "local":
        return None
    try:
        bootstrap_address = resolve_ip_for_localhost(addr)
    except Exception:
        logger.exception(f"Failed to convert {addr} to host:port")
        raise
    return bootstrap_address


def canonicalize_bootstrap_address_or_die(
    addr: str, temp_dir: Optional[str] = None
) -> str:
    """Canonicalizes Ray cluster bootstrap address to host:port.

    This function should be used when the caller expects there to be an active
    and local Ray instance. If no address is provided or address="auto", this
    will autodetect the latest Ray instance created with `ray start`.

    For convenience, if no address can be autodetected, this function will also
    look for any running local GCS processes, based on pgrep output. This is to
    allow easier use of Ray CLIs when debugging a local Ray instance (whose GCS
    addresses are not recorded).

    Returns:
        Ray cluster address string in <host:port> format. Throws a
        ConnectionError if zero or multiple active Ray instances are
        autodetected.
    """
    bootstrap_addr = canonicalize_bootstrap_address(addr, temp_dir=temp_dir)
    if bootstrap_addr is not None:
        return bootstrap_addr

    running_gcs_addresses = find_gcs_addresses()
    if len(running_gcs_addresses) == 0:
        raise ConnectionError(
            "Could not find any running Ray instance. "
            "Please specify the one to connect to by setting the `--address` "
            "flag or `RAY_ADDRESS` environment variable."
        )
    if len(running_gcs_addresses) > 1:
        raise ConnectionError(
            f"Found multiple active Ray instances: {running_gcs_addresses}. "
            "Please specify the one to connect to by setting the `--address` "
            "flag or `RAY_ADDRESS` environment variable."
        )
    return running_gcs_addresses.pop()


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


def node_ip_address_from_perspective(address: str):
    """IP address by which the local node can be reached *from* the `address`.

    Args:
        address: The IP address and port of any known live service on the
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
    if ray._private.worker._global_node is not None:
        return ray._private.worker._global_node.node_ip_address
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
    import redis

    if not hasattr(create_redis_client, "instances"):
        create_redis_client.instances = {}

    num_retries = ray_constants.START_REDIS_WAIT_RETRIES
    delay = 0.001
    for i in range(num_retries):
        cli = create_redis_client.instances.get(redis_address)
        if cli is None:
            redis_ip_address, redis_port = extract_ip_port(
                canonicalize_bootstrap_address_or_die(redis_address)
            )
            cli = redis.StrictRedis(
                host=redis_ip_address, port=int(redis_port), password=password
            )
            create_redis_client.instances[redis_address] = cli
        try:
            cli.ping()
            return cli
        except Exception as e:
            create_redis_client.instances.pop(redis_address)
            if i >= num_retries - 1:
                raise RuntimeError(
                    f"Unable to connect to Redis at {redis_address}: {e}"
                )
            # Wait a little bit.
            time.sleep(delay)
            # Make sure the retry interval doesn't increase too large.
            delay = min(1, delay * 2)


def start_ray_process(
    command: List[str],
    process_type: str,
    fate_share: bool,
    env_updates: Optional[dict] = None,
    cwd: Optional[str] = None,
    use_valgrind: bool = False,
    use_gdb: bool = False,
    use_valgrind_profiler: bool = False,
    use_perftools_profiler: bool = False,
    use_tmux: bool = False,
    stdout_file: Optional[str] = None,
    stderr_file: Optional[str] = None,
    pipe_stdin: bool = False,
):
    """Start one of the Ray processes.

    TODO(rkn): We need to figure out how these commands interact. For example,
    it may only make sense to start a process in gdb if we also start it in
    tmux. Similarly, certain combinations probably don't make sense, like
    simultaneously running the process in valgrind and the profiler.

    Args:
        command: The command to use to start the Ray process.
        process_type: The type of the process that is being started
            (e.g., "raylet").
        fate_share: If true, the child will be killed if its parent (us) dies.
            True must only be passed after detection of this functionality.
        env_updates: A dictionary of additional environment variables to
            run the command with (in addition to the caller's environment
            variables).
        cwd: The directory to run the process in.
        use_valgrind: True if we should start the process in valgrind.
        use_gdb: True if we should start the process in gdb.
        use_valgrind_profiler: True if we should start the process in
            the valgrind profiler.
        use_perftools_profiler: True if we should profile the process
            using perftools.
        use_tmux: True if we should start the process in tmux.
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
    perftools_profiler_env_var = f"RAY_{process_type.upper()}_PERFTOOLS_PROFILER"
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
                f"setpgrp failed, processes may not be cleaned up properly: {e}."
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


def start_log_monitor(
    logs_dir: str,
    gcs_address: str,
    fate_share: Optional[bool] = None,
    max_bytes: int = 0,
    backup_count: int = 0,
    redirect_logging: bool = True,
    stdout_file: Optional[IO[AnyStr]] = subprocess.DEVNULL,
    stderr_file: Optional[IO[AnyStr]] = subprocess.DEVNULL,
):
    """Start a log monitor process.

    Args:
        logs_dir: The directory of logging files.
        gcs_address: GCS address for pubsub.
        fate_share: Whether to share fate between log_monitor
            and this process.
        max_bytes: Log rotation parameter. Corresponding to
            RotatingFileHandler's maxBytes.
        backup_count: Log rotation parameter. Corresponding to
            RotatingFileHandler's backupCount.
        redirect_logging: Whether we should redirect logging to
            the provided log directory.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.

    Returns:
        ProcessInfo for the process that was started.
    """
    log_monitor_filepath = os.path.join(RAY_PATH, RAY_PRIVATE_DIR, "log_monitor.py")

    command = [
        sys.executable,
        "-u",
        log_monitor_filepath,
        f"--logs-dir={logs_dir}",
        f"--gcs-address={gcs_address}",
        f"--logging-rotate-bytes={max_bytes}",
        f"--logging-rotate-backup-count={backup_count}",
    ]

    if not redirect_logging:
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
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_LOG_MONITOR,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=fate_share,
    )
    return process_info


def start_api_server(
    include_dashboard: bool,
    raise_on_failure: bool,
    host: str,
    gcs_address: str,
    temp_dir: str,
    logdir: str,
    session_dir: str,
    port: Optional[int] = None,
    fate_share: Optional[bool] = None,
    max_bytes: int = 0,
    backup_count: int = 0,
    redirect_logging: bool = True,
    stdout_file: Optional[IO[AnyStr]] = subprocess.DEVNULL,
    stderr_file: Optional[IO[AnyStr]] = subprocess.DEVNULL,
):
    """Start a API server process.

    Args:
        include_dashboard: If true, this will load all dashboard-related modules
            when starting the API server. Otherwise, it will only
            start the modules that are not relevant to the dashboard.
        raise_on_failure: If true, this will raise an exception
            if we fail to start the API server. Otherwise it will print
            a warning if we fail to start the API server.
        host: The host to bind the dashboard web server to.
        gcs_address: The gcs address the dashboard should connect to
        temp_dir: The temporary directory used for log files and
            information for this Ray session.
        session_dir: The session directory under temp_dir.
            It is used as a identifier of individual cluster.
        logdir: The log directory used to generate dashboard log.
        port: The port to bind the dashboard web server to.
            Defaults to 8265.
        max_bytes: Log rotation parameter. Corresponding to
            RotatingFileHandler's maxBytes.
        backup_count: Log rotation parameter. Corresponding to
            RotatingFileHandler's backupCount.
        redirect_logging: Whether we should redirect logging to
            the provided log directory.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.

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
            *_build_python_executable_command_memory_profileable(
                ray_constants.PROCESS_TYPE_DASHBOARD,
                session_dir,
                unbuffered=False,
            ),
            dashboard_filepath,
            f"--host={host}",
            f"--port={port}",
            f"--port-retries={port_retries}",
            f"--temp-dir={temp_dir}",
            f"--log-dir={logdir}",
            f"--session-dir={session_dir}",
            f"--logging-rotate-bytes={max_bytes}",
            f"--logging-rotate-backup-count={backup_count}",
            f"--gcs-address={gcs_address}",
        ]

        if not redirect_logging:
            # If not redirecting logging to files, unset log filename.
            # This will cause log records to go to stderr.
            command.append("--logging-filename=")
            # Use stderr log format with the component name as a message prefix.
            logging_format = ray_constants.LOGGER_FORMAT_STDERR.format(
                component=ray_constants.PROCESS_TYPE_DASHBOARD
            )
            command.append(f"--logging-format={logging_format}")
            # Inherit stdout/stderr streams so that
            # logs are redirected to stderr.
            stdout_file = None
            stderr_file = None
        if minimal:
            command.append("--minimal")

        if not include_dashboard:
            # If dashboard is not included, load modules
            # that are irrelevant to the dashboard.
            # TODO(sang): Modules like job or state APIs should be
            # loaded although dashboard is disabled. Fix it.
            command.append("--modules-to-load=UsageStatsHead")
            command.append("--disable-frontend-serving")

        process_info = start_ray_process(
            command,
            ray_constants.PROCESS_TYPE_DASHBOARD,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            fate_share=fate_share,
        )

        # Retrieve the dashboard url
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

        # Dashboard couldn't be started.
        if dashboard_url is None:
            returncode_str = (
                f", return code {dashboard_returncode}"
                if dashboard_returncode is not None
                else ""
            )
            logger.error(f"Failed to start the dashboard {returncode_str}")

            def read_log(filename, lines_to_read):
                """Read a log file and return the last 20 lines."""
                dashboard_log = os.path.join(logdir, filename)
                # Read last n lines of dashboard log. The log file may be large.
                lines_to_read = 20
                lines = []
                with open(dashboard_log, "rb") as f:
                    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                        end = mm.size()
                        for _ in range(lines_to_read):
                            sep = mm.rfind(b"\n", 0, end - 1)
                            if sep == -1:
                                break
                            lines.append(mm[sep + 1 : end].decode("utf-8"))
                            end = sep
                lines.append(
                    f"The last {lines_to_read} lines of {dashboard_log} "
                    "(it contains the error message from the dashboard): "
                )
                return lines

            if logdir:
                lines_to_read = 20
                logger.error(
                    "Error should be written to 'dashboard.log' or "
                    "'dashboard.err'. We are printing the last "
                    f"{lines_to_read} lines for you. See "
                    "'https://docs.ray.io/en/master/ray-observability/ray-logging.html#logging-directory-structure' "  # noqa
                    "to find where the log file is."
                )
                try:
                    lines = read_log("dashboard.log", lines_to_read=lines_to_read)
                except Exception as e:
                    logger.error(
                        f"Couldn't read dashboard.log file. Error: {e}. "
                        "It means the dashboard is broken even before it "
                        "initializes the logger (mostly dependency issues). "
                        "Reading the dashboard.err file which contains stdout/stderr."
                    )
                    # If we cannot read the .log file, we fallback to .err file.
                    # This is the case where dashboard couldn't be started at all
                    # and couldn't even initialize the logger to write logs to .log
                    # file.
                    try:
                        lines = read_log("dashboard.err", lines_to_read=lines_to_read)
                    except Exception as e:
                        raise Exception(
                            f"Failed to read dashboard.err file: {e}. "
                            "It is unexpected. Please report an issue to "
                            "Ray github. "
                            "https://github.com/ray-project/ray/issues"
                        )
                last_log_str = "\n" + "\n".join(reversed(lines[-lines_to_read:]))
                raise Exception(last_log_str)
            else:
                # Is it reachable?
                raise Exception("Failed to start a dashboard.")

        if minimal or not include_dashboard:
            # If it is the minimal installation, the web url (dashboard url)
            # shouldn't be configured because it doesn't start a server.
            dashboard_url = ""
        return dashboard_url, process_info
    except Exception as e:
        if raise_on_failure:
            raise e from e
        else:
            logger.error(e)
            return None, None


def start_gcs_server(
    redis_address: str,
    log_dir: str,
    session_name: str,
    stdout_file: Optional[str] = None,
    stderr_file: Optional[str] = None,
    redis_password: Optional[str] = None,
    config: Optional[dict] = None,
    fate_share: Optional[bool] = None,
    gcs_server_port: Optional[int] = None,
    metrics_agent_port: Optional[int] = None,
    node_ip_address: Optional[str] = None,
):
    """Start a gcs server.

    Args:
        redis_address: The address that the Redis server is listening on.
        log_dir: The path of the dir where log files are created.
        session_name: The session name (cluster id) of this cluster.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        redis_password: The password of the redis server.
        config: Optional configuration that will
            override defaults in RayConfig.
        gcs_server_port: Port number of the gcs server.
        metrics_agent_port: The port where metrics agent is bound to.
        node_ip_address: IP Address of a node where gcs server starts.

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
        f"--session-name={session_name}",
    ]
    if redis_address:
        parts = redis_address.split("://", 1)
        enable_redis_ssl = "false"
        if len(parts) == 1:
            redis_ip_address, redis_port = parts[0].rsplit(":", 1)
        else:
            if len(parts) != 2 or parts[0] not in ("redis", "rediss"):
                raise ValueError(f"Invalid redis address {redis_address}")
            redis_ip_address, redis_port = parts[1].rsplit(":", 1)
            if parts[0] == "rediss":
                enable_redis_ssl = "true"

        command += [
            f"--redis_address={redis_ip_address}",
            f"--redis_port={redis_port}",
            f"--redis_enable_ssl={enable_redis_ssl}",
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
    redis_address: str,
    gcs_address: str,
    node_ip_address: str,
    node_manager_port: int,
    raylet_name: str,
    plasma_store_name: str,
    worker_path: str,
    setup_worker_path: str,
    storage: str,
    temp_dir: str,
    session_dir: str,
    resource_dir: str,
    log_dir: str,
    resource_spec,
    plasma_directory: str,
    object_store_memory: int,
    session_name: str,
    min_worker_port: Optional[int] = None,
    max_worker_port: Optional[int] = None,
    worker_port_list: Optional[List[int]] = None,
    object_manager_port: Optional[int] = None,
    redis_password: Optional[str] = None,
    metrics_agent_port: Optional[int] = None,
    metrics_export_port: Optional[int] = None,
    dashboard_agent_listen_port: Optional[int] = None,
    use_valgrind: bool = False,
    use_profiler: bool = False,
    stdout_file: Optional[str] = None,
    stderr_file: Optional[str] = None,
    config: Optional[dict] = None,
    huge_pages: bool = False,
    fate_share: Optional[bool] = None,
    socket_to_use: Optional[int] = None,
    start_initial_python_workers_for_first_job: bool = False,
    max_bytes: int = 0,
    backup_count: int = 0,
    ray_debugger_external: bool = False,
    env_updates: Optional[dict] = None,
    node_name: Optional[str] = None,
    webui: Optional[str] = None,
):
    """Start a raylet, which is a combined local scheduler and object manager.

    Args:
        redis_address: The address of the primary Redis server.
        gcs_address: The address of GCS server.
        node_ip_address: The IP address of this node.
        node_manager_port: The port to use for the node manager. If it's
            0, a random port will be used.
        raylet_name: The name of the raylet socket to create.
        plasma_store_name: The name of the plasma store socket to connect
             to.
        worker_path: The path of the Python file that new worker
            processes will execute.
        setup_worker_path: The path of the Python file that will set up
            the environment for the worker process.
        storage: The persistent storage URI.
        temp_dir: The path of the temporary directory Ray will use.
        session_dir: The path of this session.
        resource_dir: The path of resource of this session .
        log_dir: The path of the dir where log files are created.
        resource_spec: Resources for this raylet.
        session_name: The session name (cluster id) of this cluster.
        object_manager_port: The port to use for the object manager. If this is
            None, then the object manager will choose its own port.
        min_worker_port: The lowest port number that workers will bind
            on. If not set, random ports will be chosen.
        max_worker_port: The highest port number that workers will bind
            on. If set, min_worker_port must also be set.
        redis_password: The password to use when connecting to Redis.
        metrics_agent_port: The port where metrics agent is bound to.
        metrics_export_port: The port at which metrics are exposed to.
        use_valgrind: True if the raylet should be started inside
            of valgrind. If this is True, use_profiler must be False.
        use_profiler: True if the raylet should be started inside
            a profiler. If this is True, use_valgrind must be False.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        tracing_startup_hook: Tracing startup hook.
        config: Optional Raylet configuration that will
            override defaults in RayConfig.
        max_bytes: Log rotation parameter. Corresponding to
            RotatingFileHandler's maxBytes.
        backup_count: Log rotation parameter. Corresponding to
            RotatingFileHandler's backupCount.
        ray_debugger_external: True if the Ray debugger should be made
            available externally to this node.
        env_updates: Environment variable overrides.

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
            gcs_address,
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
            gcs_address,
            plasma_store_name,
            raylet_name,
            redis_password,
            session_dir,
            log_dir,
            node_ip_address,
            setup_worker_path,
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
        f"--session-name={session_name}",
        f"--temp-dir={temp_dir}",
        f"--webui={webui}",
    ]

    start_worker_command.append(f"--storage={storage}")

    start_worker_command.append("RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER")

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
        *_build_python_executable_command_memory_profileable(
            ray_constants.PROCESS_TYPE_DASHBOARD_AGENT, session_dir
        ),
        os.path.join(RAY_PATH, "dashboard", "agent.py"),
        f"--node-ip-address={node_ip_address}",
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
        f"--session-name={session_name}",
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
        f"--temp_dir={temp_dir}",
        f"--session_dir={session_dir}",
        f"--log_dir={log_dir}",
        f"--resource_dir={resource_dir}",
        f"--metrics-agent-port={metrics_agent_port}",
        f"--metrics_export_port={metrics_export_port}",
        f"--object_store_memory={object_store_memory}",
        f"--plasma_directory={plasma_directory}",
        f"--ray-debugger-external={1 if ray_debugger_external else 0}",
        f"--gcs-address={gcs_address}",
        f"--session-name={session_name}",
    ]

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
    if node_name is not None:
        command.append(
            f"--node-name={node_name}",
        )
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
    bootstrap_address: str,
    plasma_store_name: str,
    raylet_name: str,
    redis_password: str,
    session_dir: str,
    node_ip_address: str,
    setup_worker_path: str,
):
    """This method assembles the command used to start a Java worker.

    Args:
        bootstrap_address: Bootstrap address of ray cluster.
        plasma_store_name: The name of the plasma store socket to connect
           to.
        raylet_name: The name of the raylet socket to create.
        redis_password: The password of connect to redis.
        session_dir: The path of this session.
        node_ip_address: The ip address for this node.
        setup_worker_path: The path of the Python file that will set up
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
        + ["-D{}={}".format(*pair) for pair in pairs]
    )

    command += ["RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER"]
    command += ["io.ray.runtime.runner.worker.DefaultWorker"]

    return command


def build_cpp_worker_command(
    bootstrap_address: str,
    plasma_store_name: str,
    raylet_name: str,
    redis_password: str,
    session_dir: str,
    log_dir: str,
    node_ip_address: str,
    setup_worker_path: str,
):
    """This method assembles the command used to start a CPP worker.

    Args:
        bootstrap_address: The bootstrap address of the cluster.
        plasma_store_name: The name of the plasma store socket to connect
           to.
        raylet_name: The name of the raylet socket to create.
        redis_password: The password of connect to redis.
        session_dir: The path of this session.
        log_dir: The path of logs.
        node_ip_address: The ip address for this node.
        setup_worker_path: The path of the Python file that will set up
            the environment for the worker process.
    Returns:
        The command string for starting CPP worker.
    """

    command = [
        sys.executable,
        setup_worker_path,
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
    object_store_memory: int,
    plasma_directory: Optional[str] = None,
    huge_pages: bool = False,
):
    """Figure out how to configure the plasma object store.

    This will determine which directory to use for the plasma store. On Linux,
    we will try to use /dev/shm unless the shared memory file system is too
    small, in which case we will fall back to /tmp. If any of the object store
    memory or plasma directory parameters are specified by the user, then those
    values will be preserved.

    Args:
        object_store_memory: The object store memory to use.
        plasma_directory: The user-specified plasma directory parameter.
        huge_pages: The user-specified huge pages parameter.

    Returns:
        The plasma directory to use. If it is specified by the user, then that
            value will be preserved.
    """
    if not isinstance(object_store_memory, int):
        object_store_memory = int(object_store_memory)

    if huge_pages and not (sys.platform == "linux" or sys.platform == "linux2"):
        raise ValueError("The huge_pages argument is only supported on Linux.")

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
                        object_store_memory * (1.1) / (2**30),
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
        logger.info("object_store_memory is not verified when plasma_directory is set.")

    if not os.path.isdir(plasma_directory):
        raise ValueError(
            f"The file {plasma_directory} does not exist or is not a directory."
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
                object_store_memory / 2**30,
                ray_constants.MAC_DEGRADED_PERF_MMAP_SIZE_LIMIT / 2**30,
                ray_constants.MAC_DEGRADED_PERF_MMAP_SIZE_LIMIT / 2**30,
            )
        )

    # Print the object store memory using two decimal places.
    logger.debug(
        "Determine to start the Plasma object store with {} GB memory "
        "using {}.".format(round(object_store_memory / 10**9, 2), plasma_directory)
    )

    return plasma_directory, object_store_memory


def start_monitor(
    gcs_address: str,
    logs_dir: str,
    stdout_file: Optional[str] = None,
    stderr_file: Optional[str] = None,
    autoscaling_config: Optional[str] = None,
    fate_share: Optional[bool] = None,
    max_bytes: int = 0,
    backup_count: int = 0,
    monitor_ip: Optional[str] = None,
):
    """Run a process to monitor the other processes.

    Args:
        gcs_address: The address of GCS server.
        logs_dir: The path to the log directory.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        autoscaling_config: path to autoscaling config file.
        max_bytes: Log rotation parameter. Corresponding to
            RotatingFileHandler's maxBytes.
        backup_count: Log rotation parameter. Corresponding to
            RotatingFileHandler's backupCount.
        monitor_ip: IP address of the machine that the monitor will be
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
        f"--logging-rotate-bytes={max_bytes}",
        f"--logging-rotate-backup-count={backup_count}",
    ]
    if gcs_address is not None:
        command.append(f"--gcs-address={gcs_address}")
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
    address: str,
    ray_client_server_ip: str,
    ray_client_server_port: int,
    stdout_file: Optional[int] = None,
    stderr_file: Optional[int] = None,
    redis_password: Optional[int] = None,
    fate_share: Optional[bool] = None,
    metrics_agent_port: Optional[int] = None,
    server_type: str = "proxy",
    serialized_runtime_env_context: Optional[str] = None,
):
    """Run the server process of the Ray client.

    Args:
        address: The address of the cluster.
        ray_client_server_ip: Host IP the Ray client server listens on.
        ray_client_server_port: Port the Ray client server listens on.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        redis_password: The password of the redis server.
        server_type: Whether to start the proxy version of Ray Client.
        serialized_runtime_env_context (str|None): If specified, the serialized
            runtime_env_context to start the client server in.

    Returns:
        ProcessInfo for the process that was started.
    """
    root_ray_dir = Path(__file__).resolve().parents[1]
    setup_worker_path = os.path.join(
        root_ray_dir, "_private", "workers", ray_constants.SETUP_WORKER_FILENAME
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
