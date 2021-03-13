import asyncio
import errno
import io
import fnmatch
import os
import subprocess
import sys
import time
import socket
import math

from contextlib import redirect_stdout, redirect_stderr
import yaml

import ray
import ray._private.services
import ray._private.utils
import requests
from prometheus_client.parser import text_string_to_metric_families
from ray.scripts.scripts import main as ray_main

import psutil  # We must import psutil after ray because we bundle it with ray.

if sys.platform == "win32":
    import _winapi


class RayTestTimeoutException(Exception):
    """Exception used to identify timeouts from test utilities."""
    pass


def _pid_alive(pid):
    """Check if the process with this PID is alive or not.

    Args:
        pid: The pid to check.

    Returns:
        This returns false if the process is dead. Otherwise, it returns true.
    """
    no_such_process = errno.EINVAL if sys.platform == "win32" else errno.ESRCH
    alive = True
    try:
        if sys.platform == "win32":
            SYNCHRONIZE = 0x00100000  # access mask defined in <winnt.h>
            handle = _winapi.OpenProcess(SYNCHRONIZE, False, pid)
            try:
                alive = (_winapi.WaitForSingleObject(handle, 0) !=
                         _winapi.WAIT_OBJECT_0)
            finally:
                _winapi.CloseHandle(handle)
        else:
            os.kill(pid, 0)
    except OSError as ex:
        if ex.errno != no_such_process:
            raise
        alive = False
    return alive


def check_call_module(main, argv, capture_stdout=False, capture_stderr=False):
    # We use this function instead of calling the "ray" command to work around
    # some deadlocks that occur when piping ray's output on Windows
    stream = io.TextIOWrapper(io.BytesIO(), encoding=sys.stdout.encoding)
    old_argv = sys.argv[:]
    try:
        sys.argv = argv[:]
        try:
            with redirect_stderr(stream if capture_stderr else sys.stderr):
                with redirect_stdout(stream if capture_stdout else sys.stdout):
                    main()
        finally:
            stream.flush()
    except SystemExit as ex:
        if ex.code:
            output = stream.buffer.getvalue()
            raise subprocess.CalledProcessError(ex.code, argv, output)
    except Exception as ex:
        output = stream.buffer.getvalue()
        raise subprocess.CalledProcessError(1, argv, output, ex.args[0])
    finally:
        sys.argv = old_argv
        if capture_stdout:
            sys.stdout.buffer.write(stream.buffer.getvalue())
        elif capture_stderr:
            sys.stderr.buffer.write(stream.buffer.getvalue())
    return stream.buffer.getvalue()


def check_call_ray(args, capture_stdout=False, capture_stderr=False):
    # We use this function instead of calling the "ray" command to work around
    # some deadlocks that occur when piping ray's output on Windows
    argv = ["ray"] + args
    if sys.platform == "win32":
        result = check_call_module(
            ray_main,
            argv,
            capture_stdout=capture_stdout,
            capture_stderr=capture_stderr)
    else:
        stdout_redir = None
        stderr_redir = None
        if capture_stdout:
            stdout_redir = subprocess.PIPE
        if capture_stderr and capture_stdout:
            stderr_redir = subprocess.STDOUT
        elif capture_stderr:
            stderr_redir = subprocess.PIPE
        proc = subprocess.Popen(argv, stdout=stdout_redir, stderr=stderr_redir)
        (stdout, stderr) = proc.communicate()
        if proc.returncode:
            raise subprocess.CalledProcessError(proc.returncode, argv, stdout,
                                                stderr)
        result = b"".join([s for s in [stdout, stderr] if s is not None])
    return result


def wait_for_pid_to_exit(pid, timeout=20):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if not _pid_alive(pid):
            return
        time.sleep(0.1)
    raise RayTestTimeoutException(
        f"Timed out while waiting for process {pid} to exit.")


def wait_for_children_names_of_pid(pid, children_names, timeout=20):
    p = psutil.Process(pid)
    start_time = time.time()
    children_names = set(children_names)
    not_found_children = []
    children = []
    while time.time() - start_time < timeout:
        children = p.children(recursive=False)
        not_found_children = set(children_names) - {c.name() for c in children}
        if len(not_found_children) == 0:
            return
        time.sleep(0.1)
    raise RayTestTimeoutException(
        "Timed out while waiting for process {} children to start "
        "({} not found from children {}).".format(pid, not_found_children,
                                                  children))


def wait_for_children_of_pid(pid, num_children=1, timeout=20):
    p = psutil.Process(pid)
    start_time = time.time()
    while time.time() - start_time < timeout:
        num_alive = len(p.children(recursive=False))
        if num_alive >= num_children:
            return
        time.sleep(0.1)
    raise RayTestTimeoutException(
        "Timed out while waiting for process {} children to start "
        "({}/{} started).".format(pid, num_alive, num_children))


def wait_for_children_of_pid_to_exit(pid, timeout=20):
    children = psutil.Process(pid).children()
    if len(children) == 0:
        return

    _, alive = psutil.wait_procs(children, timeout=timeout)
    if len(alive) > 0:
        raise RayTestTimeoutException(
            "Timed out while waiting for process children to exit."
            " Children still alive: {}.".format([p.name() for p in alive]))


def kill_process_by_name(name, SIGKILL=False):
    for p in psutil.process_iter(attrs=["name"]):
        if p.info["name"] == name + ray._private.services.EXE_SUFFIX:
            if SIGKILL:
                p.kill()
            else:
                p.terminate()


def run_string_as_driver(driver_script):
    """Run a driver as a separate process.

    Args:
        driver_script: A string to run as a Python script.

    Returns:
        The script's output.
    """
    proc = subprocess.Popen(
        [sys.executable, "-"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    with proc:
        output = proc.communicate(driver_script.encode("ascii"))[0]
        if proc.returncode:
            print(ray._private.utils.decode(output))
            raise subprocess.CalledProcessError(proc.returncode, proc.args,
                                                output, proc.stderr)
        out = ray._private.utils.decode(output)
    return out


def run_string_as_driver_nonblocking(driver_script):
    """Start a driver as a separate process and return immediately.

    Args:
        driver_script: A string to run as a Python script.

    Returns:
        A handle to the driver process.
    """
    script = "; ".join([
        "import sys",
        "script = sys.stdin.read()",
        "sys.stdin.close()",
        "del sys",
        "exec(\"del script\\n\" + script)",
    ])
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    proc.stdin.write(driver_script.encode("ascii"))
    proc.stdin.close()
    return proc


def wait_for_num_actors(num_actors, state=None, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len([
                _ for _ in ray.actors().values()
                if state is None or _["State"] == state
        ]) >= num_actors:
            return
        time.sleep(0.1)
    raise RayTestTimeoutException("Timed out while waiting for global state.")


def kill_actor_and_wait_for_failure(actor, timeout=10, retry_interval_ms=100):
    actor_id = actor._actor_id.hex()
    current_num_restarts = ray.actors(actor_id)["NumRestarts"]
    ray.kill(actor)
    start = time.time()
    while time.time() - start <= timeout:
        actor_status = ray.actors(actor_id)
        if actor_status["State"] == ray.gcs_utils.ActorTableData.DEAD \
                or actor_status["NumRestarts"] > current_num_restarts:
            return
        time.sleep(retry_interval_ms / 1000.0)
    raise RuntimeError(
        "It took too much time to kill an actor: {}".format(actor_id))


def wait_for_condition(condition_predictor, timeout=10, retry_interval_ms=100):
    """Wait until a condition is met or time out with an exception.

    Args:
        condition_predictor: A function that predicts the condition.
        timeout: Maximum timeout in seconds.
        retry_interval_ms: Retry interval in milliseconds.

    Raises:
        RuntimeError: If the condition is not met before the timeout expires.
    """
    start = time.time()
    while time.time() - start <= timeout:
        if condition_predictor():
            return
        time.sleep(retry_interval_ms / 1000.0)
    raise RuntimeError("The condition wasn't met before the timeout expired.")


def wait_until_succeeded_without_exception(func,
                                           exceptions,
                                           *args,
                                           timeout_ms=1000,
                                           retry_interval_ms=100):
    """A helper function that waits until a given function
        completes without exceptions.

    Args:
        func: A function to run.
        exceptions(tuple): Exceptions that are supposed to occur.
        args: arguments to pass for a given func
        timeout_ms: Maximum timeout in milliseconds.
        retry_interval_ms: Retry interval in milliseconds.

    Return:
        Whether exception occurs within a timeout.
    """
    if type(exceptions) != tuple:
        print("exceptions arguments should be given as a tuple")
        return False

    time_elapsed = 0
    start = time.time()
    while time_elapsed <= timeout_ms:
        try:
            func(*args)
            return True
        except exceptions:
            time_elapsed = (time.time() - start) * 1000
            time.sleep(retry_interval_ms / 1000.0)
    return False


def recursive_fnmatch(dirpath, pattern):
    """Looks at a file directory subtree for a filename pattern.

    Similar to glob.glob(..., recursive=True) but also supports 2.7
    """
    matches = []
    for root, dirnames, filenames in os.walk(dirpath):
        for filename in fnmatch.filter(filenames, pattern):
            matches.append(os.path.join(root, filename))
    return matches


def generate_system_config_map(**kwargs):
    ray_kwargs = {
        "_system_config": kwargs,
    }
    return ray_kwargs


@ray.remote(num_cpus=0)
class SignalActor:
    def __init__(self):
        self.ready_event = asyncio.Event()

    def send(self, clear=False):
        self.ready_event.set()
        if clear:
            self.ready_event.clear()

    async def wait(self, should_wait=True):
        if should_wait:
            await self.ready_event.wait()


@ray.remote(num_cpus=0)
class Semaphore:
    def __init__(self, value=1):
        self._sema = asyncio.Semaphore(value=value)

    async def acquire(self):
        await self._sema.acquire()

    async def release(self):
        self._sema.release()

    async def locked(self):
        return self._sema.locked()


def dicts_equal(dict1, dict2, abs_tol=1e-4):
    """Compares to dicts whose values may be floating point numbers."""

    if dict1.keys() != dict2.keys():
        return False

    for k, v in dict1.items():
        if isinstance(v, float) and \
           isinstance(dict2[k], float) and \
           math.isclose(v, dict2[k], abs_tol=abs_tol):
            continue
        if v != dict2[k]:
            return False
    return True


def same_elements(elems_a, elems_b):
    """Checks if two iterables (such as lists) contain the same elements. Elements
        do not have to be hashable (this allows us to compare sets of dicts for
        example). This comparison is not necessarily efficient.
    """
    a = list(elems_a)
    b = list(elems_b)

    for x in a:
        if x not in b:
            return False

    for x in b:
        if x not in a:
            return False

    return True


@ray.remote
def _put(obj):
    return obj


def put_object(obj, use_ray_put):
    if use_ray_put:
        return ray.put(obj)
    else:
        return _put.remote(obj)


def put_unpinned_object(obj):
    value = ray.worker.global_worker.get_serialization_context().serialize(obj)
    return ray.ObjectRef(
        ray.worker.global_worker.core_worker.put_serialized_object(
            value, pin_object=False))


def wait_until_server_available(address,
                                timeout_ms=5000,
                                retry_interval_ms=100):
    ip_port = address.split(":")
    ip = ip_port[0]
    port = int(ip_port[1])
    time_elapsed = 0
    start = time.time()
    while time_elapsed <= timeout_ms:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        try:
            s.connect((ip, port))
        except Exception:
            time_elapsed = (time.time() - start) * 1000
            time.sleep(retry_interval_ms / 1000.0)
            s.close()
            continue
        s.close()
        return True
    return False


def get_other_nodes(cluster, exclude_head=False):
    """Get all nodes except the one that we're connected to."""
    return [
        node for node in cluster.list_all_nodes() if
        node._raylet_socket_name != ray.worker._global_node._raylet_socket_name
        and (exclude_head is False or node.head is False)
    ]


def get_non_head_nodes(cluster):
    """Get all non-head nodes."""
    return list(filter(lambda x: x.head is False, cluster.list_all_nodes()))


def init_error_pubsub():
    """Initialize redis error info pub/sub"""
    p = ray.worker.global_worker.redis_client.pubsub(
        ignore_subscribe_messages=True)
    error_pubsub_channel = ray.gcs_utils.RAY_ERROR_PUBSUB_PATTERN
    p.psubscribe(error_pubsub_channel)
    return p


def get_error_message(pub_sub, num, error_type=None, timeout=20):
    """Get errors through pub/sub."""
    start_time = time.time()
    msgs = []
    while time.time() - start_time < timeout and len(msgs) < num:
        msg = pub_sub.get_message()
        if msg is None:
            time.sleep(0.01)
            continue
        pubsub_msg = ray.gcs_utils.PubSubMessage.FromString(msg["data"])
        error_data = ray.gcs_utils.ErrorTableData.FromString(pubsub_msg.data)
        if error_type is None or error_type == error_data.type:
            msgs.append(error_data)
        else:
            time.sleep(0.01)

    return msgs


def format_web_url(url):
    """Format web url."""
    url = url.replace("localhost", "http://127.0.0.1")
    if not url.startswith("http://"):
        return "http://" + url
    return url


def new_scheduler_enabled():
    return os.environ.get("RAY_ENABLE_NEW_SCHEDULER", "1") == "1"


def client_test_enabled() -> bool:
    return os.environ.get("RAY_CLIENT_MODE") == "1"


def fetch_prometheus(prom_addresses):
    components_dict = {}
    metric_names = set()
    metric_samples = []
    for address in prom_addresses:
        if address not in components_dict:
            components_dict[address] = set()
        try:
            response = requests.get(f"http://{address}/metrics")
        except requests.exceptions.ConnectionError:
            continue

        for line in response.text.split("\n"):
            for family in text_string_to_metric_families(line):
                for sample in family.samples:
                    metric_names.add(sample.name)
                    metric_samples.append(sample)
                    if "Component" in sample.labels:
                        components_dict[address].add(
                            sample.labels["Component"])
    return components_dict, metric_names, metric_samples


def load_test_config(config_file_name):
    """Loads a config yaml from tests/test_cli_patterns."""
    here = os.path.realpath(__file__)
    parent = os.path.dirname(here)
    config_path = os.path.join(parent, "tests/test_cli_patterns",
                               config_file_name)
    config = yaml.safe_load(open(config_path).read())
    return config
