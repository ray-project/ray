import asyncio
import io
import fnmatch
import os
import json
import pathlib
import subprocess
import sys
import time
import timeit
import socket
import math
import traceback
from typing import Optional, Any, List, Dict
from contextlib import redirect_stdout, redirect_stderr, contextmanager

import yaml
import logging
import tempfile
import grpc
from grpc._channel import _InactiveRpcError
import numpy as np

import ray
import ray._private.services
import ray._private.utils
import ray._private.gcs_utils as gcs_utils
import ray._private.memory_monitor as memory_monitor
from ray._raylet import GcsClientOptions, GlobalStateAccessor
from ray.core.generated import gcs_pb2
from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc
from ray._private.gcs_pubsub import gcs_pubsub_enabled, GcsErrorSubscriber, \
    GcsLogSubscriber
from ray._private.tls_utils import generate_self_signed_tls_certs
from ray.util.queue import Queue, _QueueActor, Empty
from ray.scripts.scripts import main as ray_main
try:
    from prometheus_client.parser import text_string_to_metric_families
except (ImportError, ModuleNotFoundError):

    def text_string_to_metric_families(*args, **kwargs):
        raise ModuleNotFoundError("`prometheus_client` not found")


import psutil  # We must import psutil after ray because we bundle it with ray.


class RayTestTimeoutException(Exception):
    """Exception used to identify timeouts from test utilities."""
    pass


def make_global_state_accessor(address_info):
    if not gcs_utils.use_gcs_for_bootstrap():
        gcs_options = GcsClientOptions.from_redis_address(
            address_info["redis_address"],
            ray.ray_constants.REDIS_DEFAULT_PASSWORD)
    else:
        gcs_options = GcsClientOptions.from_gcs_address(
            address_info["gcs_address"])
    global_state_accessor = GlobalStateAccessor(gcs_options)
    global_state_accessor.connect()
    return global_state_accessor


def _pid_alive(pid):
    """Check if the process with this PID is alive or not.

    Args:
        pid: The pid to check.

    Returns:
        This returns false if the process is dead. Otherwise, it returns true.
    """
    alive = True
    try:
        psutil.Process(pid)
    except psutil.NoSuchProcess:
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
    alive = []
    while time.time() - start_time < timeout:
        alive = p.children(recursive=False)
        num_alive = len(alive)
        if num_alive >= num_children:
            return
        time.sleep(0.1)
    raise RayTestTimeoutException(
        f"Timed out while waiting for process {pid} children to start "
        f"({num_alive}/{num_children} started: {alive}).")


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


def run_string_as_driver(driver_script: str, env: Dict = None):
    """Run a driver as a separate process.

    Args:
        driver_script (str): A string to run as a Python script.
        env (dict): The environment variables for the driver.

    Returns:
        The script's output.
    """
    if env is not None and gcs_utils.use_gcs_for_bootstrap():
        env.update({
            "RAY_bootstrap_with_gcs": "1",
            "RAY_gcs_grpc_based_pubsub": "1",
            "RAY_gcs_storage": "memory",
            "RAY_bootstrap_with_gcs": "1",
        })

    proc = subprocess.Popen(
        [sys.executable, "-"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
    )
    with proc:
        output = proc.communicate(driver_script.encode("ascii"))[0]
        if proc.returncode:
            print(ray._private.utils.decode(output))
            raise subprocess.CalledProcessError(proc.returncode, proc.args,
                                                output, proc.stderr)
        out = ray._private.utils.decode(output)
    return out


def run_string_as_driver_nonblocking(driver_script, env: Dict = None):
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
        stderr=subprocess.PIPE,
        env=env,
    )
    proc.stdin.write(driver_script.encode("ascii"))
    proc.stdin.close()
    return proc


def convert_actor_state(state):
    if not state:
        return None
    return (gcs_pb2.ActorTableData.ActorState.DESCRIPTOR.values_by_number[
        state].name)


def wait_for_num_actors(num_actors, state=None, timeout=10):
    state = convert_actor_state(state)
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len([
                _ for _ in ray.state.actors().values()
                if state is None or _["State"] == state
        ]) >= num_actors:
            return
        time.sleep(0.1)
    raise RayTestTimeoutException("Timed out while waiting for global state.")


def wait_for_num_nodes(num_nodes: int, timeout_s: int):
    curr_nodes = 0
    start = time.time()
    next_feedback = start
    max_time = start + timeout_s
    while not curr_nodes >= num_nodes:
        now = time.time()

        if now >= max_time:
            raise RuntimeError(
                f"Maximum wait time reached, but only "
                f"{curr_nodes}/{num_nodes} nodes came up. Aborting.")

        if now >= next_feedback:
            passed = now - start
            print(f"Waiting for more nodes to come up: "
                  f"{curr_nodes}/{num_nodes} "
                  f"({passed:.0f} seconds passed)")
            next_feedback = now + 10

        time.sleep(5)
        curr_nodes = len(ray.nodes())

    passed = time.time() - start
    print(f"Cluster is up: {curr_nodes}/{num_nodes} nodes online after "
          f"{passed:.0f} seconds")


def kill_actor_and_wait_for_failure(actor, timeout=10, retry_interval_ms=100):
    actor_id = actor._actor_id.hex()
    current_num_restarts = ray.state.actors(actor_id)["NumRestarts"]
    ray.kill(actor)
    start = time.time()
    while time.time() - start <= timeout:
        actor_status = ray.state.actors(actor_id)
        if actor_status["State"] == convert_actor_state(
            gcs_utils.ActorTableData.DEAD) \
                or actor_status["NumRestarts"] > current_num_restarts:
            return
        time.sleep(retry_interval_ms / 1000.0)
    raise RuntimeError(
        "It took too much time to kill an actor: {}".format(actor_id))


def wait_for_condition(condition_predictor,
                       timeout=10,
                       retry_interval_ms=100,
                       **kwargs: Any):
    """Wait until a condition is met or time out with an exception.

    Args:
        condition_predictor: A function that predicts the condition.
        timeout: Maximum timeout in seconds.
        retry_interval_ms: Retry interval in milliseconds.

    Raises:
        RuntimeError: If the condition is not met before the timeout expires.
    """
    start = time.time()
    last_ex = None
    while time.time() - start <= timeout:
        try:
            if condition_predictor(**kwargs):
                return
        except Exception as ex:
            last_ex = ex
        time.sleep(retry_interval_ms / 1000.0)
    message = "The condition wasn't met before the timeout expired."
    if last_ex is not None:
        message += f" Last exception: {last_ex}"
    raise RuntimeError(message)


def wait_until_succeeded_without_exception(func,
                                           exceptions,
                                           *args,
                                           timeout_ms=1000,
                                           retry_interval_ms=100,
                                           raise_last_ex=False):
    """A helper function that waits until a given function
        completes without exceptions.

    Args:
        func: A function to run.
        exceptions(tuple): Exceptions that are supposed to occur.
        args: arguments to pass for a given func
        timeout_ms: Maximum timeout in milliseconds.
        retry_interval_ms: Retry interval in milliseconds.
        raise_last_ex: Raise the last exception when timeout.

    Return:
        Whether exception occurs within a timeout.
    """
    if type(exceptions) != tuple:
        raise Exception("exceptions arguments should be given as a tuple")

    time_elapsed = 0
    start = time.time()
    last_ex = None
    while time_elapsed <= timeout_ms:
        try:
            func(*args)
            return True
        except exceptions as ex:
            last_ex = ex
            time_elapsed = (time.time() - start) * 1000
            time.sleep(retry_interval_ms / 1000.0)
    if raise_last_ex:
        ex_stack = traceback.format_exception(
            type(last_ex), last_ex, last_ex.__traceback__) if last_ex else []
        ex_stack = "".join(ex_stack)
        raise Exception(f"Timed out while testing, {ex_stack}")
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
        self.num_waiters = 0

    def send(self, clear=False):
        self.ready_event.set()
        if clear:
            self.ready_event.clear()

    async def wait(self, should_wait=True):
        if should_wait:
            self.num_waiters += 1
            await self.ready_event.wait()
            self.num_waiters -= 1

    async def cur_num_waiters(self):
        return self.num_waiters


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
    if gcs_pubsub_enabled():
        s = GcsErrorSubscriber(
            address=ray.worker.global_worker.gcs_client.address)
        s.subscribe()
    else:
        s = ray.worker.global_worker.redis_client.pubsub(
            ignore_subscribe_messages=True)
        s.psubscribe(gcs_utils.RAY_ERROR_PUBSUB_PATTERN)
    return s


def get_error_message(subscriber, num=1e6, error_type=None, timeout=20):
    """Gets errors from GCS / Redis subscriber.

    Returns maximum `num` error strings within `timeout`.
    Only returns errors of `error_type` if specified.
    """
    deadline = time.time() + timeout
    msgs = []
    while time.time() < deadline and len(msgs) < num:
        if isinstance(subscriber, GcsErrorSubscriber):
            _, error_data = subscriber.poll(timeout=deadline - time.time())
            if not error_data:
                # Timed out before any data is received.
                break
        else:
            msg = subscriber.get_message()
            if msg is None:
                time.sleep(0.01)
                continue
            pubsub_msg = gcs_utils.PubSubMessage.FromString(msg["data"])
            error_data = gcs_utils.ErrorTableData.FromString(pubsub_msg.data)
        if error_type is None or error_type == error_data.type:
            msgs.append(error_data)
        else:
            time.sleep(0.01)

    return msgs


def init_log_pubsub():
    """Initialize redis error info pub/sub"""
    if gcs_pubsub_enabled():
        s = GcsLogSubscriber(
            address=ray.worker.global_worker.gcs_client.address)
        s.subscribe()
    else:
        s = ray.worker.global_worker.redis_client.pubsub(
            ignore_subscribe_messages=True)
        s.psubscribe(gcs_utils.LOG_FILE_CHANNEL)
    return s


def get_log_message(subscriber,
                    num: int = 1e6,
                    timeout: float = 20,
                    job_id: Optional[str] = None,
                    matcher=None) -> List[str]:
    """Gets log lines through GCS / Redis subscriber.

    Returns maximum `num` lines of log messages, within `timeout`.

    If `job_id` or `match` is specified, only returns log lines from `job_id`
    or when `matcher` is true.
    """
    deadline = time.time() + timeout
    msgs = []
    while time.time() < deadline and len(msgs) < num:
        if isinstance(subscriber, GcsLogSubscriber):
            logs_data = subscriber.poll(timeout=deadline - time.time())
            if not logs_data:
                # Timed out before any data is received.
                break
        else:
            msg = subscriber.get_message()
            if msg is None:
                time.sleep(0.01)
                continue
            logs_data = json.loads(ray._private.utils.decode(msg["data"]))

        if job_id and job_id != logs_data["job"]:
            continue
        if matcher and all(not matcher(line) for line in logs_data["lines"]):
            continue
        msgs.extend(logs_data["lines"])

    return msgs


def get_log_batch(subscriber,
                  num: int,
                  timeout: float = 20,
                  job_id: Optional[str] = None,
                  matcher=None) -> List[str]:
    """Gets log batches through GCS / Redis subscriber.

    Returns maximum `num` batches of logs. Each batch is a dict that includes
    metadata such as `pid`, `job_id`, and `lines` of log messages.

    If `job_id` or `match` is specified, only returns log batches from `job_id`
    or when `matcher` is true.
    """
    deadline = time.time() + timeout
    batches = []
    while time.time() < deadline and len(batches) < num:
        if isinstance(subscriber, GcsLogSubscriber):
            logs_data = subscriber.poll(timeout=deadline - time.time())
            if not logs_data:
                # Timed out before any data is received.
                break
        else:
            msg = subscriber.get_message()
            if msg is None:
                time.sleep(0.01)
                continue
            logs_data = json.loads(ray._private.utils.decode(msg["data"]))

        if job_id and job_id != logs_data["job"]:
            continue
        if matcher and not matcher(logs_data):
            continue
        batches.append(logs_data)

    return batches


def format_web_url(url):
    """Format web url."""
    url = url.replace("localhost", "http://127.0.0.1")
    if not url.startswith("http://"):
        return "http://" + url
    return url


def client_test_enabled() -> bool:
    return ray._private.client_mode_hook.is_client_mode_enabled


def object_memory_usage() -> bool:
    """Returns the number of bytes used in the object store."""
    total = ray.cluster_resources().get("object_store_memory", 0)
    avail = ray.available_resources().get("object_store_memory", 0)
    return total - avail


def fetch_prometheus(prom_addresses):
    # Local import so minimal dependency tests can run without requests
    import requests

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
    path = pathlib.Path(here)
    grandparent = path.parent.parent
    config_path = os.path.join(grandparent, "tests/test_cli_patterns",
                               config_file_name)
    config = yaml.safe_load(open(config_path).read())
    return config


def set_setup_func():
    import ray._private.runtime_env as runtime_env
    runtime_env.VAR = "hello world"


class BatchQueue(Queue):
    def __init__(self, maxsize: int = 0,
                 actor_options: Optional[Dict] = None) -> None:
        actor_options = actor_options or {}
        self.maxsize = maxsize
        self.actor = ray.remote(_BatchQueueActor).options(
            **actor_options).remote(self.maxsize)

    def get_batch(self,
                  batch_size: int = None,
                  total_timeout: Optional[float] = None,
                  first_timeout: Optional[float] = None) -> List[Any]:
        """Gets batch of items from the queue and returns them in a
        list in order.

        Raises:
            Empty: if the queue does not contain the desired number of items
        """
        return ray.get(
            self.actor.get_batch.remote(batch_size, total_timeout,
                                        first_timeout))


class _BatchQueueActor(_QueueActor):
    async def get_batch(self,
                        batch_size=None,
                        total_timeout=None,
                        first_timeout=None):
        start = timeit.default_timer()
        try:
            first = await asyncio.wait_for(self.queue.get(), first_timeout)
            batch = [first]
            if total_timeout:
                end = timeit.default_timer()
                total_timeout = max(total_timeout - (end - start), 0)
        except asyncio.TimeoutError:
            raise Empty
        if batch_size is None:
            if total_timeout is None:
                total_timeout = 0
            while True:
                try:
                    start = timeit.default_timer()
                    batch.append(await asyncio.wait_for(
                        self.queue.get(), total_timeout))
                    if total_timeout:
                        end = timeit.default_timer()
                        total_timeout = max(total_timeout - (end - start), 0)
                except asyncio.TimeoutError:
                    break
        else:
            for _ in range(batch_size - 1):
                try:
                    start = timeit.default_timer()
                    batch.append(await asyncio.wait_for(
                        self.queue.get(), total_timeout))
                    if total_timeout:
                        end = timeit.default_timer()
                        total_timeout = max(total_timeout - (end - start), 0)
                except asyncio.TimeoutError:
                    break
        return batch


def is_placement_group_removed(pg):
    table = ray.util.placement_group_table(pg)
    if "state" not in table:
        return False
    return table["state"] == "REMOVED"


def placement_group_assert_no_leak(pgs_created):
    for pg in pgs_created:
        ray.util.remove_placement_group(pg)

    def wait_for_pg_removed():
        for pg_entry in ray.util.placement_group_table().values():
            if pg_entry["state"] != "REMOVED":
                return False
        return True

    wait_for_condition(wait_for_pg_removed)

    cluster_resources = ray.cluster_resources()
    cluster_resources.pop("memory")
    cluster_resources.pop("object_store_memory")

    def wait_for_resource_recovered():
        for resource, val in ray.available_resources().items():
            if (resource in cluster_resources
                    and cluster_resources[resource] != val):
                return False
            if "_group_" in resource:
                return False
        return True

    wait_for_condition(wait_for_resource_recovered)


def monitor_memory_usage(print_interval_s: int = 30,
                         record_interval_s: int = 5,
                         warning_threshold: float = 0.9):
    """Run the memory monitor actor that prints the memory usage.

    The monitor will run on the same node as this function is called.

    Params:
        interval_s (int): The interval memory usage information is printed
        warning_threshold (float): The threshold where the
            memory usage warning is printed.

    Returns:
        The memory monitor actor.
    """
    assert ray.is_initialized(), (
        "The API is only available when Ray is initialized.")

    @ray.remote(num_cpus=0)
    class MemoryMonitorActor:
        def __init__(self,
                     print_interval_s: float = 20,
                     record_interval_s: float = 5,
                     warning_threshold: float = 0.9,
                     n: int = 10):
            """The actor that monitor the memory usage of the cluster.

            Params:
                print_interval_s (float): The interval where
                    memory usage is printed.
                record_interval_s (float): The interval where
                    memory usage is recorded.
                warning_threshold (float): The threshold where
                    memory warning is printed
                n (int): When memory usage is printed,
                    top n entries are printed.
            """
            # -- Interval the monitor prints the memory usage information. --
            self.print_interval_s = print_interval_s
            # -- Interval the monitor records the memory usage information. --
            self.record_interval_s = record_interval_s
            # -- Whether or not the monitor is running. --
            self.is_running = False
            # -- The used_gb/total_gb threshold where warning message omits. --
            self.warning_threshold = warning_threshold
            # -- The monitor that calculates the memory usage of the node. --
            self.monitor = memory_monitor.MemoryMonitor()
            # -- The top n memory usage of processes are printed. --
            self.n = n
            # -- The peak memory usage in GB during lifetime of monitor. --
            self.peak_memory_usage = 0
            # -- The top n memory usage of processes
            # during peak memory usage. --
            self.peak_top_n_memory_usage = ""
            # -- The last time memory usage was printed --
            self._last_print_time = 0
            # -- logger. --
            logging.basicConfig(level=logging.INFO)

        def ready(self):
            pass

        async def run(self):
            """Run the monitor.
            """
            self.is_running = True
            while self.is_running:
                now = time.time()
                used_gb, total_gb = self.monitor.get_memory_usage()
                top_n_memory_usage = memory_monitor.get_top_n_memory_usage(
                    n=self.n)
                if used_gb > self.peak_memory_usage:
                    self.peak_memory_usage = used_gb
                    self.peak_top_n_memory_usage = top_n_memory_usage

                if used_gb > total_gb * self.warning_threshold:
                    logging.warning("The memory usage is high: "
                                    f"{used_gb / total_gb * 100}%")
                if now - self._last_print_time > self.print_interval_s:
                    logging.info(f"Memory usage: {used_gb} / {total_gb}")
                    logging.info(f"Top {self.n} process memory usage:")
                    logging.info(top_n_memory_usage)
                    self._last_print_time = now
                await asyncio.sleep(self.record_interval_s)

        async def stop_run(self):
            """Stop running the monitor.

            Returns:
                True if the monitor is stopped. False otherwise.
            """
            was_running = self.is_running
            self.is_running = False
            return was_running

        async def get_peak_memory_info(self):
            """Return the tuple of the peak memory usage and the
                top n process information during the peak memory usage.
            """
            return self.peak_memory_usage, self.peak_top_n_memory_usage

    current_node_ip = ray.worker.global_worker.node_ip_address
    # Schedule the actor on the current node.
    memory_monitor_actor = MemoryMonitorActor.options(resources={
        f"node:{current_node_ip}": 0.001
    }).remote(
        print_interval_s=print_interval_s,
        record_interval_s=record_interval_s,
        warning_threshold=warning_threshold)
    print("Waiting for memory monitor actor to be ready...")
    ray.get(memory_monitor_actor.ready.remote())
    print("Memory monitor actor is ready now.")
    memory_monitor_actor.run.remote()
    return memory_monitor_actor


def setup_tls():
    """Sets up required environment variables for tls"""
    import pytest
    if sys.platform == "darwin":
        pytest.skip("Cryptography doesn't install in Mac build pipeline")
    cert, key = generate_self_signed_tls_certs()
    temp_dir = tempfile.mkdtemp("ray-test-certs")
    cert_filepath = os.path.join(temp_dir, "server.crt")
    key_filepath = os.path.join(temp_dir, "server.key")
    with open(cert_filepath, "w") as fh:
        fh.write(cert)
    with open(key_filepath, "w") as fh:
        fh.write(key)

    os.environ["RAY_USE_TLS"] = "1"
    os.environ["RAY_TLS_SERVER_CERT"] = cert_filepath
    os.environ["RAY_TLS_SERVER_KEY"] = key_filepath
    os.environ["RAY_TLS_CA_CERT"] = cert_filepath

    return key_filepath, cert_filepath, temp_dir


def teardown_tls(key_filepath, cert_filepath, temp_dir):
    os.remove(key_filepath)
    os.remove(cert_filepath)
    os.removedirs(temp_dir)
    del os.environ["RAY_USE_TLS"]
    del os.environ["RAY_TLS_SERVER_CERT"]
    del os.environ["RAY_TLS_SERVER_KEY"]
    del os.environ["RAY_TLS_CA_CERT"]


def get_and_run_node_killer(node_kill_interval_s,
                            namespace=None,
                            lifetime=None,
                            no_start=False,
                            max_nodes_to_kill=2):
    assert ray.is_initialized(), (
        "The API is only available when Ray is initialized.")

    @ray.remote(num_cpus=0)
    class NodeKillerActor:
        def __init__(self,
                     head_node_id,
                     node_kill_interval_s: float = 60,
                     max_nodes_to_kill: int = 2):
            self.node_kill_interval_s = node_kill_interval_s
            self.is_running = False
            self.head_node_id = head_node_id
            self.killed_nodes = set()
            self.done = asyncio.get_event_loop().create_future()
            self.max_nodes_to_kill = max_nodes_to_kill
            # -- logger. --
            logging.basicConfig(level=logging.INFO)

        def ready(self):
            pass

        async def run(self):
            self.is_running = True
            while self.is_running:
                node_to_kill_ip = None
                node_to_kill_port = None
                while node_to_kill_port is None and self.is_running:
                    nodes = ray.nodes()
                    alive_nodes = self._get_alive_nodes(nodes)
                    for node in nodes:
                        node_id = node["NodeID"]
                        # make sure at least 1 worker node is alive.
                        if (node["Alive"] and node_id != self.head_node_id
                                and node_id not in self.killed_nodes
                                and alive_nodes > 2):
                            node_to_kill_ip = node["NodeManagerAddress"]
                            node_to_kill_port = node["NodeManagerPort"]
                            break
                    # Give the cluster some time to start.
                    await asyncio.sleep(0.1)

                if not self.is_running:
                    break

                sleep_interval = np.random.rand() * self.node_kill_interval_s
                time.sleep(sleep_interval)

                if node_to_kill_port is not None:
                    try:
                        self._kill_raylet(
                            node_to_kill_ip, node_to_kill_port, graceful=False)
                    except Exception:
                        pass
                    logging.info(
                        f"Killed node {node_id} at address: "
                        f"{node_to_kill_ip}, port: {node_to_kill_port}")
                    self.killed_nodes.add(node_id)
                if len(self.killed_nodes) >= self.max_nodes_to_kill:
                    break
                await asyncio.sleep(self.node_kill_interval_s - sleep_interval)

            self.done.set_result(True)

        async def stop_run(self):
            was_running = self.is_running
            self.is_running = False
            return was_running

        async def get_total_killed_nodes(self):
            """Get the total number of killed nodes"""
            await self.done
            return self.killed_nodes

        def _kill_raylet(self, ip, port, graceful=False):
            raylet_address = f"{ip}:{port}"
            channel = grpc.insecure_channel(raylet_address)
            stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
            try:
                stub.ShutdownRaylet(
                    node_manager_pb2.ShutdownRayletRequest(graceful=graceful))
            except _InactiveRpcError:
                assert not graceful

        def _get_alive_nodes(self, nodes):
            alive_nodes = 0
            for node in nodes:
                if node["Alive"]:
                    alive_nodes += 1
            return alive_nodes

    head_node_ip = ray.worker.global_worker.node_ip_address
    head_node_id = ray.worker.global_worker.current_node_id.hex()
    # Schedule the actor on the current node.
    node_killer = NodeKillerActor.options(
        resources={
            f"node:{head_node_ip}": 0.001
        },
        namespace=namespace,
        name="node_killer",
        lifetime=lifetime).remote(
            head_node_id,
            node_kill_interval_s=node_kill_interval_s,
            max_nodes_to_kill=max_nodes_to_kill)
    print("Waiting for node killer actor to be ready...")
    ray.get(node_killer.ready.remote())
    print("Node killer actor is ready now.")
    if not no_start:
        node_killer.run.remote()
    return node_killer


@contextmanager
def chdir(d: str):
    old_dir = os.getcwd()
    os.chdir(d)
    yield
    os.chdir(old_dir)


def check_local_files_gced(cluster):
    for node in cluster.list_all_nodes():
        for subdir in [
                "conda", "pip", "working_dir_files", "py_modules_files"
        ]:
            all_files = os.listdir(
                os.path.join(node.get_runtime_env_dir_path(), subdir))
            # Check that there are no files remaining except for .lock files
            # and generated requirements.txt files.
            # TODO(architkulkarni): these files should get cleaned up too!
            if len(
                    list(
                        filter(lambda f: not f.endswith((".lock", ".txt")),
                               all_files))) > 0:
                print(str(all_files))
                return False

    return True


def generate_runtime_env_dict(field, spec_format, tmp_path, pip_list=None):
    if pip_list is None:
        pip_list = ["pip-install-test==0.5"]
    if field == "conda":
        conda_dict = {"dependencies": ["pip", {"pip": pip_list}]}
        if spec_format == "file":
            conda_file = tmp_path / f"environment-{hash(str(pip_list))}.yml"
            conda_file.write_text(yaml.dump(conda_dict))
            conda = str(conda_file)
        elif spec_format == "python_object":
            conda = conda_dict
        runtime_env = {"conda": conda}
    elif field == "pip":
        if spec_format == "file":
            pip_file = tmp_path / f"requirements-{hash(str(pip_list))}.txt"
            pip_file.write_text("\n".join(pip_list))
            pip = str(pip_file)
        elif spec_format == "python_object":
            pip = pip_list
        runtime_env = {"pip": pip}
    return runtime_env
