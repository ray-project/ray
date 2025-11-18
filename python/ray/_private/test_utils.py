import asyncio
import fnmatch
import io
import json
import logging
import os
import pathlib
import random
import socket
import subprocess
import sys
import tempfile
import threading
import time
import timeit
import traceback
import uuid
from collections import defaultdict
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

import requests
import yaml

import ray
import ray._private.memory_monitor as memory_monitor
import ray._private.services
import ray._private.services as services
import ray._private.utils
import ray.dashboard.consts as dashboard_consts
from ray._common.network_utils import build_address, parse_address
from ray._common.test_utils import wait_for_condition
from ray._common.utils import get_or_create_event_loop
from ray._private import (
    ray_constants,
)
from ray._private.internal_api import memory_summary
from ray._private.tls_utils import generate_self_signed_tls_certs
from ray._private.worker import RayContext
from ray._raylet import Config, GcsClient, GcsClientOptions, GlobalStateAccessor
from ray.core.generated import (
    gcs_pb2,
    gcs_service_pb2,
    node_manager_pb2,
)
from ray.util.queue import Empty, Queue, _QueueActor
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.util.state import get_actor, list_actors

import psutil  # We must import psutil after ray because we bundle it with ray.

logger = logging.getLogger(__name__)

EXE_SUFFIX = ".exe" if sys.platform == "win32" else ""
RAY_PATH = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
REDIS_EXECUTABLE = os.path.join(
    RAY_PATH, "core/src/ray/thirdparty/redis/src/redis-server" + EXE_SUFFIX
)

try:
    from prometheus_client.core import Metric
    from prometheus_client.parser import Sample, text_string_to_metric_families
except (ImportError, ModuleNotFoundError):
    Metric = None
    Sample = None

    def text_string_to_metric_families(*args, **kwargs):
        raise ModuleNotFoundError("`prometheus_client` not found")


def make_global_state_accessor(ray_context):
    gcs_options = GcsClientOptions.create(
        ray_context.address_info["gcs_address"],
        None,
        allow_cluster_id_nil=True,
        fetch_cluster_id_if_nil=False,
    )
    global_state_accessor = GlobalStateAccessor(gcs_options)
    global_state_accessor.connect()
    return global_state_accessor


def external_redis_test_enabled():
    return os.environ.get("TEST_EXTERNAL_REDIS") == "1"


def redis_replicas():
    return int(os.environ.get("TEST_EXTERNAL_REDIS_REPLICAS", "1"))


def redis_sentinel_replicas():
    return int(os.environ.get("TEST_EXTERNAL_REDIS_SENTINEL_REPLICAS", "2"))


def get_redis_cli(port, enable_tls):
    try:
        # If there is no redis libs installed, skip the check.
        # This could happen In minimal test, where we don't have
        # redis.
        import redis
    except Exception:
        return True

    params = {}
    if enable_tls:
        from ray._raylet import Config

        params = {"ssl": True, "ssl_cert_reqs": "required"}
        if Config.REDIS_CA_CERT():
            params["ssl_ca_certs"] = Config.REDIS_CA_CERT()
        if Config.REDIS_CLIENT_CERT():
            params["ssl_certfile"] = Config.REDIS_CLIENT_CERT()
        if Config.REDIS_CLIENT_KEY():
            params["ssl_keyfile"] = Config.REDIS_CLIENT_KEY()

    return redis.Redis("localhost", str(port), **params)


def start_redis_sentinel_instance(
    session_dir_path: str,
    port: int,
    redis_master_port: int,
    password: Optional[str] = None,
    enable_tls: bool = False,
    db_dir=None,
    free_port=0,
):
    config_file = os.path.join(
        session_dir_path, "redis-sentinel-" + uuid.uuid4().hex + ".conf"
    )
    config_lines = []
    # Port for this Sentinel instance
    if enable_tls:
        config_lines.append(f"port {free_port}")
    else:
        config_lines.append(f"port {port}")

    # Monitor the Redis master
    config_lines.append(f"sentinel monitor redis-test 127.0.0.1 {redis_master_port} 1")
    config_lines.append(
        "sentinel down-after-milliseconds redis-test 1000"
    )  # failover after 1 second
    config_lines.append("sentinel failover-timeout redis-test 5000")  #
    config_lines.append("sentinel parallel-syncs redis-test 1")

    if password:
        config_lines.append(f"sentinel auth-pass redis-test {password}")

    if enable_tls:
        config_lines.append(f"tls-port {port}")
        if Config.REDIS_CA_CERT():
            config_lines.append(f"tls-ca-cert-file {Config.REDIS_CA_CERT()}")
        # Check and add TLS client certificate file
        if Config.REDIS_CLIENT_CERT():
            config_lines.append(f"tls-cert-file {Config.REDIS_CLIENT_CERT()}")
        # Check and add TLS client key file
        if Config.REDIS_CLIENT_KEY():
            config_lines.append(f"tls-key-file {Config.REDIS_CLIENT_KEY()}")
        config_lines.append("tls-auth-clients no")
        config_lines.append("sentinel tls-auth-clients redis-test no")
    if db_dir:
        config_lines.append(f"dir {db_dir}")

    with open(config_file, "w") as f:
        f.write("\n".join(config_lines))

    command = [REDIS_EXECUTABLE, config_file, "--sentinel"]
    process_info = ray._private.services.start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_REDIS_SERVER,
        fate_share=False,
    )
    return process_info


def start_redis_instance(
    session_dir_path: str,
    port: int,
    redis_max_clients: Optional[int] = None,
    num_retries: int = 20,
    stdout_file: Optional[str] = None,
    stderr_file: Optional[str] = None,
    password: Optional[str] = None,
    fate_share: Optional[bool] = None,
    port_denylist: Optional[List[int]] = None,
    listen_to_localhost_only: bool = False,
    enable_tls: bool = False,
    replica_of=None,
    leader_id=None,
    db_dir=None,
    free_port=0,
):
    """Start a single Redis server.

    Notes:
        We will initially try to start the Redis instance at the given port,
        and then try at most `num_retries - 1` times to start the Redis
        instance at successive random ports.

    Args:
        session_dir_path: Path to the session directory of
            this Ray cluster.
        port: Try to start a Redis server at this port.
        redis_max_clients: If this is provided, Ray will attempt to configure
            Redis with this maxclients number.
        num_retries: The number of times to attempt to start Redis at
            successive ports.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        password: Prevents external clients without the password
            from connecting to Redis if provided.
        port_denylist: A set of denylist ports that shouldn't
            be used when allocating a new port.
        listen_to_localhost_only: Redis server only listens to
            localhost (127.0.0.1) if it's true,
            otherwise it listens to all network interfaces.
        enable_tls: Enable the TLS/SSL in Redis or not

    Returns:
        A tuple of the port used by Redis and ProcessInfo for the process that
            was started. If a port is passed in, then the returned port value
            is the same.

    Raises:
        Exception: An exception is raised if Redis could not be started.
    """

    assert os.path.isfile(REDIS_EXECUTABLE)

    # Construct the command to start the Redis server.
    command = [REDIS_EXECUTABLE]
    if password:
        if " " in password:
            raise ValueError("Spaces not permitted in redis password.")
        command += ["--requirepass", password]
    if redis_replicas() > 1:
        command += ["--cluster-enabled", "yes", "--cluster-config-file", f"node-{port}"]
    if enable_tls:
        command += [
            "--tls-port",
            str(port),
            "--loglevel",
            "warning",
            "--port",
            str(free_port),
        ]
    else:
        command += ["--port", str(port), "--loglevel", "warning"]

    if listen_to_localhost_only:
        command += ["--bind", "127.0.0.1"]
    pidfile = os.path.join(session_dir_path, "redis-" + uuid.uuid4().hex + ".pid")
    command += ["--pidfile", pidfile]
    if enable_tls:
        if Config.REDIS_CA_CERT():
            command += ["--tls-ca-cert-file", Config.REDIS_CA_CERT()]
        if Config.REDIS_CLIENT_CERT():
            command += ["--tls-cert-file", Config.REDIS_CLIENT_CERT()]
        if Config.REDIS_CLIENT_KEY():
            command += ["--tls-key-file", Config.REDIS_CLIENT_KEY()]
        if replica_of is not None:
            command += ["--tls-replication", "yes"]
        command += ["--tls-auth-clients", "no", "--tls-cluster", "yes"]
    if sys.platform != "win32":
        command += ["--save", "", "--appendonly", "no"]
    if db_dir is not None:
        command += ["--dir", str(db_dir)]

    process_info = ray._private.services.start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_REDIS_SERVER,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=fate_share,
    )
    node_id = None
    if redis_replicas() > 1:
        # Setup redis cluster
        import redis

        while True:
            try:
                redis_cli = get_redis_cli(port, enable_tls)
                if replica_of is None:
                    slots = [str(i) for i in range(16384)]
                    redis_cli.cluster("addslots", *slots)
                else:
                    logger.info(redis_cli.cluster("meet", "127.0.0.1", str(replica_of)))
                    logger.info(redis_cli.cluster("replicate", leader_id))
                node_id = redis_cli.cluster("myid")
                break
            except (
                redis.exceptions.ConnectionError,
                redis.exceptions.ResponseError,
            ) as e:
                from time import sleep

                logger.info(
                    f"Waiting for redis to be up. Check failed with error: {e}. "
                    "Will retry in 0.1s"
                )

                if process_info.process.poll() is not None:
                    raise Exception(
                        f"Redis process exited unexpectedly: {process_info}. "
                        f"Exit code: {process_info.process.returncode}"
                    )

                sleep(0.1)

    logger.info(
        f"Redis started with node_id {node_id} and pid {process_info.process.pid}"
    )

    return node_id, process_info


def _pid_alive(pid):
    """Check if the process with this PID is alive or not.

    Args:
        pid: The pid to check.

    Returns:
        This returns false if the process is dead. Otherwise, it returns true.
    """
    alive = True
    try:
        proc = psutil.Process(pid)
        if proc.status() == psutil.STATUS_ZOMBIE:
            alive = False
    except psutil.NoSuchProcess:
        alive = False
    return alive


def _check_call_windows(main, argv, capture_stdout=False, capture_stderr=False):
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


def check_call_subprocess(argv, capture_stdout=False, capture_stderr=False):
    # We use this function instead of calling the "ray" command to work around
    # some deadlocks that occur when piping ray's output on Windows
    from ray.scripts.scripts import main as ray_main

    if sys.platform == "win32":
        result = _check_call_windows(
            ray_main, argv, capture_stdout=capture_stdout, capture_stderr=capture_stderr
        )
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
            raise subprocess.CalledProcessError(proc.returncode, argv, stdout, stderr)
        result = b"".join([s for s in [stdout, stderr] if s is not None])
    return result


def check_call_ray(args, capture_stdout=False, capture_stderr=False):
    check_call_subprocess(["ray"] + args, capture_stdout, capture_stderr)


def wait_for_dashboard_agent_available(cluster):
    gcs_client = GcsClient(address=cluster.address)

    def get_dashboard_agent_address():
        return gcs_client.internal_kv_get(
            f"{dashboard_consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{cluster.head_node.node_id}".encode(),
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
            timeout=dashboard_consts.GCS_RPC_TIMEOUT_SECONDS,
        )

    wait_for_condition(lambda: get_dashboard_agent_address() is not None)


def wait_for_pid_to_exit(pid: int, timeout: float = 20):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if not _pid_alive(pid):
            return
        time.sleep(0.1)
    raise TimeoutError(f"Timed out while waiting for process {pid} to exit.")


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
    raise TimeoutError(
        f"Timed out while waiting for process {pid} children to start "
        f"({num_alive}/{num_children} started: {alive})."
    )


def wait_for_children_of_pid_to_exit(pid, timeout=20):
    children = psutil.Process(pid).children()
    if len(children) == 0:
        return

    _, alive = psutil.wait_procs(children, timeout=timeout)
    if len(alive) > 0:
        raise TimeoutError(
            "Timed out while waiting for process children to exit."
            " Children still alive: {}.".format([p.name() for p in alive])
        )


def kill_process_by_name(name, SIGKILL=False):
    for p in psutil.process_iter(attrs=["name"]):
        if p.info["name"] == name + ray._private.services.EXE_SUFFIX:
            if SIGKILL:
                p.kill()
            else:
                p.terminate()


def run_string_as_driver(driver_script: str, env: Dict = None, encode: str = "utf-8"):
    """Run a driver as a separate process.

    Args:
        driver_script: A string to run as a Python script.
        env: The environment variables for the driver.

    Returns:
        The script's output.
    """
    proc = subprocess.Popen(
        [sys.executable, "-"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
    )
    with proc:
        output = proc.communicate(driver_script.encode(encoding=encode))[0]
        if proc.returncode:
            print(ray._common.utils.decode(output, encode_type=encode))
            logger.error(proc.stderr)
            raise subprocess.CalledProcessError(
                proc.returncode, proc.args, output, proc.stderr
            )
        out = ray._common.utils.decode(output, encode_type=encode)
    return out


def run_string_as_driver_stdout_stderr(
    driver_script: str, env: Dict = None, encode: str = "utf-8"
) -> Tuple[str, str]:
    """Run a driver as a separate process.

    Args:
        driver_script: A string to run as a Python script.
        env: The environment variables for the driver.

    Returns:
        The script's stdout and stderr.
    """
    proc = subprocess.Popen(
        [sys.executable, "-"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    with proc:
        outputs_bytes = proc.communicate(driver_script.encode(encoding=encode))
        out_str, err_str = [
            ray._common.utils.decode(output, encode_type=encode)
            for output in outputs_bytes
        ]
        if proc.returncode:
            print(out_str)
            print(err_str)
            raise subprocess.CalledProcessError(
                proc.returncode, proc.args, out_str, err_str
            )
        return out_str, err_str


def run_string_as_driver_nonblocking(driver_script, env: Dict = None):
    """Start a driver as a separate process and return immediately.

    Args:
        driver_script: A string to run as a Python script.

    Returns:
        A handle to the driver process.
    """
    script = "; ".join(
        [
            "import sys",
            "script = sys.stdin.read()",
            "sys.stdin.close()",
            "del sys",
            'exec("del script\\n" + script)',
        ]
    )
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
    return gcs_pb2.ActorTableData.ActorState.DESCRIPTOR.values_by_number[state].name


def wait_for_num_actors(num_actors, state=None, timeout=10):
    state = convert_actor_state(state)
    start_time = time.time()
    while time.time() - start_time < timeout:
        if (
            len(
                list_actors(
                    filters=[("state", "=", state)] if state else None,
                    limit=num_actors,
                )
            )
            >= num_actors
        ):
            return
        time.sleep(0.1)
    raise TimeoutError("Timed out while waiting for global state.")


def kill_actor_and_wait_for_failure(actor, timeout=10, retry_interval_ms=100):
    actor_id = actor._actor_id.hex()
    current_num_restarts = get_actor(id=actor_id).num_restarts
    ray.kill(actor)
    start = time.time()
    while time.time() - start <= timeout:
        actor_state = get_actor(id=actor_id)
        if (
            actor_state.state == "DEAD"
            or actor_state.num_restarts > current_num_restarts
        ):
            return
        time.sleep(retry_interval_ms / 1000.0)
    raise RuntimeError("It took too much time to kill an actor: {}".format(actor_id))


def wait_for_assertion(
    assertion_predictor: Callable,
    timeout: int = 10,
    retry_interval_ms: int = 100,
    raise_exceptions: bool = False,
    **kwargs: Any,
):
    """Wait until an assertion is met or time out with an exception.

    Args:
        assertion_predictor: A function that predicts the assertion.
        timeout: Maximum timeout in seconds.
        retry_interval_ms: Retry interval in milliseconds.
        raise_exceptions: If true, exceptions that occur while executing
            assertion_predictor won't be caught and instead will be raised.
        **kwargs: Arguments to pass to the condition_predictor.

    Raises:
        RuntimeError: If the assertion is not met before the timeout expires.
    """

    def _assertion_to_condition():
        try:
            assertion_predictor(**kwargs)
            return True
        except AssertionError:
            return False

    try:
        wait_for_condition(
            _assertion_to_condition,
            timeout=timeout,
            retry_interval_ms=retry_interval_ms,
            raise_exceptions=raise_exceptions,
            **kwargs,
        )
    except RuntimeError:
        assertion_predictor(**kwargs)  # Should fail assert


@dataclass
class MetricSamplePattern:
    name: Optional[str] = None
    value: Optional[str] = None
    partial_label_match: Optional[Dict[str, str]] = None

    def matches(self, sample: Sample):
        if self.name is not None:
            if self.name != sample.name:
                return False

        if self.value is not None:
            if self.value != sample.value:
                return False

        if self.partial_label_match is not None:
            for label, value in self.partial_label_match.items():
                if sample.labels.get(label) != value:
                    return False

        return True


@dataclass
class PrometheusTimeseries:
    """A collection of timeseries from multiple addresses. Each timeseries is a
    collection of samples with the same metric name and labels. Concretely:
    - components_dict: a dictionary of addresses to the Component labels
    - metric_descriptors: a dictionary of metric names to the Metric object
    - metric_samples: the latest value of each label
    """

    components_dict: Dict[str, Set[str]] = field(default_factory=dict)
    metric_descriptors: Dict[str, Metric] = field(default_factory=dict)
    metric_samples: Dict[frozenset, Sample] = field(default_factory=dict)

    def flush(self):
        self.components_dict.clear()
        self.metric_descriptors.clear()
        self.metric_samples.clear()


def get_metric_check_condition(
    metrics_to_check: List[MetricSamplePattern],
    timeseries: PrometheusTimeseries,
    export_addr: Optional[str] = None,
) -> Callable[[], bool]:
    """A condition to check if a prometheus metrics reach a certain value.

    This is a blocking check that can be passed into a `wait_for_condition`
    style function.

    Args:
        metrics_to_check: A list of MetricSamplePattern. The fields that
            aren't `None` will be matched.
        timeseries: A PrometheusTimeseries object to store the metrics.
        export_addr: Optional address to export metrics to.

    Returns:
        A function that returns True if all the metrics are emitted.
    """
    node_info = ray.nodes()[0]
    metrics_export_port = node_info["MetricsExportPort"]
    addr = node_info["NodeManagerAddress"]
    prom_addr = export_addr or build_address(addr, metrics_export_port)

    def f():
        for metric_pattern in metrics_to_check:
            metric_samples = fetch_prometheus_timeseries(
                [prom_addr], timeseries
            ).metric_samples.values()
            for metric_sample in metric_samples:
                if metric_pattern.matches(metric_sample):
                    break
            else:
                print(
                    f"Didn't find {metric_pattern}",
                    "all samples",
                    metric_samples,
                )
                return False
        return True

    return f


def wait_until_succeeded_without_exception(
    func, exceptions, *args, timeout_ms=1000, retry_interval_ms=100, raise_last_ex=False
):
    """A helper function that waits until a given function
        completes without exceptions.

    Args:
        func: A function to run.
        exceptions: Exceptions that are supposed to occur.
        args: arguments to pass for a given func
        timeout_ms: Maximum timeout in milliseconds.
        retry_interval_ms: Retry interval in milliseconds.
        raise_last_ex: Raise the last exception when timeout.

    Return:
        Whether exception occurs within a timeout.
    """
    if isinstance(type(exceptions), tuple):
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
        ex_stack = (
            traceback.format_exception(type(last_ex), last_ex, last_ex.__traceback__)
            if last_ex
            else []
        )
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


def wait_until_server_available(address, timeout_ms=5000, retry_interval_ms=100):
    ip, port_str = parse_address(address)
    port = int(port_str)
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
        node
        for node in cluster.list_all_nodes()
        if node._raylet_socket_name
        != ray._private.worker._global_node._raylet_socket_name
        and (exclude_head is False or node.head is False)
    ]


def get_non_head_nodes(cluster):
    """Get all non-head nodes."""
    return list(filter(lambda x: x.head is False, cluster.list_all_nodes()))


def init_error_pubsub():
    """Initialize error info pub/sub"""
    s = ray._raylet.GcsErrorSubscriber(
        address=ray._private.worker.global_worker.gcs_client.address
    )
    s.subscribe()
    return s


def get_error_message(subscriber, num=1e6, error_type=None, timeout=20):
    """Gets errors from GCS subscriber.

    Returns maximum `num` error strings within `timeout`.
    Only returns errors of `error_type` if specified.
    """
    deadline = time.time() + timeout
    msgs = []
    while time.time() < deadline and len(msgs) < num:
        _, error_data = subscriber.poll(timeout=deadline - time.time())
        if not error_data:
            # Timed out before any data is received.
            break
        if error_type is None or error_type == error_data["type"]:
            msgs.append(error_data)
        else:
            time.sleep(0.01)

    return msgs


def init_log_pubsub():
    """Initialize log pub/sub"""
    s = ray._raylet.GcsLogSubscriber(
        address=ray._private.worker.global_worker.gcs_client.address
    )
    s.subscribe()
    return s


def get_log_data(
    subscriber,
    num: int = 1e6,
    timeout: float = 20,
    job_id: Optional[str] = None,
    matcher=None,
) -> List[dict]:
    deadline = time.time() + timeout
    msgs = []
    while time.time() < deadline and len(msgs) < num:
        logs_data = subscriber.poll(timeout=deadline - time.time())
        if not logs_data:
            # Timed out before any data is received.
            break
        if job_id and job_id != logs_data["job"]:
            continue
        if matcher and all(not matcher(line) for line in logs_data["lines"]):
            continue
        msgs.append(logs_data)
    return msgs


def get_log_message(
    subscriber,
    num: int = 1e6,
    timeout: float = 20,
    job_id: Optional[str] = None,
    matcher=None,
) -> List[List[str]]:
    """Gets log lines through GCS subscriber.

    Returns maximum `num` of log messages, within `timeout`.

    If `job_id` or `match` is specified, only returns log lines from `job_id`
    or when `matcher` is true.
    """
    msgs = get_log_data(subscriber, num, timeout, job_id, matcher)
    return [msg["lines"] for msg in msgs]


def get_log_sources(
    subscriber,
    num: int = 1e6,
    timeout: float = 20,
    job_id: Optional[str] = None,
    matcher=None,
):
    """Get the source of all log messages"""
    msgs = get_log_data(subscriber, num, timeout, job_id, matcher)
    return {msg["pid"] for msg in msgs}


def get_log_batch(
    subscriber,
    num: int,
    timeout: float = 20,
    job_id: Optional[str] = None,
    matcher=None,
) -> List[str]:
    """Gets log batches through GCS subscriber.

    Returns maximum `num` batches of logs. Each batch is a dict that includes
    metadata such as `pid`, `job_id`, and `lines` of log messages.

    If `job_id` or `match` is specified, only returns log batches from `job_id`
    or when `matcher` is true.
    """
    deadline = time.time() + timeout
    batches = []
    while time.time() < deadline and len(batches) < num:
        logs_data = subscriber.poll(timeout=deadline - time.time())
        if not logs_data:
            # Timed out before any data is received.
            break
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


def fetch_raw_prometheus(prom_addresses):
    # Local import so minimal dependency tests can run without requests
    import requests

    for address in prom_addresses:
        try:
            response = requests.get(f"http://{address}/metrics")
            yield address, response.text
        except requests.exceptions.ConnectionError:
            continue


def fetch_prometheus(prom_addresses):
    components_dict = {}
    metric_descriptors = {}
    metric_samples = []

    for address in prom_addresses:
        if address not in components_dict:
            components_dict[address] = set()

    for address, response in fetch_raw_prometheus(prom_addresses):
        for metric in text_string_to_metric_families(response):
            for sample in metric.samples:
                metric_descriptors[sample.name] = metric
                metric_samples.append(sample)
                if "Component" in sample.labels:
                    components_dict[address].add(sample.labels["Component"])
    return components_dict, metric_descriptors, metric_samples


def fetch_prometheus_timeseries(
    prom_addreses: List[str],
    result: PrometheusTimeseries,
) -> PrometheusTimeseries:
    components_dict, metric_descriptors, metric_samples = fetch_prometheus(
        prom_addreses
    )
    for address, components in components_dict.items():
        if address not in result.components_dict:
            result.components_dict[address] = set()
        result.components_dict[address].update(components)
    result.metric_descriptors.update(metric_descriptors)
    for sample in metric_samples:
        # udpate sample to the latest value
        result.metric_samples[
            frozenset(list(sample.labels.items()) + [("_metric_name_", sample.name)])
        ] = sample
    return result


def fetch_prometheus_metrics(prom_addresses: List[str]) -> Dict[str, List[Any]]:
    """Return prometheus metrics from the given addresses.

    Args:
        prom_addresses: List of metrics_agent addresses to collect metrics from.

    Returns:
        Dict mapping from metric name to list of samples for the metric.
    """
    _, _, samples = fetch_prometheus(prom_addresses)
    samples_by_name = defaultdict(list)
    for sample in samples:
        samples_by_name[sample.name].append(sample)
    return samples_by_name


def fetch_prometheus_metric_timeseries(
    prom_addresses: List[str], result: PrometheusTimeseries
) -> Dict[str, List[Any]]:
    samples = fetch_prometheus_timeseries(
        prom_addresses, result
    ).metric_samples.values()
    samples_by_name = defaultdict(list)
    for sample in samples:
        samples_by_name[sample.name].append(sample)
    return samples_by_name


def raw_metric_timeseries(
    info: RayContext, result: PrometheusTimeseries
) -> Dict[str, List[Any]]:
    """Return prometheus timeseries from a RayContext"""
    metrics_page = "localhost:{}".format(info.address_info["metrics_export_port"])
    print("Fetch metrics from", metrics_page)
    return fetch_prometheus_metric_timeseries([metrics_page], result)


def get_system_metric_for_component(
    system_metric: str, component: str, prometheus_server_address: str
) -> List[float]:
    """Get the system metric for a given component from a Prometheus server address.
    Please note:
    - This function requires the availability of the Prometheus server. Therefore, it
    requires the server address.
    - It assumes the system metric has a `Component` label and `pid` label. `pid` is the
    process id, so it can be used to uniquely identify the process.
    """
    session_name = os.path.basename(
        ray._private.worker._global_node.get_session_dir_path()
    )
    query = f"sum({system_metric}{{Component='{component}',SessionName='{session_name}'}}) by (pid)"
    resp = requests.get(
        f"{prometheus_server_address}/api/v1/query?query={quote(query)}"
    )
    if resp.status_code != 200:
        raise Exception(f"Failed to query Prometheus: {resp.status_code}")
    result = resp.json()
    return [float(item["value"][1]) for item in result["data"]["result"]]


def get_test_config_path(config_file_name):
    """Resolve the test config path from the config file dir"""
    here = os.path.realpath(__file__)
    path = pathlib.Path(here)
    grandparent = path.parent.parent
    return os.path.join(grandparent, "tests/test_cli_patterns", config_file_name)


def load_test_config(config_file_name):
    """Loads a config yaml from tests/test_cli_patterns."""
    config_path = get_test_config_path(config_file_name)
    config = yaml.safe_load(open(config_path).read())
    return config


def set_setup_func():
    import ray._private.runtime_env as runtime_env

    runtime_env.VAR = "hello world"


class BatchQueue(Queue):
    def __init__(self, maxsize: int = 0, actor_options: Optional[Dict] = None) -> None:
        actor_options = actor_options or {}
        self.maxsize = maxsize
        self.actor = (
            ray.remote(_BatchQueueActor).options(**actor_options).remote(self.maxsize)
        )

    def get_batch(
        self,
        batch_size: int = None,
        total_timeout: Optional[float] = None,
        first_timeout: Optional[float] = None,
    ) -> List[Any]:
        """Gets batch of items from the queue and returns them in a
        list in order.

        Raises:
            Empty: if the queue does not contain the desired number of items
        """
        return ray.get(
            self.actor.get_batch.remote(batch_size, total_timeout, first_timeout)
        )


class _BatchQueueActor(_QueueActor):
    async def get_batch(self, batch_size=None, total_timeout=None, first_timeout=None):
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
                    batch.append(
                        await asyncio.wait_for(self.queue.get(), total_timeout)
                    )
                    if total_timeout:
                        end = timeit.default_timer()
                        total_timeout = max(total_timeout - (end - start), 0)
                except asyncio.TimeoutError:
                    break
        else:
            for _ in range(batch_size - 1):
                try:
                    start = timeit.default_timer()
                    batch.append(
                        await asyncio.wait_for(self.queue.get(), total_timeout)
                    )
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
            if resource in cluster_resources and cluster_resources[resource] != val:
                return False
            if "_group_" in resource:
                return False
        return True

    wait_for_condition(wait_for_resource_recovered)


def monitor_memory_usage(
    print_interval_s: int = 30,
    record_interval_s: int = 5,
    warning_threshold: float = 0.9,
):
    """Run the memory monitor actor that prints the memory usage.

    The monitor will run on the same node as this function is called.

    Params:
        interval_s: The interval memory usage information is printed
        warning_threshold: The threshold where the
            memory usage warning is printed.

    Returns:
        The memory monitor actor.
    """
    assert ray.is_initialized(), "The API is only available when Ray is initialized."

    @ray.remote(num_cpus=0)
    class MemoryMonitorActor:
        def __init__(
            self,
            print_interval_s: float = 20,
            record_interval_s: float = 5,
            warning_threshold: float = 0.9,
            n: int = 10,
        ):
            """The actor that monitor the memory usage of the cluster.

            Params:
                print_interval_s: The interval where
                    memory usage is printed.
                record_interval_s: The interval where
                    memory usage is recorded.
                warning_threshold: The threshold where
                    memory warning is printed
                n: When memory usage is printed,
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
            """Run the monitor."""
            self.is_running = True
            while self.is_running:
                now = time.time()
                used_gb, total_gb = self.monitor.get_memory_usage()
                top_n_memory_usage = memory_monitor.get_top_n_memory_usage(n=self.n)
                if used_gb > self.peak_memory_usage:
                    self.peak_memory_usage = used_gb
                    self.peak_top_n_memory_usage = top_n_memory_usage

                if used_gb > total_gb * self.warning_threshold:
                    logging.warning(
                        "The memory usage is high: " f"{used_gb / total_gb * 100}%"
                    )
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

    current_node_ip = ray._private.worker.global_worker.node_ip_address
    # Schedule the actor on the current node.
    memory_monitor_actor = MemoryMonitorActor.options(
        resources={f"node:{current_node_ip}": 0.001}
    ).remote(
        print_interval_s=print_interval_s,
        record_interval_s=record_interval_s,
        warning_threshold=warning_threshold,
    )
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


class ResourceKillerActor:
    """Abstract base class used to implement resource killers for chaos testing.

    Subclasses should implement _find_resource_to_kill, which should find a resource
    to kill. This method should return the args to _kill_resource, which is another
    abstract method that should kill the resource and add it to the `killed` set.
    """

    def __init__(
        self,
        head_node_id,
        kill_interval_s: float = 60,
        kill_delay_s: float = 0,
        max_to_kill: Optional[int] = 2,
        batch_size_to_kill: int = 1,
        kill_filter_fn: Optional[Callable] = None,
    ):
        self.kill_interval_s = kill_interval_s
        self.kill_delay_s = kill_delay_s
        self.is_running = False
        self.head_node_id = head_node_id
        self.killed = set()
        self.done = get_or_create_event_loop().create_future()
        self.max_to_kill = max_to_kill
        self.batch_size_to_kill = batch_size_to_kill
        self.kill_filter_fn = kill_filter_fn
        self.kill_immediately_after_found = False
        # -- logger. --
        logging.basicConfig(level=logging.INFO)

    def ready(self):
        pass

    async def run(self):
        self.is_running = True

        time.sleep(self.kill_delay_s)

        while self.is_running:
            to_kills = await self._find_resources_to_kill()

            if not self.is_running:
                break

            if self.kill_immediately_after_found:
                sleep_interval = 0
            else:
                sleep_interval = random.random() * self.kill_interval_s
                time.sleep(sleep_interval)

            for to_kill in to_kills:
                self._kill_resource(*to_kill)
            if self.max_to_kill is not None and len(self.killed) >= self.max_to_kill:
                break
            await asyncio.sleep(self.kill_interval_s - sleep_interval)

        self.done.set_result(True)
        await self.stop_run()

    async def _find_resources_to_kill(self):
        raise NotImplementedError

    def _kill_resource(self, *args):
        raise NotImplementedError

    async def stop_run(self):
        was_running = self.is_running
        if was_running:
            self._cleanup()

        self.is_running = False
        return was_running

    async def get_total_killed(self):
        """Get the total number of killed resources"""
        await self.done
        return self.killed

    def _cleanup(self):
        """Cleanup any resources created by the killer.

        Overriding this method is optional.
        """
        pass


class NodeKillerBase(ResourceKillerActor):
    async def _find_resources_to_kill(self):
        nodes_to_kill = []
        while not nodes_to_kill and self.is_running:
            worker_nodes = [
                node
                for node in ray.nodes()
                if node["Alive"]
                and (node["NodeID"] != self.head_node_id)
                and (node["NodeID"] not in self.killed)
            ]
            if self.kill_filter_fn:
                candidates = list(filter(self.kill_filter_fn(), worker_nodes))
            else:
                candidates = worker_nodes

            # Ensure at least one worker node remains alive.
            if len(worker_nodes) < self.batch_size_to_kill + 1:
                # Give the cluster some time to start.
                await asyncio.sleep(1)
                continue

            # Collect nodes to kill, limited by batch size.
            for candidate in candidates[: self.batch_size_to_kill]:
                nodes_to_kill.append(
                    (
                        candidate["NodeID"],
                        candidate["NodeManagerAddress"],
                        candidate["NodeManagerPort"],
                    )
                )

        return nodes_to_kill


@ray.remote(num_cpus=0)
class RayletKiller(NodeKillerBase):
    def _kill_resource(self, node_id, node_to_kill_ip, node_to_kill_port):
        if node_to_kill_port is not None:
            try:
                self._kill_raylet(node_to_kill_ip, node_to_kill_port, graceful=False)
            except Exception:
                pass
            logging.info(
                f"Killed node {node_id} at address: "
                f"{node_to_kill_ip}, port: {node_to_kill_port}"
            )
            self.killed.add(node_id)

    def _kill_raylet(self, ip, port, graceful=False):
        import grpc
        from grpc._channel import _InactiveRpcError

        from ray.core.generated import node_manager_pb2_grpc

        raylet_address = build_address(ip, port)
        channel = grpc.insecure_channel(raylet_address)
        stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
        try:
            stub.ShutdownRaylet(
                node_manager_pb2.ShutdownRayletRequest(graceful=graceful)
            )
        except _InactiveRpcError:
            assert not graceful


@ray.remote(num_cpus=0)
class EC2InstanceTerminator(NodeKillerBase):
    def _kill_resource(self, node_id, node_to_kill_ip, _):
        if node_to_kill_ip is not None:
            try:
                _terminate_ec2_instance(node_to_kill_ip)
            except Exception:
                pass
            logging.info(f"Terminated instance, {node_id=}, address={node_to_kill_ip}")
            self.killed.add(node_id)


@ray.remote(num_cpus=0)
class EC2InstanceTerminatorWithGracePeriod(NodeKillerBase):
    def __init__(self, *args, grace_period_s: int = 30, **kwargs):
        super().__init__(*args, **kwargs)

        self._grace_period_s = grace_period_s
        self._kill_threads: Set[threading.Thread] = set()

    def _kill_resource(self, node_id, node_to_kill_ip, _):
        assert node_id not in self.killed

        # Clean up any completed threads.
        for thread in self._kill_threads.copy():
            if not thread.is_alive():
                thread.join()
                self._kill_threads.remove(thread)

        def _kill_node_with_grace_period(node_id, node_to_kill_ip):
            self._drain_node(node_id)
            time.sleep(self._grace_period_s)
            # Anyscale extends the drain deadline if you shut down the instance
            # directly. To work around this, we force-stop Ray on the node. Anyscale
            # should then terminate it shortly after without updating the drain
            # deadline.
            _execute_command_on_node("ray stop --force", node_to_kill_ip)

        logger.info(f"Starting killing thread {node_id=}, {node_to_kill_ip=}")
        thread = threading.Thread(
            target=_kill_node_with_grace_period,
            args=(node_id, node_to_kill_ip),
            daemon=True,
        )
        thread.start()
        self._kill_threads.add(thread)
        self.killed.add(node_id)

    def _drain_node(self, node_id: str) -> None:
        # We need to lazily import this object. Otherwise, Ray can't serialize the
        # class.
        from ray.core.generated import autoscaler_pb2

        assert ray.NodeID.from_hex(node_id) != ray.NodeID.nil()

        logging.info(f"Draining node {node_id=}")
        address = services.canonicalize_bootstrap_address_or_die(addr="auto")
        gcs_client = ray._raylet.GcsClient(address=address)
        deadline_timestamp_ms = (time.time_ns() // 1e6) + (self._grace_period_s * 1e3)

        try:
            is_accepted, _ = gcs_client.drain_node(
                node_id,
                autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
                "",
                deadline_timestamp_ms,
            )
        except ray.exceptions.RayError as e:
            logger.error(f"Failed to drain node {node_id=}")
            raise e

        assert is_accepted, "Drain node request was rejected"

    def _cleanup(self):
        for thread in self._kill_threads.copy():
            thread.join()
            self._kill_threads.remove(thread)

        assert not self._kill_threads


@ray.remote(num_cpus=0)
class WorkerKillerActor(ResourceKillerActor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Kill worker immediately so that the task does
        # not finish successfully on its own.
        self.kill_immediately_after_found = True

        from ray.util.state.api import StateApiClient
        from ray.util.state.common import ListApiOptions

        self.client = StateApiClient()
        self.task_options = ListApiOptions(
            filters=[
                ("state", "=", "RUNNING"),
                ("name", "!=", "WorkerKillActor.run"),
            ]
        )

    async def _find_resources_to_kill(self):
        from ray.util.state.common import StateResource

        process_to_kill_task_id = None
        process_to_kill_pid = None
        process_to_kill_node_id = None
        while process_to_kill_pid is None and self.is_running:
            tasks = self.client.list(
                StateResource.TASKS,
                options=self.task_options,
                raise_on_missing_output=False,
            )
            if self.kill_filter_fn is not None:
                tasks = list(filter(self.kill_filter_fn(), tasks))

            for task in tasks:
                if task.worker_id is not None and task.node_id is not None:
                    process_to_kill_task_id = task.task_id
                    process_to_kill_pid = task.worker_pid
                    process_to_kill_node_id = task.node_id
                    break

            # Give the cluster some time to start.
            await asyncio.sleep(0.1)

        return [(process_to_kill_task_id, process_to_kill_pid, process_to_kill_node_id)]

    def _kill_resource(
        self, process_to_kill_task_id, process_to_kill_pid, process_to_kill_node_id
    ):
        if process_to_kill_pid is not None:

            @ray.remote
            def kill_process(pid):
                import psutil

                proc = psutil.Process(pid)
                proc.kill()

            scheduling_strategy = (
                ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=process_to_kill_node_id,
                    soft=False,
                )
            )
            kill_process.options(scheduling_strategy=scheduling_strategy).remote(
                process_to_kill_pid
            )
            logging.info(
                f"Killing pid {process_to_kill_pid} on node {process_to_kill_node_id}"
            )
            # Store both task_id and pid because retried tasks have same task_id.
            self.killed.add((process_to_kill_task_id, process_to_kill_pid))


def get_and_run_resource_killer(
    resource_killer_cls,
    kill_interval_s,
    namespace=None,
    lifetime=None,
    no_start=False,
    max_to_kill=2,
    batch_size_to_kill=1,
    kill_delay_s=0,
    kill_filter_fn=None,
):
    assert ray.is_initialized(), "The API is only available when Ray is initialized."

    head_node_id = ray.get_runtime_context().get_node_id()
    # Schedule the actor on the current node.
    resource_killer = resource_killer_cls.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=head_node_id, soft=False
        ),
        namespace=namespace,
        name="ResourceKiller",
        lifetime=lifetime,
    ).remote(
        head_node_id,
        kill_interval_s=kill_interval_s,
        kill_delay_s=kill_delay_s,
        max_to_kill=max_to_kill,
        batch_size_to_kill=batch_size_to_kill,
        kill_filter_fn=kill_filter_fn,
    )
    print("Waiting for ResourceKiller to be ready...")
    ray.get(resource_killer.ready.remote())
    print("ResourceKiller is ready now.")
    if not no_start:
        resource_killer.run.remote()
    return resource_killer


def get_actor_node_id(actor_handle: "ray.actor.ActorHandle") -> str:
    return ray.get(
        actor_handle.__ray_call__.remote(
            lambda self: ray.get_runtime_context().get_node_id()
        )
    )


@contextmanager
def chdir(d: str):
    old_dir = os.getcwd()
    os.chdir(d)
    try:
        yield
    finally:
        os.chdir(old_dir)


def test_get_directory_size_bytes():
    with tempfile.TemporaryDirectory() as tmp_dir, chdir(tmp_dir):
        assert ray._private.utils.get_directory_size_bytes(tmp_dir) == 0
        with open("test_file", "wb") as f:
            f.write(os.urandom(100))
        assert ray._private.utils.get_directory_size_bytes(tmp_dir) == 100
        with open("test_file_2", "wb") as f:
            f.write(os.urandom(50))
        assert ray._private.utils.get_directory_size_bytes(tmp_dir) == 150
        os.mkdir("subdir")
        with open("subdir/subdir_file", "wb") as f:
            f.write(os.urandom(2))
        assert ray._private.utils.get_directory_size_bytes(tmp_dir) == 152


def check_local_files_gced(cluster):
    for node in cluster.list_all_nodes():
        for subdir in ["conda", "pip", "working_dir_files", "py_modules_files"]:
            all_files = os.listdir(
                os.path.join(node.get_runtime_env_dir_path(), subdir)
            )
            # Check that there are no files remaining except for .lock files
            # and generated requirements.txt files.
            # Note: On Windows the top folder is not deleted as it is in use.
            # TODO(architkulkarni): these files should get cleaned up too!
            items = list(filter(lambda f: not f.endswith((".lock", ".txt")), all_files))
            if len(items) > 0:
                print(f"runtime_env files not GC'd from subdir '{subdir}': {items}")
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


def check_spilled_mb(address, spilled=None, restored=None, fallback=None):
    def ok():
        s = memory_summary(address=address["address"], stats_only=True)
        print(s)
        if restored:
            if "Restored {} MiB".format(restored) not in s:
                return False
        else:
            if "Restored" in s:
                return False
        if spilled:
            if not isinstance(spilled, list):
                spilled_lst = [spilled]
            else:
                spilled_lst = spilled
            found = False
            for n in spilled_lst:
                if "Spilled {} MiB".format(n) in s:
                    found = True
            if not found:
                return False
        else:
            if "Spilled" in s:
                return False
        if fallback:
            if "Plasma filesystem mmap usage: {} MiB".format(fallback) not in s:
                return False
        else:
            if "Plasma filesystem mmap usage:" in s:
                return False
        return True

    wait_for_condition(ok, timeout=3, retry_interval_ms=1000)


def no_resource_leaks_excluding_node_resources():
    cluster_resources = ray.cluster_resources()
    available_resources = ray.available_resources()
    for r in ray.cluster_resources():
        if "node" in r:
            del cluster_resources[r]
            del available_resources[r]

    return cluster_resources == available_resources


def job_hook(**kwargs):
    """Function called by reflection by test_cli_integration."""
    cmd = " ".join(kwargs["entrypoint"])
    print(f"hook intercepted: {cmd}")
    sys.exit(0)


def wandb_setup_api_key_hook():
    """
    Example external hook to set up W&B API key in
    WandbIntegrationTest.testWandbLoggerConfig
    """
    return "abcd"


# Get node stats from node manager.
def get_node_stats(raylet, num_retry=5, timeout=2):
    import grpc

    from ray._private.grpc_utils import init_grpc_channel
    from ray.core.generated import node_manager_pb2_grpc

    raylet_address = build_address(
        raylet["NodeManagerAddress"], raylet["NodeManagerPort"]
    )
    channel = init_grpc_channel(raylet_address)
    stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
    for _ in range(num_retry):
        try:
            reply = stub.GetNodeStats(
                node_manager_pb2.GetNodeStatsRequest(), timeout=timeout
            )
            break
        except grpc.RpcError:
            continue
    assert reply is not None
    return reply


# Gets resource usage assuming gcs is local.
def get_resource_usage(gcs_address, timeout=10):
    from ray._private.grpc_utils import init_grpc_channel
    from ray.core.generated import gcs_service_pb2_grpc

    if not gcs_address:
        gcs_address = ray.worker._global_node.gcs_address

    gcs_channel = init_grpc_channel(
        gcs_address, ray_constants.GLOBAL_GRPC_OPTIONS, asynchronous=False
    )

    gcs_node_resources_stub = gcs_service_pb2_grpc.NodeResourceInfoGcsServiceStub(
        gcs_channel
    )

    request = gcs_service_pb2.GetAllResourceUsageRequest()
    response = gcs_node_resources_stub.GetAllResourceUsage(request, timeout=timeout)
    resources_batch_data = response.resource_usage_data

    return resources_batch_data


# Gets the load metrics report assuming gcs is local.
def get_load_metrics_report(webui_url):
    webui_url = format_web_url(webui_url)
    response = requests.get(f"{webui_url}/api/cluster_status")
    response.raise_for_status()
    return response.json()["data"]["clusterStatus"]["loadMetricsReport"]


# Send a RPC to the raylet to have it self-destruct its process.
def kill_raylet(raylet, graceful=False):
    import grpc
    from grpc._channel import _InactiveRpcError

    from ray.core.generated import node_manager_pb2_grpc

    raylet_address = build_address(
        raylet["NodeManagerAddress"], raylet["NodeManagerPort"]
    )
    channel = grpc.insecure_channel(raylet_address)
    stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
    try:
        stub.ShutdownRaylet(node_manager_pb2.ShutdownRayletRequest(graceful=graceful))
    except _InactiveRpcError:
        assert not graceful


def get_gcs_memory_used():
    import psutil

    m = {
        proc.info["name"]: proc.info["memory_info"].rss
        for proc in psutil.process_iter(["status", "name", "memory_info"])
        if (
            proc.info["status"] not in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD)
            and proc.info["name"] in ("gcs_server", "redis-server")
        )
    }
    assert "gcs_server" in m
    return sum(m.values())


def safe_write_to_results_json(
    result: dict,
    default_file_name: str = "/tmp/release_test_output.json",
    env_var: Optional[str] = "TEST_OUTPUT_JSON",
):
    """
    Safe (atomic) write to file to guard against malforming the json
    if the job gets interrupted in the middle of writing.
    """
    test_output_json = os.environ.get(env_var, default_file_name)
    test_output_json_tmp = f"{test_output_json}.tmp.{str(uuid.uuid4())}"
    with open(test_output_json_tmp, "wt") as f:
        json.dump(result, f)
        f.flush()
    os.replace(test_output_json_tmp, test_output_json)
    logger.info(f"Wrote results to {test_output_json}")
    logger.info(json.dumps(result))


def get_current_unused_port():
    """
    Returns a port number that is not currently in use.

    This is useful for testing when we need to bind to a port but don't
    care which one.

    Returns:
        A port number that is not currently in use. (Note that this port
        might become used by the time you try to bind to it.)
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind the socket to a local address with a random port number
    sock.bind(("localhost", 0))

    port = sock.getsockname()[1]
    sock.close()
    return port


# Global counter to test different return values
# for external_ray_cluster_activity_hook1.
ray_cluster_activity_hook_counter = 0
ray_cluster_activity_hook_5_counter = 0


def external_ray_cluster_activity_hook1():
    """
    Example external hook for test_component_activities_hook.
    Returns valid response and increments counter in `reason`
    field on each call.
    """
    global ray_cluster_activity_hook_counter
    ray_cluster_activity_hook_counter += 1

    from pydantic import BaseModel, Extra

    class TestRayActivityResponse(BaseModel, extra=Extra.allow):
        """
        Redefinition of dashboard.modules.api.api_head.RayActivityResponse
        used in test_component_activities_hook to mimic typical
        usage of redefining or extending response type.
        """

        is_active: str
        reason: Optional[str] = None
        timestamp: float

    return {
        "test_component1": TestRayActivityResponse(
            is_active="ACTIVE",
            reason=f"Counter: {ray_cluster_activity_hook_counter}",
            timestamp=datetime.now().timestamp(),
        )
    }


def external_ray_cluster_activity_hook2():
    """
    Example external hook for test_component_activities_hook.
    Returns invalid output because the value of `test_component2`
    should be of type RayActivityResponse.
    """
    return {"test_component2": "bad_output"}


def external_ray_cluster_activity_hook3():
    """
    Example external hook for test_component_activities_hook.
    Returns invalid output because return type is not
    Dict[str, RayActivityResponse]
    """
    return "bad_output"


def external_ray_cluster_activity_hook4():
    """
    Example external hook for test_component_activities_hook.
    Errors during execution.
    """
    raise Exception("Error in external cluster activity hook")


def external_ray_cluster_activity_hook5():
    """
    Example external hook for test_component_activities_hook.
    Returns valid response and increments counter in `reason`
    field on each call.
    """
    global ray_cluster_activity_hook_5_counter
    ray_cluster_activity_hook_5_counter += 1
    return {
        "test_component5": {
            "is_active": "ACTIVE",
            "reason": f"Counter: {ray_cluster_activity_hook_5_counter}",
            "timestamp": datetime.now().timestamp(),
        }
    }


# TODO(rickyx): We could remove this once we unify the autoscaler v1 and v2
# code path for ray status
def reset_autoscaler_v2_enabled_cache():
    import ray.autoscaler.v2.utils as u

    u.cached_is_autoscaler_v2 = None


def _terminate_ec2_instance(node_ip: str) -> None:
    logging.info(f"Terminating instance {node_ip}")
    # This command uses IMDSv2 to get the host instance id and region.
    # After that it terminates itself using aws cli.
    command = (
        'instanceId=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id/);'  # noqa: E501
        'region=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/region);'  # noqa: E501
        "aws ec2 terminate-instances --region $region --instance-ids $instanceId"  # noqa: E501
    )
    _execute_command_on_node(command, node_ip)


def _execute_command_on_node(command: str, node_ip: str):
    logging.debug(f"Executing command on node {node_ip}: {command}")

    multi_line_command = (
        'TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600");'  # noqa: E501
        f"{command}"
    )

    # This is a feature on Anyscale platform that enables
    # easy ssh access to worker nodes.
    ssh_command = f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 2222 ray@{node_ip} '{multi_line_command}'"  # noqa: E501

    try:
        subprocess.run(
            ssh_command, shell=True, capture_output=True, text=True, check=True
        )
    except subprocess.CalledProcessError as e:
        print("Exit code:", e.returncode)
        print("Stderr:", e.stderr)


RPC_FAILURE_MAP = {
    "request": "100:0:0",
    "response": "0:100:0",
    "in_flight": "0:0:100",
}

RPC_FAILURE_TYPES = list(RPC_FAILURE_MAP.keys())
