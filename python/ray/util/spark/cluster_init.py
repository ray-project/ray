import copy
import signal

import yaml
import json
import os
import socket
import sys
import time
import threading
import logging
import uuid
import warnings
import requests
from packaging.version import Version
from typing import Optional, Dict, Tuple, Type

import ray
import ray._private.services
from ray.autoscaler._private.spark.node_provider import HEAD_NODE_ID
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray._common.utils import load_class
from ray._common.network_utils import build_address, parse_address

from .utils import (
    exec_cmd,
    is_port_in_use,
    get_random_unused_port,
    get_spark_session,
    get_spark_application_driver_host,
    is_in_databricks_runtime,
    get_spark_task_assigned_physical_gpus,
    get_avail_mem_per_ray_worker_node,
    get_max_num_concurrent_tasks,
    gen_cmd_exec_failure_msg,
    calc_mem_ray_head_node,
    _wait_service_up,
    _get_local_ray_node_slots,
    get_configured_spark_executor_memory_bytes,
    _get_cpu_cores,
    _get_num_physical_gpus,
)
from .start_hook_base import RayOnSparkStartHook
from .databricks_hook import DefaultDatabricksRayOnSparkStartHook
from threading import Event


_logger = logging.getLogger("ray.util.spark")
_logger.setLevel(logging.INFO)


RAY_ON_SPARK_START_HOOK = "RAY_ON_SPARK_START_HOOK"

MAX_NUM_WORKER_NODES = -1

RAY_ON_SPARK_COLLECT_LOG_TO_PATH = "RAY_ON_SPARK_COLLECT_LOG_TO_PATH"
RAY_ON_SPARK_START_RAY_PARENT_PID = "RAY_ON_SPARK_START_RAY_PARENT_PID"


def _check_system_environment():
    if os.name != "posix":
        raise RuntimeError("Ray on spark only supports running on POSIX system.")

    spark_dependency_error = "ray.util.spark module requires pyspark >= 3.3"
    try:
        import pyspark

        if Version(pyspark.__version__).release < (3, 3, 0):
            raise RuntimeError(spark_dependency_error)
    except ImportError:
        raise RuntimeError(spark_dependency_error)


class RayClusterOnSpark:
    """
    This class is the type of instance returned by the `_setup_ray_cluster` interface.
    Its main functionality is to:
    Connect to, disconnect from, and shutdown the Ray cluster running on Apache Spark.
    Serve as a Python context manager for the `RayClusterOnSpark` instance.

    Args
        address: The url for the ray head node (defined as the hostname and unused
                 port on Spark driver node)
        head_proc: Ray head process
        spark_job_group_id: The Spark job id for a submitted ray job
        num_workers_node: The number of workers in the ray cluster.
    """

    def __init__(
        self,
        address,
        head_proc,
        min_worker_nodes,
        max_worker_nodes,
        temp_dir,
        cluster_unique_id,
        start_hook,
        ray_dashboard_port,
        spark_job_server,
        global_cluster_lock_fd,
        ray_client_server_port,
    ):
        self.address = address
        self.head_proc = head_proc
        self.min_worker_nodes = min_worker_nodes
        self.max_worker_nodes = max_worker_nodes
        self.temp_dir = temp_dir
        self.cluster_unique_id = cluster_unique_id
        self.start_hook = start_hook
        self.ray_dashboard_port = ray_dashboard_port
        self.spark_job_server = spark_job_server
        self.global_cluster_lock_fd = global_cluster_lock_fd
        self.ray_client_server_port = ray_client_server_port

        self.is_shutdown = False
        self.spark_job_is_canceled = False
        self.background_job_exception = None

        # Ray client context returns by `ray.init`
        self.ray_ctx = None

    def wait_until_ready(self):
        import ray

        if self.is_shutdown:
            raise RuntimeError(
                "The ray cluster has been shut down or it failed to start."
            )

        try:
            ray.init(address=self.address)

            if self.ray_dashboard_port is not None and _wait_service_up(
                parse_address(self.address)[0],
                self.ray_dashboard_port,
                _RAY_DASHBOARD_STARTUP_TIMEOUT,
            ):
                self.start_hook.on_ray_dashboard_created(self.ray_dashboard_port)
            else:
                try:
                    __import__("ray.dashboard.optional_deps")
                except ModuleNotFoundError as e:
                    _logger.warning(
                        "Dependencies to launch the optional dashboard API "
                        "server cannot be found. They can be installed with "
                        f"pip install ray[default], root cause: ({repr(e)})"
                    )

            last_alive_worker_count = 0
            last_progress_move_time = time.time()
            while True:
                time.sleep(_RAY_CLUSTER_STARTUP_PROGRESS_CHECKING_INTERVAL)

                # Inside the waiting ready loop,
                # checking `self.background_job_exception`, if it is not None,
                # it means the background spark job has failed,
                # in this case, raise error directly.
                if self.background_job_exception is not None:
                    raise RuntimeError(
                        "Ray workers failed to start."
                    ) from self.background_job_exception

                cur_alive_worker_count = (
                    len([node for node in ray.nodes() if node["Alive"]]) - 1
                )  # Minus 1 means excluding the head node.

                if cur_alive_worker_count >= self.min_worker_nodes:
                    _logger.info(
                        f"Started {cur_alive_worker_count} Ray worker nodes, "
                        f"meet the minimum number of Ray worker nodes required."
                    )
                    return

                if cur_alive_worker_count > last_alive_worker_count:
                    last_alive_worker_count = cur_alive_worker_count
                    last_progress_move_time = time.time()
                    _logger.info(
                        "Ray worker nodes are starting. Progress: "
                        f"({cur_alive_worker_count} / {self.max_worker_nodes})"
                    )
                else:
                    if (
                        time.time() - last_progress_move_time
                        > _RAY_CONNECT_CLUSTER_POLL_PROGRESS_TIMEOUT
                    ):
                        if cur_alive_worker_count == 0:
                            (
                                job_server_host,
                                job_server_port,
                            ) = self.spark_job_server.server_address[:2]
                            response = requests.post(
                                url=(
                                    f"http://{build_address(job_server_host, job_server_port)}"
                                    "/query_last_worker_err"
                                ),
                                json={"spark_job_group_id": None},
                            )
                            response.raise_for_status()

                            decoded_resp = response.content.decode("utf-8")
                            json_res = json.loads(decoded_resp)
                            last_worker_err = json_res["last_worker_err"]

                            if last_worker_err:
                                raise RuntimeError(
                                    "Starting Ray worker node failed, error:\n"
                                    f"{last_worker_err}"
                                )
                            else:
                                raise RuntimeError(
                                    "Current spark cluster has no resources to launch "
                                    "Ray worker nodes."
                                )
                        _logger.warning(
                            "Timeout in waiting for minimal ray workers to start. "
                            "Started / Total requested: "
                            f"({cur_alive_worker_count} / {self.min_worker_nodes}). "
                            "Current spark cluster does not have sufficient resources "
                            "to launch requested minimal number of Ray worker nodes."
                        )
                        return
        finally:
            ray.shutdown()

    def connect(self):
        if ray.is_initialized():
            raise RuntimeError("Already connected to Ray cluster.")
        self.ray_ctx = ray.init(address=self.address)

    def disconnect(self):
        ray.shutdown()
        self.ray_ctx = None

    def shutdown(self):
        """
        Shutdown the ray cluster created by the `setup_ray_cluster` API.
        """
        import fcntl

        if not self.is_shutdown:
            try:
                self.disconnect()
            except Exception:
                pass
            os.environ.pop("RAY_ADDRESS", None)

            if self.global_cluster_lock_fd is not None:
                # release global mode cluster lock.
                fcntl.flock(self.global_cluster_lock_fd, fcntl.LOCK_UN)

            self.spark_job_server.shutdown()
            try:
                self.head_proc.terminate()
            except Exception as e:
                # swallow exception.
                _logger.warning(
                    "An Error occurred during shutdown of ray head node: " f"{repr(e)}"
                )
            self.is_shutdown = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()


def _convert_ray_node_option(key, value):
    converted_key = f"--{key.replace('_', '-')}"
    if key in ["system_config", "resources", "labels"]:
        return f"{converted_key}={json.dumps(value)}"
    if value is None:
        return converted_key
    return f"{converted_key}={str(value)}"


def _convert_ray_node_options(options):
    return [_convert_ray_node_option(k, v) for k, v in options.items()]


_RAY_HEAD_STARTUP_TIMEOUT = 20
_RAY_DASHBOARD_STARTUP_TIMEOUT = 60
_BACKGROUND_JOB_STARTUP_WAIT = int(
    os.environ.get("RAY_ON_SPARK_BACKGROUND_JOB_STARTUP_WAIT", "30")
)
_RAY_CLUSTER_STARTUP_PROGRESS_CHECKING_INTERVAL = 3
_RAY_WORKER_NODE_STARTUP_INTERVAL = int(
    os.environ.get("RAY_ON_SPARK_RAY_WORKER_NODE_STARTUP_INTERVAL", "10")
)
_RAY_CONNECT_CLUSTER_POLL_PROGRESS_TIMEOUT = 120


def _preallocate_ray_worker_port_range():
    """
    If we start multiple ray workers on a machine concurrently, some ray worker
    processes might fail due to ray port conflicts, this is because race condition
    on getting free port and opening the free port.
    To address the issue, this function use an exclusive file lock to delay the
    worker processes to ensure that port acquisition does not create a resource
    contention issue due to a race condition.

    After acquiring lock, it will allocate port range for worker ports
    (for ray node config --min-worker-port and --max-worker-port).
    Because on a spark cluster, multiple ray cluster might be created, so on one spark
    worker machine, there might be multiple ray worker nodes running, these worker
    nodes might belong to different ray cluster, and we must ensure these ray nodes on
    the same machine using non-overlapping worker port range, to achieve this, in this
    function, it creates a file `/tmp/ray_on_spark_worker_port_allocation.txt` file,
    the file format is composed of multiple lines, each line contains 2 number: `pid`
    and `port_range_slot_index`, each port range slot allocates 1000 ports, and
    corresponding port range is:
     - range_begin (inclusive): 20000 + port_range_slot_index * 1000
     - range_end (exclusive): range_begin + 1000
    In this function, it first scans `/tmp/ray_on_spark_worker_port_allocation.txt`
    file, removing lines that containing dead process pid, then find the first unused
    port_range_slot_index, then regenerate this file, and return the allocated port
    range.

    Returns: Allocated port range for current worker ports
    """
    import psutil
    import fcntl

    def acquire_lock(file_path):
        mode = os.O_RDWR | os.O_CREAT | os.O_TRUNC
        try:
            fd = os.open(file_path, mode)
            # The lock file must be readable / writable to all users.
            os.chmod(file_path, 0o0777)
            # Allow for retrying getting a file lock a maximum number of seconds
            max_lock_iter = 600
            for _ in range(max_lock_iter):
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    # Lock is used by other processes, continue loop to wait for lock
                    # available
                    pass
                else:
                    # Acquire lock successfully.
                    return fd
                time.sleep(10)
            raise TimeoutError(f"Acquiring lock on file {file_path} timeout.")
        except Exception:
            os.close(fd)

    lock_file_path = "/tmp/ray_on_spark_worker_startup_barrier_lock.lock"
    try:
        lock_fd = acquire_lock(lock_file_path)
    except TimeoutError:
        # If timeout happens, the file lock might be hold by another process and that
        # process does not release the lock in time by some unexpected reason.
        # In this case, remove the existing lock file and create the file again, and
        # then acquire file lock on the new file.
        try:
            os.remove(lock_file_path)
        except Exception:
            pass
        lock_fd = acquire_lock(lock_file_path)

    def release_lock():
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)

    try:
        port_alloc_file = "/tmp/ray_on_spark_worker_port_allocation.txt"

        # NB: reading / writing `port_alloc_file` is protected by exclusive lock
        # on file `lock_file_path`
        if os.path.exists(port_alloc_file):
            with open(port_alloc_file, mode="r") as fp:
                port_alloc_data = fp.read()
            port_alloc_table = [
                line.split(" ") for line in port_alloc_data.strip().split("\n")
            ]
            port_alloc_table = [
                (int(pid_str), int(slot_index_str))
                for pid_str, slot_index_str in port_alloc_table
            ]
        else:
            port_alloc_table = []
            with open(port_alloc_file, mode="w"):
                pass
            # The port range allocation file must be readable / writable to all users.
            os.chmod(port_alloc_file, 0o0777)

        port_alloc_map = {
            pid: slot_index
            for pid, slot_index in port_alloc_table
            if psutil.pid_exists(pid)  # remove slot used by dead process
        }

        allocated_slot_set = set(port_alloc_map.values())

        if len(allocated_slot_set) == 0:
            new_slot_index = 0
        else:
            new_slot_index = max(allocated_slot_set) + 1
            for index in range(new_slot_index):
                if index not in allocated_slot_set:
                    new_slot_index = index
                    break

        port_alloc_map[os.getpid()] = new_slot_index

        with open(port_alloc_file, mode="w") as fp:
            for pid, slot_index in port_alloc_map.items():
                fp.write(f"{pid} {slot_index}\n")

        worker_port_range_begin = 20000 + new_slot_index * 1000
        worker_port_range_end = worker_port_range_begin + 1000

        if worker_port_range_end > 65536:
            raise RuntimeError(
                "Too many ray worker nodes are running on this machine, cannot "
                "allocate worker port range for new ray worker node."
            )
    except Exception:
        release_lock()
        raise

    def hold_lock():
        time.sleep(_RAY_WORKER_NODE_STARTUP_INTERVAL)
        release_lock()

    threading.Thread(target=hold_lock, args=()).start()

    return worker_port_range_begin, worker_port_range_end


def _append_default_spilling_dir_config(head_node_options, object_spilling_dir):
    if "system_config" not in head_node_options:
        head_node_options["system_config"] = {}
    sys_conf = head_node_options["system_config"]
    if "object_spilling_config" not in sys_conf:
        sys_conf["object_spilling_config"] = json.dumps(
            {
                "type": "filesystem",
                "params": {
                    "directory_path": object_spilling_dir,
                },
            }
        )
    return head_node_options


def _append_resources_config(node_options, resources):
    if "resources" not in node_options:
        node_options["resources"] = {}

    node_options["resources"].update(resources)
    return node_options


def _get_default_ray_tmp_dir():
    return os.path.join(os.environ.get("RAY_TMPDIR", "/tmp"), "ray")


def _create_hook_entry(is_global):
    if RAY_ON_SPARK_START_HOOK in os.environ:
        return load_class(os.environ[RAY_ON_SPARK_START_HOOK])()
    elif is_in_databricks_runtime():
        return DefaultDatabricksRayOnSparkStartHook(is_global)
    else:
        return RayOnSparkStartHook(is_global)


def _setup_ray_cluster(
    *,
    max_worker_nodes: int,
    min_worker_nodes: int,
    num_cpus_worker_node: int,
    num_cpus_head_node: int,
    num_gpus_worker_node: int,
    num_gpus_head_node: int,
    using_stage_scheduling: bool,
    heap_memory_worker_node: int,
    heap_memory_head_node: int,
    object_store_memory_worker_node: int,
    object_store_memory_head_node: int,
    head_node_options: Dict,
    worker_node_options: Dict,
    ray_temp_root_dir: str,
    collect_log_to_path: str,
    autoscale_upscaling_speed: float,
    autoscale_idle_timeout_minutes: float,
    is_global: bool,
) -> Type[RayClusterOnSpark]:
    """
    The public API `ray.util.spark.setup_ray_cluster` does some argument
    validation and then pass validated arguments to this interface.
    and it returns a `RayClusterOnSpark` instance.

    The returned instance can be used to connect to, disconnect from and shutdown the
    ray cluster. This instance can also be used as a context manager (used by
    encapsulating operations within `with _setup_ray_cluster(...):`). Upon entering the
    managed scope, the ray cluster is initiated and connected to. When exiting the
    scope, the ray cluster is disconnected and shut down.

    Note: This function interface is stable and can be used for
    instrumentation logging patching.
    """
    import fcntl

    start_hook = _create_hook_entry(is_global)
    spark = get_spark_session()

    ray_head_ip = socket.gethostbyname(get_spark_application_driver_host(spark))
    ray_head_port = get_random_unused_port(ray_head_ip, min_port=9000, max_port=10000)
    port_exclude_list = [ray_head_port]

    # Make a copy for head_node_options to avoid changing original dict in user code.
    head_node_options = head_node_options.copy()
    include_dashboard = head_node_options.pop("include_dashboard", None)
    ray_dashboard_port = head_node_options.pop("dashboard_port", None)

    if is_global:
        ray_client_server_port = 10001
    else:
        ray_client_server_port = get_random_unused_port(
            ray_head_ip,
            min_port=9000,
            max_port=10000,
            exclude_list=port_exclude_list,
        )

    port_exclude_list.append(ray_client_server_port)

    spark_job_server_port = get_random_unused_port(
        ray_head_ip,
        min_port=9000,
        max_port=10000,
        exclude_list=port_exclude_list,
    )
    port_exclude_list.append(spark_job_server_port)

    if include_dashboard is None or include_dashboard is True:
        if ray_dashboard_port is None:
            ray_dashboard_port = get_random_unused_port(
                ray_head_ip,
                min_port=9000,
                max_port=10000,
                exclude_list=port_exclude_list,
            )
            port_exclude_list.append(ray_dashboard_port)
        ray_dashboard_agent_port = get_random_unused_port(
            ray_head_ip,
            min_port=9000,
            max_port=10000,
            exclude_list=port_exclude_list,
        )
        port_exclude_list.append(ray_dashboard_agent_port)

        dashboard_options = [
            "--dashboard-host=0.0.0.0",
            f"--dashboard-port={ray_dashboard_port}",
            f"--dashboard-agent-listen-port={ray_dashboard_agent_port}",
        ]
        # If include_dashboard is None, we don't set `--include-dashboard` option,
        # in this case Ray will decide whether dashboard can be started
        # (e.g. checking any missing dependencies).
        if include_dashboard is True:
            dashboard_options += ["--include-dashboard=true"]
    else:
        dashboard_options = [
            "--include-dashboard=false",
        ]

    _logger.info(
        f"Ray head hostname: {ray_head_ip}, port: {ray_head_port}, "
        f"ray client server port: {ray_client_server_port}."
    )

    cluster_unique_id = uuid.uuid4().hex[:8]

    if is_global:
        # global mode enabled
        # for global mode, Ray always uses default temp dir
        # so that local Ray client can discover it without specifying
        # head node address.
        if ray_temp_root_dir is not None:
            raise ValueError(
                "Ray on spark global mode cluster does not allow you to set "
                "'ray_temp_root_dir' argument."
            )

        # We only allow user to launch one active Ray on spark global cluster
        # at a time. So acquiring a global file lock before setting up a new
        # Ray on spark global cluster.
        global_cluster_lock_fd = os.open(
            "/tmp/ray_on_spark_global_cluster.lock", os.O_RDWR | os.O_CREAT | os.O_TRUNC
        )

        try:
            # acquiring exclusive lock to ensure copy logs and removing dir safely.
            fcntl.flock(global_cluster_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # acquiring global lock failed.
            raise ValueError(
                "Acquiring global lock failed for setting up new global mode Ray on "
                "spark cluster. If there is an active global mode Ray on spark "
                "cluster, please shut down it before you create a new one."
            )

        ray_temp_dir = None
        ray_default_tmp_dir = _get_default_ray_tmp_dir()
        os.makedirs(ray_default_tmp_dir, exist_ok=True)
        object_spilling_dir = os.path.join(ray_default_tmp_dir, "spill")
    else:
        global_cluster_lock_fd = None
        if ray_temp_root_dir is None:
            ray_temp_root_dir = start_hook.get_default_temp_root_dir()
        ray_temp_dir = os.path.join(
            ray_temp_root_dir, f"ray-{ray_head_port}-{cluster_unique_id}"
        )
        os.makedirs(ray_temp_dir, exist_ok=True)
        object_spilling_dir = os.path.join(ray_temp_dir, "spill")

    os.makedirs(object_spilling_dir, exist_ok=True)

    head_node_options = _append_default_spilling_dir_config(
        head_node_options, object_spilling_dir
    )

    from ray.autoscaler._private.spark.spark_job_server import (
        _start_spark_job_server,
    )

    ray_node_custom_env = start_hook.custom_environment_variables()
    spark_job_server = _start_spark_job_server(
        ray_head_ip, spark_job_server_port, spark, ray_node_custom_env
    )
    autoscaling_cluster = AutoscalingCluster(
        head_resources={
            "CPU": num_cpus_head_node,
            "GPU": num_gpus_head_node,
            "memory": heap_memory_head_node,
            "object_store_memory": object_store_memory_head_node,
        },
        worker_node_types={
            "ray.worker": {
                "resources": {
                    "CPU": num_cpus_worker_node,
                    "GPU": num_gpus_worker_node,
                    "memory": heap_memory_worker_node,
                    "object_store_memory": object_store_memory_worker_node,
                },
                "node_config": {},
                "min_workers": min_worker_nodes,
                "max_workers": max_worker_nodes,
            },
        },
        extra_provider_config={
            "ray_head_ip": ray_head_ip,
            "ray_head_port": ray_head_port,
            "cluster_unique_id": cluster_unique_id,
            "using_stage_scheduling": using_stage_scheduling,
            "ray_temp_dir": ray_temp_dir,
            "worker_node_options": worker_node_options,
            "collect_log_to_path": collect_log_to_path,
            "spark_job_server_port": spark_job_server_port,
        },
        upscaling_speed=autoscale_upscaling_speed,
        idle_timeout_minutes=autoscale_idle_timeout_minutes,
    )
    ray_head_proc, tail_output_deque = autoscaling_cluster.start(
        ray_head_ip,
        ray_head_port,
        ray_client_server_port,
        ray_temp_dir,
        dashboard_options,
        head_node_options,
        collect_log_to_path,
        ray_node_custom_env,
    )
    ray_head_node_cmd = autoscaling_cluster.ray_head_node_cmd

    # wait ray head node spin up.
    time.sleep(_RAY_HEAD_STARTUP_TIMEOUT)

    if not is_port_in_use(ray_head_ip, ray_head_port):
        if ray_head_proc.poll() is None:
            # Ray head GCS service is down. Kill ray head node.
            ray_head_proc.terminate()
            # wait killing complete.
            time.sleep(0.5)

        cmd_exec_failure_msg = gen_cmd_exec_failure_msg(
            ray_head_node_cmd, ray_head_proc.returncode, tail_output_deque
        )
        raise RuntimeError("Start Ray head node failed!\n" + cmd_exec_failure_msg)

    _logger.info("Ray head node started.")

    cluster_address = build_address(ray_head_ip, ray_head_port)
    # Set RAY_ADDRESS environment variable to the cluster address.
    os.environ["RAY_ADDRESS"] = cluster_address

    ray_cluster_handler = RayClusterOnSpark(
        address=cluster_address,
        head_proc=ray_head_proc,
        min_worker_nodes=min_worker_nodes,
        max_worker_nodes=max_worker_nodes,
        temp_dir=ray_temp_dir,
        cluster_unique_id=cluster_unique_id,
        start_hook=start_hook,
        ray_dashboard_port=ray_dashboard_port,
        spark_job_server=spark_job_server,
        global_cluster_lock_fd=global_cluster_lock_fd,
        ray_client_server_port=ray_client_server_port,
    )

    start_hook.on_cluster_created(ray_cluster_handler)

    return ray_cluster_handler


_active_ray_cluster = None
_active_ray_cluster_rwlock = threading.RLock()


def _create_resource_profile(num_cpus_per_node, num_gpus_per_node):
    from pyspark.resource.profile import ResourceProfileBuilder
    from pyspark.resource.requests import TaskResourceRequests

    task_res_req = TaskResourceRequests().cpus(num_cpus_per_node)
    if num_gpus_per_node > 0:
        task_res_req = task_res_req.resource("gpu", num_gpus_per_node)
    return ResourceProfileBuilder().require(task_res_req).build


# A dict storing blocked key to replacement argument you should use.
_head_node_option_block_keys = {
    "temp_dir": "ray_temp_root_dir",
    "block": None,
    "head": None,
    "node_ip_address": None,
    "port": None,
    "num_cpus": None,
    "num_gpus": None,
    "dashboard_host": None,
    "dashboard_agent_listen_port": None,
}

_worker_node_option_block_keys = {
    "temp_dir": "ray_temp_root_dir",
    "block": None,
    "head": None,
    "address": None,
    "num_cpus": "num_cpus_worker_node",
    "num_gpus": "num_gpus_worker_node",
    "memory": None,
    "object_store_memory": "object_store_memory_worker_node",
    "dashboard_agent_listen_port": None,
    "min_worker_port": None,
    "max_worker_port": None,
}


def _verify_node_options(node_options, block_keys, node_type):
    for key in node_options:
        if key.startswith("--") or "-" in key:
            raise ValueError(
                "For a ray node option like '--foo-bar', you should convert it to "
                "following format 'foo_bar' in 'head_node_options' / "
                "'worker_node_options' arguments."
            )

        if key in block_keys:
            common_err_msg = (
                f"Setting the option '{key}' for {node_type} nodes is not allowed."
            )
            replacement_arg = block_keys[key]
            if replacement_arg:
                raise ValueError(
                    f"{common_err_msg} You should set the '{replacement_arg}' option "
                    "instead."
                )
            else:
                raise ValueError(
                    f"{common_err_msg} This option is controlled by Ray on Spark."
                )


def _setup_ray_cluster_internal(
    max_worker_nodes: int,
    min_worker_nodes: Optional[int],
    num_cpus_worker_node: Optional[int],
    num_cpus_head_node: Optional[int],
    num_gpus_worker_node: Optional[int],
    num_gpus_head_node: Optional[int],
    heap_memory_worker_node: Optional[int],
    heap_memory_head_node: Optional[int],
    object_store_memory_worker_node: Optional[int],
    object_store_memory_head_node: Optional[int],
    head_node_options: Optional[Dict],
    worker_node_options: Optional[Dict],
    ray_temp_root_dir: Optional[str],
    strict_mode: bool,
    collect_log_to_path: Optional[str],
    autoscale_upscaling_speed: Optional[float],
    autoscale_idle_timeout_minutes: Optional[float],
    is_global: bool,
    **kwargs,
) -> Tuple[str, str]:
    global _active_ray_cluster

    _check_system_environment()
    _install_sigterm_signal()

    head_node_options = head_node_options or {}
    worker_node_options = worker_node_options or {}

    _verify_node_options(
        head_node_options,
        _head_node_option_block_keys,
        "Ray head node on spark",
    )
    _verify_node_options(
        worker_node_options,
        _worker_node_option_block_keys,
        "Ray worker node on spark",
    )

    if _active_ray_cluster is not None:
        raise RuntimeError(
            "Current active ray cluster on spark haven't shut down. Please call "
            "`ray.util.spark.shutdown_ray_cluster()` before initiating a new Ray "
            "cluster on spark."
        )

    if ray.is_initialized():
        raise RuntimeError(
            "Current python process already initialized Ray, Please shut down it "
            "by `ray.shutdown()` before initiating a Ray cluster on spark."
        )

    spark = get_spark_session()

    spark_master = spark.sparkContext.master

    is_spark_local_mode = spark_master == "local" or spark_master.startswith("local[")

    if not (
        spark_master.startswith("spark://")
        or spark_master.startswith("local-cluster[")
        or is_spark_local_mode
    ):
        raise RuntimeError(
            "Ray on Spark only supports spark cluster in standalone mode, "
            "local-cluster mode or spark local mode."
        )

    if is_spark_local_mode:
        support_stage_scheduling = False
    elif (
        is_in_databricks_runtime()
        and Version(os.environ["DATABRICKS_RUNTIME_VERSION"]).major >= 12
    ):
        support_stage_scheduling = True
    else:
        import pyspark

        if Version(pyspark.__version__).release >= (3, 4, 0):
            support_stage_scheduling = True
        else:
            support_stage_scheduling = False

    if "num_cpus_per_node" in kwargs:
        if num_cpus_worker_node is not None:
            raise ValueError(
                "'num_cpus_per_node' and 'num_cpus_worker_node' arguments are "
                "equivalent. Only set 'num_cpus_worker_node'."
            )
        num_cpus_worker_node = kwargs["num_cpus_per_node"]
        warnings.warn(
            "'num_cpus_per_node' argument is deprecated, please use "
            "'num_cpus_worker_node' argument instead.",
            DeprecationWarning,
        )

    if "num_gpus_per_node" in kwargs:
        if num_gpus_worker_node is not None:
            raise ValueError(
                "'num_gpus_per_node' and 'num_gpus_worker_node' arguments are "
                "equivalent. Only set 'num_gpus_worker_node'."
            )
        num_gpus_worker_node = kwargs["num_gpus_per_node"]
        warnings.warn(
            "'num_gpus_per_node' argument is deprecated, please use "
            "'num_gpus_worker_node' argument instead.",
            DeprecationWarning,
        )

    if "object_store_memory_per_node" in kwargs:
        if object_store_memory_worker_node is not None:
            raise ValueError(
                "'object_store_memory_per_node' and 'object_store_memory_worker_node' "
                "arguments  are equivalent. Only set "
                "'object_store_memory_worker_node'."
            )
        object_store_memory_worker_node = kwargs["object_store_memory_per_node"]
        warnings.warn(
            "'object_store_memory_per_node' argument is deprecated, please use "
            "'object_store_memory_worker_node' argument instead.",
            DeprecationWarning,
        )

    # Environment configurations within the Spark Session that dictate how many cpus
    # and gpus to use for each submitted spark task.
    num_spark_task_cpus = int(spark.sparkContext.getConf().get("spark.task.cpus", "1"))

    if num_cpus_worker_node is not None and num_cpus_worker_node <= 0:
        raise ValueError("Argument `num_cpus_worker_node` value must be > 0.")

    # note: spark.task.resource.gpu.amount config might be fractional value like 0.5
    default_num_spark_task_gpus = float(
        spark.sparkContext.getConf().get("spark.task.resource.gpu.amount", "0")
    )
    rounded_num_spark_task_gpus = int(default_num_spark_task_gpus)
    if default_num_spark_task_gpus > 0:
        warn_msg = (
            "You configured 'spark.task.resource.gpu.amount' to "
            f"{default_num_spark_task_gpus},"
            "we recommend setting this value to 0 so that Spark jobs do not "
            "reserve GPU resources, preventing Ray-on-Spark workloads from having the "
            "maximum number of GPUs available."
        )

        if is_in_databricks_runtime():
            from ray.util.spark.databricks_hook import (
                get_databricks_display_html_function,
            )

            get_databricks_display_html_function()(
                f"<b style='color:red;'>{warn_msg}</b>"
            )
        else:
            _logger.warning(warn_msg)

    if num_gpus_worker_node is not None and num_gpus_worker_node < 0:
        raise ValueError("Argument `num_gpus_worker_node` value must be >= 0.")

    def _get_spark_worker_resources(_):
        from ray.util.spark.utils import (
            _get_cpu_cores,
            _get_num_physical_gpus,
            _get_spark_worker_total_physical_memory,
        )

        num_cpus_spark_worker = _get_cpu_cores()
        num_gpus_spark_worker = _get_num_physical_gpus()
        total_mem_bytes = _get_spark_worker_total_physical_memory()

        return (
            num_cpus_spark_worker,
            num_gpus_spark_worker,
            total_mem_bytes,
        )

    (num_cpus_spark_worker, num_gpus_spark_worker, spark_worker_mem_bytes,) = (
        spark.sparkContext.parallelize([1], 1)
        .map(_get_spark_worker_resources)
        .collect()[0]
    )

    if num_cpus_worker_node is not None and num_gpus_worker_node is not None:
        if support_stage_scheduling:
            using_stage_scheduling = True
            res_profile = _create_resource_profile(
                num_cpus_worker_node, num_gpus_worker_node
            )
        else:
            raise ValueError(
                "Current spark version does not support stage scheduling, so that "
                "you cannot set the argument `num_cpus_worker_node` and "
                "`num_gpus_worker_node` values. Without setting the 2 arguments, "
                "per-Ray worker node will be assigned with number of "
                f"'spark.task.cpus' (equals to {num_spark_task_cpus}) cpu cores "
                "and rounded down number of 'spark.task.resource.gpu.amount' "
                f"(equals to {rounded_num_spark_task_gpus}) GPUs. To enable spark "
                f"stage scheduling, you need to upgrade spark to 3.4 version or use "
                "Databricks Runtime 12.x, and you cannot use spark local mode."
            )
    elif num_cpus_worker_node is None and num_gpus_worker_node is None:
        if support_stage_scheduling:
            # Make one Ray worker node using maximum CPU / GPU resources
            # of the whole spark worker node, this is the optimal
            # configuration.
            num_cpus_worker_node = num_cpus_spark_worker
            num_gpus_worker_node = num_gpus_spark_worker
            using_stage_scheduling = True
            res_profile = _create_resource_profile(
                num_cpus_worker_node, num_gpus_worker_node
            )
        else:
            using_stage_scheduling = False
            res_profile = None

            num_cpus_worker_node = num_spark_task_cpus
            num_gpus_worker_node = rounded_num_spark_task_gpus
    else:
        raise ValueError(
            "'num_cpus_worker_node' and 'num_gpus_worker_node' arguments must be"
            "set together or unset together."
        )

    (
        ray_worker_node_heap_mem_bytes,
        ray_worker_node_object_store_mem_bytes,
    ) = get_avail_mem_per_ray_worker_node(
        spark,
        heap_memory_worker_node,
        object_store_memory_worker_node,
        num_cpus_worker_node,
        num_gpus_worker_node,
    )

    spark_worker_ray_node_slots = _get_local_ray_node_slots(
        num_cpus_spark_worker,
        num_gpus_spark_worker,
        num_cpus_worker_node,
        num_gpus_worker_node,
    )

    spark_executor_memory_bytes = get_configured_spark_executor_memory_bytes(spark)
    spark_worker_required_memory_bytes = (
        spark_executor_memory_bytes
        + spark_worker_ray_node_slots
        * (ray_worker_node_heap_mem_bytes + ray_worker_node_object_store_mem_bytes)
    )
    if spark_worker_required_memory_bytes > 0.8 * spark_worker_mem_bytes:
        warn_msg = (
            "In each spark worker node, we recommend making the sum of "
            "'spark_executor_memory + num_Ray_worker_nodes_per_spark_worker * "
            "(memory_worker_node + object_store_memory_worker_node)' to be less than "
            "'spark_worker_physical_memory * 0.8', otherwise it might lead to "
            "spark worker physical memory exhaustion and Ray task OOM errors."
        )

        if is_in_databricks_runtime():
            from ray.util.spark.databricks_hook import (
                get_databricks_display_html_function,
            )

            get_databricks_display_html_function()(
                f"<b style='background-color:Cyan;'>{warn_msg}<br></b>"
            )
        else:
            _logger.warning(warn_msg)

    if "num_worker_nodes" in kwargs:
        raise ValueError(
            "'num_worker_nodes' argument is removed, please set "
            "'max_worker_nodes' and 'min_worker_nodes' argument instead."
        )

    if max_worker_nodes == MAX_NUM_WORKER_NODES:
        if min_worker_nodes is not None:
            raise ValueError(
                "If you set 'max_worker_nodes' to 'MAX_NUM_WORKER_NODES', autoscaling "
                "is not supported, so that you cannot set 'min_worker_nodes' argument "
                "and 'min_worker_nodes' is automatically set to be equal to "
                "'max_worker_nodes'."
            )

        # max_worker_nodes=MAX_NUM_WORKER_NODES represents using all available
        # spark task slots
        max_worker_nodes = get_max_num_concurrent_tasks(spark.sparkContext, res_profile)
        min_worker_nodes = max_worker_nodes
    elif max_worker_nodes <= 0:
        raise ValueError(
            "The value of 'max_worker_nodes' argument must be either a positive "
            "integer or 'ray.util.spark.MAX_NUM_WORKER_NODES'."
        )

    if "autoscale" in kwargs:
        raise ValueError(
            "'autoscale' argument is removed. You can set 'min_worker_nodes' argument "
            "to be less than 'max_worker_nodes' to make autoscaling enabled."
        )

    if min_worker_nodes is None:
        min_worker_nodes = max_worker_nodes
    elif not (0 <= min_worker_nodes <= max_worker_nodes):
        raise ValueError(
            "The value of 'max_worker_nodes' argument must be an integer >= 0 "
            "and <= 'max_worker_nodes'"
        )

    insufficient_resources = []

    if num_cpus_worker_node < 4:
        insufficient_resources.append(
            "The provided CPU resources for each ray worker are inadequate to start "
            "a ray cluster. Based on the total cpu resources available and the "
            "configured task sizing, each ray worker node would start with "
            f"{num_cpus_worker_node} CPU cores. This is less than the recommended "
            "value of `4` CPUs per worker. On spark version >= 3.4 or Databricks "
            "Runtime 12.x, you can set the argument `num_cpus_worker_node` to "
            "a value >= 4 to address it, otherwise you need to increase the spark "
            "application configuration 'spark.task.cpus' to a minimum of `4` to "
            "address it."
        )

    if ray_worker_node_heap_mem_bytes < 10 * 1024 * 1024 * 1024:
        insufficient_resources.append(
            "The provided memory resources for each ray worker node are inadequate. "
            "Based on the total memory available on the spark cluster and the "
            "configured task sizing, each ray worker would start with "
            f"{ray_worker_node_heap_mem_bytes} bytes heap memory. This is less than "
            "the recommended value of 10GB. The ray worker node heap memory size is "
            "calculated by "
            "(SPARK_WORKER_PHYSICAL_MEMORY / num_local_spark_task_slots * 0.8) - "
            "object_store_memory_worker_node. To increase the heap space available, "
            "increase the memory in the spark cluster by using instance types with "
            "larger memory, or increase number of CPU/GPU per Ray worker node "
            "(so it leads to less Ray worker node slots per spark worker node), "
            "or apply a lower `object_store_memory_worker_node`."
        )
    if insufficient_resources:
        if strict_mode:
            raise ValueError(
                "You are creating ray cluster on spark with strict mode (it can be "
                "disabled by setting argument 'strict_mode=False' when calling API "
                "'setup_ray_cluster'), strict mode requires the spark cluster config "
                "satisfying following criterion: "
                "\n".join(insufficient_resources)
            )
        else:
            _logger.warning("\n".join(insufficient_resources))

    if num_cpus_head_node is None:
        if is_global:
            num_cpus_head_node = _get_cpu_cores()
        else:
            num_cpus_head_node = 0
    else:
        if num_cpus_head_node < 0:
            raise ValueError(
                "Argument `num_cpus_head_node` value must be >= 0. "
                f"Current value is {num_cpus_head_node}."
            )

    if num_gpus_head_node is None:
        if is_global:
            try:
                num_gpus_head_node = _get_num_physical_gpus()
            except Exception:
                num_gpus_head_node = 0
        else:
            num_gpus_head_node = 0
    else:
        if num_gpus_head_node < 0:
            raise ValueError(
                "Argument `num_gpus_head_node` value must be >= 0."
                f"Current value is {num_gpus_head_node}."
            )

    if (
        num_cpus_head_node == 0
        and num_gpus_head_node == 0
        and object_store_memory_head_node is None
    ):
        # Because tasks that require CPU or GPU resources are not scheduled to Ray
        # head node, and user does not set `object_store_memory_head_node` explicitly,
        # limit the heap memory and object store memory allocation to the
        # head node, in order to save spark driver memory.
        heap_memory_head_node = 1024 * 1024 * 1024
        object_store_memory_head_node = 1024 * 1024 * 1024
    else:
        heap_memory_head_node, object_store_memory_head_node = calc_mem_ray_head_node(
            heap_memory_head_node, object_store_memory_head_node
        )

    with _active_ray_cluster_rwlock:
        cluster = _setup_ray_cluster(
            max_worker_nodes=max_worker_nodes,
            min_worker_nodes=min_worker_nodes,
            num_cpus_worker_node=num_cpus_worker_node,
            num_cpus_head_node=num_cpus_head_node,
            num_gpus_worker_node=num_gpus_worker_node,
            num_gpus_head_node=num_gpus_head_node,
            using_stage_scheduling=using_stage_scheduling,
            heap_memory_worker_node=ray_worker_node_heap_mem_bytes,
            heap_memory_head_node=heap_memory_head_node,
            object_store_memory_worker_node=ray_worker_node_object_store_mem_bytes,
            object_store_memory_head_node=object_store_memory_head_node,
            head_node_options=head_node_options,
            worker_node_options=worker_node_options,
            ray_temp_root_dir=ray_temp_root_dir,
            collect_log_to_path=collect_log_to_path,
            autoscale_upscaling_speed=autoscale_upscaling_speed,
            autoscale_idle_timeout_minutes=autoscale_idle_timeout_minutes,
            is_global=is_global,
        )
        # set global _active_ray_cluster to be the
        # started cluster.
        _active_ray_cluster = cluster

        try:
            cluster.wait_until_ready()  # NB: this line might raise error.
        except Exception as e:
            try:
                shutdown_ray_cluster()
            except Exception:
                pass
            raise RuntimeError("Launch Ray-on-Spark cluster failed") from e

    head_ip = parse_address(cluster.address)[0]
    remote_connection_address = (
        f"ray://{build_address(head_ip, cluster.ray_client_server_port)}"
    )
    return cluster.address, remote_connection_address


@PublicAPI
def setup_ray_cluster(
    *,
    max_worker_nodes: int,
    min_worker_nodes: Optional[int] = None,
    num_cpus_worker_node: Optional[int] = None,
    num_cpus_head_node: Optional[int] = None,
    num_gpus_worker_node: Optional[int] = None,
    num_gpus_head_node: Optional[int] = None,
    memory_worker_node: Optional[int] = None,
    memory_head_node: Optional[int] = None,
    object_store_memory_worker_node: Optional[int] = None,
    object_store_memory_head_node: Optional[int] = None,
    head_node_options: Optional[Dict] = None,
    worker_node_options: Optional[Dict] = None,
    ray_temp_root_dir: Optional[str] = None,
    strict_mode: bool = False,
    collect_log_to_path: Optional[str] = None,
    autoscale_upscaling_speed: Optional[float] = 1.0,
    autoscale_idle_timeout_minutes: Optional[float] = 1.0,
    **kwargs,
) -> Tuple[str, str]:
    """
    Set up a ray cluster on the spark cluster by starting a ray head node in the
    spark application's driver side node.
    After creating the head node, a background spark job is created that
    generates an instance of `RayClusterOnSpark` that contains configuration for the
    ray cluster that will run on the Spark cluster's worker nodes.
    After a ray cluster is set up, "RAY_ADDRESS" environment variable is set to
    the cluster address, so you can call `ray.init()` without specifying ray cluster
    address to connect to the cluster. To shut down the cluster you can call
    `ray.util.spark.shutdown_ray_cluster()`.
    Note: If the active ray cluster haven't shut down, you cannot create a new ray
    cluster.

    Args:
        max_worker_nodes: This argument represents maximum ray worker nodes to start
            for the ray cluster. you can
            specify the `max_worker_nodes` as `ray.util.spark.MAX_NUM_WORKER_NODES`
            represents a ray cluster
            configuration that will use all available resources configured for the
            spark application.
            To create a spark application that is intended to exclusively run a
            shared ray cluster in non-scaling, it is recommended to set this argument
            to `ray.util.spark.MAX_NUM_WORKER_NODES`.
        min_worker_nodes: Minimal number of worker nodes (default `None`),
            if "max_worker_nodes" value is equal to "min_worker_nodes" argument,
            or "min_worker_nodes" argument value is None, then autoscaling is disabled
            and Ray cluster is launched with fixed number "max_worker_nodes" of
            Ray worker nodes, otherwise autoscaling is enabled.
        num_cpus_worker_node: Number of cpus available to per-ray worker node, if not
            provided, if spark stage scheduling is supported, 'num_cpus_head_node'
            value equals to number of cpu cores per spark worker node, otherwise
            it uses spark application configuration 'spark.task.cpus' instead.
            **Limitation** Only spark version >= 3.4 or Databricks Runtime 12.x
            supports setting this argument.
        num_cpus_head_node: Number of cpus available to Ray head node, if not provide,
            if it is global mode Ray cluster, use number of cpu cores in spark driver
            node, otherwise use 0 instead.
            use 0 instead. Number 0 means tasks requiring CPU resources are not
            scheduled to Ray head node.
        num_gpus_worker_node: Number of gpus available to per-ray worker node, if not
            provided, if spark stage scheduling is supported, 'num_gpus_worker_node'
            value equals to number of GPUs per spark worker node, otherwise
            it uses rounded down value of spark application configuration
            'spark.task.resource.gpu.amount' instead.
            This argument is only available on spark cluster that is configured with
            'gpu' resources.
            **Limitation** Only spark version >= 3.4 or Databricks Runtime 12.x
            supports setting this argument.
        num_gpus_head_node: Number of gpus available to Ray head node, if not provide,
            if it is global mode Ray cluster, use number of GPUs in spark driver node,
            otherwise use 0 instead.
            This argument is only available on spark cluster which spark driver node
            has GPUs.
        memory_worker_node: Optional[int]:
            Heap memory configured for Ray worker node. This is basically setting
            `--memory` option when starting Ray node by `ray start` command.
        memory_head_node: Optional[int]:
            Heap memory configured for Ray head node. This is basically setting
            `--memory` option when starting Ray node by `ray start` command.
        object_store_memory_worker_node: Object store memory available to per-ray worker
            node, but it is capped by
            "dev_shm_available_size * 0.8 / num_tasks_per_spark_worker".
            The default value equals to
            "0.3 * spark_worker_physical_memory * 0.8 / num_tasks_per_spark_worker".
        object_store_memory_head_node: Object store memory available to Ray head
            node, but it is capped by "dev_shm_available_size * 0.8".
            The default value equals to
            "0.3 * spark_driver_physical_memory * 0.8".
        head_node_options: A dict representing Ray head node extra options, these
            options will be passed to `ray start` script. Note you need to convert
            `ray start` options key from `--foo-bar` format to `foo_bar` format.
            For flag options (e.g. '--disable-usage-stats'), you should set the value
            to None in the option dict, like `{"disable_usage_stats": None}`.
            Note: Short name options (e.g. '-v') are not supported.
        worker_node_options: A dict representing Ray worker node extra options,
            these options will be passed to `ray start` script. Note you need to
            convert `ray start` options key from `--foo-bar` format to `foo_bar`
            format.
            For flag options (e.g. '--disable-usage-stats'), you should set the value
            to None in the option dict, like `{"disable_usage_stats": None}`.
            Note: Short name options (e.g. '-v') are not supported.
        ray_temp_root_dir: A local disk path to store the ray temporary data. The
            created cluster will create a subdirectory
            "ray-{head_port}-{random_suffix}" beneath this path.
        strict_mode: Boolean flag to fast-fail initialization of the ray cluster if
            the available spark cluster does not have sufficient resources to fulfill
            the resource allocation for memory, cpu and gpu. When set to true, if the
            requested resources are not available for recommended minimum recommended
            functionality, an exception will be raised that details the inadequate
            spark cluster configuration settings. If overridden as `False`,
            a warning is raised.
        collect_log_to_path: If specified, after ray head / worker nodes terminated,
            collect their logs to the specified path. On Databricks Runtime, we
            recommend you to specify a local path starts with '/dbfs/', because the
            path mounts with a centralized storage device and stored data is persisted
            after Databricks spark cluster terminated.
        autoscale_upscaling_speed: If autoscale enabled, it represents the number of
            nodes allowed to be pending as a multiple of the current number of nodes.
            The higher the value, the more aggressive upscaling will be. For example,
            if this is set to 1.0, the cluster can grow in size by at most 100% at any
            time, so if the cluster currently has 20 nodes, at most 20 pending launches
            are allowed. The minimum number of pending launches is 5 regardless of
            this setting.
            Default value is 1.0, minimum value is 1.0
        autoscale_idle_timeout_minutes: If autoscale enabled, it represents the number
            of minutes that need to pass before an idle worker node is removed by the
            autoscaler. The smaller the value, the more aggressive downscaling will be.
            Worker nodes are considered idle when they hold no active tasks, actors,
            or referenced objects (either in-memory or spilled to disk). This parameter
            does not affect the head node.
            Default value is 1.0, minimum value is 0
    Returns:
        returns a tuple of (address, remote_connection_address)
        "address" is in format of "<ray_head_node_ip>:<port>"
        "remote_connection_address" is in format of
        "ray://<ray_head_node_ip>:<ray-client-server-port>",
        if your client runs on a machine that also hosts a Ray cluster node locally,
        you can connect to the Ray cluster via ``ray.init(address)``,
        otherwise you can connect to the Ray cluster via
        ``ray.init(remote_connection_address)``.
    """

    return _setup_ray_cluster_internal(
        max_worker_nodes=max_worker_nodes,
        min_worker_nodes=min_worker_nodes,
        num_cpus_worker_node=num_cpus_worker_node,
        num_cpus_head_node=num_cpus_head_node,
        num_gpus_worker_node=num_gpus_worker_node,
        num_gpus_head_node=num_gpus_head_node,
        heap_memory_worker_node=memory_worker_node,
        heap_memory_head_node=memory_head_node,
        object_store_memory_worker_node=object_store_memory_worker_node,
        object_store_memory_head_node=object_store_memory_head_node,
        head_node_options=head_node_options,
        worker_node_options=worker_node_options,
        ray_temp_root_dir=ray_temp_root_dir,
        strict_mode=strict_mode,
        collect_log_to_path=collect_log_to_path,
        autoscale_upscaling_speed=autoscale_upscaling_speed,
        autoscale_idle_timeout_minutes=autoscale_idle_timeout_minutes,
        is_global=False,
        **kwargs,
    )


@PublicAPI
def setup_global_ray_cluster(
    *,
    max_worker_nodes: int,
    is_blocking: bool = True,
    min_worker_nodes: Optional[int] = None,
    num_cpus_worker_node: Optional[int] = None,
    num_cpus_head_node: Optional[int] = None,
    num_gpus_worker_node: Optional[int] = None,
    num_gpus_head_node: Optional[int] = None,
    memory_worker_node: Optional[int] = None,
    memory_head_node: Optional[int] = None,
    object_store_memory_worker_node: Optional[int] = None,
    object_store_memory_head_node: Optional[int] = None,
    head_node_options: Optional[Dict] = None,
    worker_node_options: Optional[Dict] = None,
    strict_mode: bool = False,
    collect_log_to_path: Optional[str] = None,
    autoscale_upscaling_speed: Optional[float] = 1.0,
    autoscale_idle_timeout_minutes: Optional[float] = 1.0,
):
    """
    Set up a global mode cluster.
    The global Ray on spark cluster means:
    - You can only create one active global Ray on spark cluster at a time.
    On databricks cluster, the global Ray cluster can be used by all users,
    - as contrast, non-global Ray cluster can only be used by current notebook
    user.
    - It is up persistently without automatic shutdown.
    - On databricks notebook, you can connect to the global cluster by calling
    ``ray.init()`` without specifying its address, it will discover the
    global cluster automatically if it is up.

    For global mode, the ``ray_temp_root_dir`` argument is not supported.
    Global model Ray cluster always use the default Ray temporary directory
    path.

    All arguments are the same with ``setup_ray_cluster`` API except that:
    - the ``ray_temp_root_dir`` argument is not supported.
    Global model Ray cluster always use the default Ray temporary directory
    path.
    - A new argument "is_blocking" (default ``True``) is added.
    If "is_blocking" is True,
    then keep the call blocking until it is interrupted.
    once the call is interrupted, the global Ray on spark cluster is shut down and
    `setup_global_ray_cluster` call terminates.
    If "is_blocking" is False,
    once Ray cluster setup completes, return immediately.
    """

    cluster_address = _setup_ray_cluster_internal(
        max_worker_nodes=max_worker_nodes,
        min_worker_nodes=min_worker_nodes,
        num_cpus_worker_node=num_cpus_worker_node,
        num_cpus_head_node=num_cpus_head_node,
        num_gpus_worker_node=num_gpus_worker_node,
        num_gpus_head_node=num_gpus_head_node,
        heap_memory_worker_node=memory_worker_node,
        heap_memory_head_node=memory_head_node,
        object_store_memory_worker_node=object_store_memory_worker_node,
        object_store_memory_head_node=object_store_memory_head_node,
        head_node_options=head_node_options,
        worker_node_options=worker_node_options,
        ray_temp_root_dir=None,
        strict_mode=strict_mode,
        collect_log_to_path=collect_log_to_path,
        autoscale_upscaling_speed=autoscale_upscaling_speed,
        autoscale_idle_timeout_minutes=autoscale_idle_timeout_minutes,
        is_global=True,
    )

    if not is_blocking:
        return cluster_address

    global _global_ray_cluster_cancel_event
    try:
        _global_ray_cluster_cancel_event = Event()
        # serve forever until user cancel the command.
        _global_ray_cluster_cancel_event.wait()
    finally:
        _global_ray_cluster_cancel_event = None
        # once the program is interrupted,
        # or the corresponding databricks notebook command is interrupted
        # shut down the Ray cluster.
        shutdown_ray_cluster()


def _start_ray_worker_nodes(
    *,
    spark_job_server,
    spark_job_group_id,
    spark_job_group_desc,
    num_worker_nodes,
    using_stage_scheduling,
    ray_head_ip,
    ray_head_port,
    ray_temp_dir,
    num_cpus_per_node,
    num_gpus_per_node,
    heap_memory_per_node,
    object_store_memory_per_node,
    worker_node_options,
    collect_log_to_path,
    node_id,
):
    # NB:
    # In order to start ray worker nodes on spark cluster worker machines,
    # We launch a background spark job:
    #  1. Each spark task launches one ray worker node. This design ensures all ray
    #     worker nodes have the same shape (same cpus / gpus / memory configuration).
    #     If ray worker nodes have a non-uniform shape, the Ray cluster setup will
    #     be non-deterministic and could create issues with node sizing.
    #  2. A ray worker node is started via the `ray start` CLI. In each spark task,
    #     a child process is started and will execute a `ray start ...` command in
    #     blocking mode.
    #  3. Each task will acquire a file lock for 10s to ensure that the ray worker
    #     init will acquire a port connection to the ray head node that does not
    #     contend with other worker processes on the same Spark worker node.
    #  4. When the ray cluster is shutdown, killing ray worker nodes is implemented by
    #     `sparkContext.cancelJobGroup` to cancel the background spark job, sending a
    #     SIGKILL signal to all spark tasks. Once the spark tasks are killed,
    #     `ray_start_node` process detects parent died event then it kills ray
    #     worker node.
    spark = spark_job_server.spark
    spark_job_server_port = spark_job_server.server_address[1]
    ray_node_custom_env = spark_job_server.ray_node_custom_env

    def ray_cluster_job_mapper(_):
        from pyspark.taskcontext import TaskContext

        _worker_logger = logging.getLogger("ray.util.spark.worker")

        context = TaskContext.get()

        (
            worker_port_range_begin,
            worker_port_range_end,
        ) = _preallocate_ray_worker_port_range()

        # 10001 is used as ray client server port of global mode ray cluster.
        ray_worker_node_dashboard_agent_port = get_random_unused_port(
            ray_head_ip, min_port=10002, max_port=20000
        )
        ray_worker_node_cmd = [
            sys.executable,
            "-m",
            "ray.util.spark.start_ray_node",
            f"--num-cpus={num_cpus_per_node}",
            "--block",
            f"--address={build_address(ray_head_ip, ray_head_port)}",
            f"--memory={heap_memory_per_node}",
            f"--object-store-memory={object_store_memory_per_node}",
            f"--min-worker-port={worker_port_range_begin}",
            f"--max-worker-port={worker_port_range_end - 1}",
            f"--dashboard-agent-listen-port={ray_worker_node_dashboard_agent_port}",
            *_convert_ray_node_options(worker_node_options),
        ]
        if ray_temp_dir is not None:
            ray_worker_node_cmd.append(f"--temp-dir={ray_temp_dir}")

        ray_worker_node_extra_envs = {
            RAY_ON_SPARK_COLLECT_LOG_TO_PATH: collect_log_to_path or "",
            RAY_ON_SPARK_START_RAY_PARENT_PID: str(os.getpid()),
            "RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER": "1",
            **ray_node_custom_env,
        }

        if num_gpus_per_node > 0:
            task_resources = context.resources()

            if "gpu" not in task_resources:
                raise RuntimeError(
                    "Couldn't get the gpu id, Please check the GPU resource "
                    "configuration"
                )
            gpu_addr_list = [
                int(addr.strip()) for addr in task_resources["gpu"].addresses
            ]

            available_physical_gpus = get_spark_task_assigned_physical_gpus(
                gpu_addr_list
            )
            ray_worker_node_cmd.append(
                f"--num-gpus={len(available_physical_gpus)}",
            )
            ray_worker_node_extra_envs["CUDA_VISIBLE_DEVICES"] = ",".join(
                [str(gpu_id) for gpu_id in available_physical_gpus]
            )

        _worker_logger.info(
            f"Start Ray worker, command: {' '.join(ray_worker_node_cmd)}"
        )

        try:
            is_task_reschedule_failure = False
            # Check node id availability
            response = requests.post(
                url=(
                    f"http://{build_address(ray_head_ip, spark_job_server_port)}"
                    "/check_node_id_availability"
                ),
                json={
                    "node_id": node_id,
                    "spark_job_group_id": spark_job_group_id,
                },
            )
            if not response.json()["available"]:
                # The case happens when a Ray node is down unexpected
                # caused by spark worker node down and spark tries to
                # reschedule the spark task, so it triggers node
                # creation with duplicated node id.
                # in this case, finish the spark task immediately
                # so spark won't try to reschedule this task
                # and Ray autoscaler will trigger a new node creation
                # with new node id, and a new spark job will be created
                # for holding it.
                is_task_reschedule_failure = True
                raise RuntimeError(
                    "Starting Ray worker node twice with the same node id "
                    "is not allowed."
                )

            # Notify job server the task has been launched.
            requests.post(
                url=(
                    f"http://{build_address(ray_head_ip, spark_job_server_port)}"
                    "/notify_task_launched"
                ),
                json={
                    "spark_job_group_id": spark_job_group_id,
                },
            )

            # Note:
            # When a pyspark job cancelled, the UDF python worker process are killed by
            # signal "SIGKILL", then `start_ray_node` process will detect the parent
            # died event (see `ray.util.spark.start_ray_node.check_parent_alive`) and
            # then kill ray worker node process and execute cleanup routine.
            exec_cmd(
                ray_worker_node_cmd,
                synchronous=True,
                extra_env=ray_worker_node_extra_envs,
            )
        except Exception as e:
            # In the following 2 cases, exception is raised:
            # (1)
            # Starting Ray worker node fails, the `e` will contain detail
            # subprocess stdout/stderr output.
            # (2)
            # In autoscaling mode, when Ray worker node is down, autoscaler will
            # try to start new Ray worker node if necessary,
            # and it creates a new spark job to launch Ray worker node process,
            # note the old spark job will reschedule the failed spark task
            # and raise error of "Starting Ray worker node twice with the same
            # node id is not allowed".
            #
            # For either case (1) or case (2),
            # to avoid Spark triggers more spark task retries, we swallow
            # exception here to make spark the task exit normally.
            err_msg = f"Ray worker node process exit, reason: {e}."
            _logger.warning(err_msg)

            yield err_msg, is_task_reschedule_failure

    spark.sparkContext.setJobGroup(
        spark_job_group_id,
        spark_job_group_desc,
    )

    # Starting a normal spark job (not barrier spark job) to run ray worker
    # nodes, the design purpose is:
    # 1. Using normal spark job, spark tasks can automatically retry
    # individually, we don't need to write additional retry logic, But, in
    # barrier mode, if one spark task fails, it will cause all other spark
    # tasks killed.
    # 2. Using normal spark job, we can support failover when a spark worker
    # physical machine crashes. (spark will try to re-schedule the spark task
    # to other spark worker nodes)
    # 3. Using barrier mode job, if the cluster resources does not satisfy
    # "idle spark task slots >= argument num_spark_task", then the barrier
    # job gets stuck and waits until enough idle task slots available, this
    # behavior is not user-friendly, on a shared spark cluster, user is hard
    # to estimate how many idle tasks available at a time, But, if using normal
    # spark job, it can launch job with less spark tasks (i.e. user will see a
    # ray cluster setup with less worker number initially), and when more task
    # slots become available, it continues to launch tasks on new available
    # slots, and user can see the ray cluster worker number increases when more
    # slots available.
    job_rdd = spark.sparkContext.parallelize(
        list(range(num_worker_nodes)), num_worker_nodes
    )

    if using_stage_scheduling:
        resource_profile = _create_resource_profile(
            num_cpus_per_node,
            num_gpus_per_node,
        )
        job_rdd = job_rdd.withResources(resource_profile)

    hook_entry = _create_hook_entry(is_global=(ray_temp_dir is None))
    hook_entry.on_spark_job_created(spark_job_group_id)

    err_msg, is_task_reschedule_failure = job_rdd.mapPartitions(
        ray_cluster_job_mapper
    ).collect()[0]
    if not is_task_reschedule_failure:
        spark_job_server.last_worker_error = err_msg
        return err_msg

    return None


@PublicAPI
def shutdown_ray_cluster() -> None:
    """
    Shut down the active ray cluster.
    """
    global _active_ray_cluster

    with _active_ray_cluster_rwlock:
        if _active_ray_cluster is None:
            raise RuntimeError("No active ray cluster to shut down.")

        _active_ray_cluster.shutdown()
        _active_ray_cluster = None


_global_ray_cluster_cancel_event = None


@DeveloperAPI
class AutoscalingCluster:
    """Create a ray on spark autoscaling cluster."""

    def __init__(
        self,
        head_resources: dict,
        worker_node_types: dict,
        extra_provider_config: dict,
        upscaling_speed: float,
        idle_timeout_minutes: float,
    ):
        """Create the cluster.

        Args:
            head_resources: resources of the head node, including CPU.
            worker_node_types: autoscaler node types config for worker nodes.
        """
        self._head_resources = head_resources.copy()
        self._head_resources["NODE_ID_AS_RESOURCE"] = HEAD_NODE_ID
        self._config = self._generate_config(
            head_resources,
            worker_node_types,
            extra_provider_config,
            upscaling_speed,
            idle_timeout_minutes,
        )

    def _generate_config(
        self,
        head_resources,
        worker_node_types,
        extra_provider_config,
        upscaling_speed,
        idle_timeout_minutes,
    ):
        base_config = yaml.safe_load(
            open(
                os.path.join(
                    os.path.dirname(ray.__file__),
                    "autoscaler/spark/defaults.yaml",
                )
            )
        )
        custom_config = copy.deepcopy(base_config)
        custom_config["available_node_types"] = worker_node_types
        custom_config["available_node_types"]["ray.head.default"] = {
            "resources": head_resources,
            "node_config": {},
            "max_workers": 0,
        }

        custom_config["max_workers"] = sum(
            v["max_workers"] for _, v in worker_node_types.items()
        )

        custom_config["provider"].update(extra_provider_config)

        custom_config["upscaling_speed"] = upscaling_speed
        custom_config["idle_timeout_minutes"] = idle_timeout_minutes

        return custom_config

    def start(
        self,
        ray_head_ip,
        ray_head_port,
        ray_client_server_port,
        ray_temp_dir,
        dashboard_options,
        head_node_options,
        collect_log_to_path,
        ray_node_custom_env,
    ):
        """Start the cluster.

        After this call returns, you can connect to the cluster with
        ray.init("auto").
        """
        from ray.util.spark.cluster_init import (
            RAY_ON_SPARK_COLLECT_LOG_TO_PATH,
            _append_resources_config,
            _convert_ray_node_options,
        )

        if ray_temp_dir is not None:
            autoscale_config = os.path.join(ray_temp_dir, "autoscaling_config.json")
        else:
            autoscale_config = os.path.join(
                _get_default_ray_tmp_dir(), "autoscaling_config.json"
            )
        with open(autoscale_config, "w") as f:
            f.write(json.dumps(self._config))

        (
            worker_port_range_begin,
            worker_port_range_end,
        ) = _preallocate_ray_worker_port_range()

        ray_head_node_cmd = [
            sys.executable,
            "-m",
            "ray.util.spark.start_ray_node",
            "--block",
            "--head",
            f"--node-ip-address={ray_head_ip}",
            f"--port={ray_head_port}",
            f"--ray-client-server-port={ray_client_server_port}",
            f"--autoscaling-config={autoscale_config}",
            f"--min-worker-port={worker_port_range_begin}",
            f"--max-worker-port={worker_port_range_end - 1}",
            *dashboard_options,
        ]

        if ray_temp_dir is not None:
            ray_head_node_cmd.append(f"--temp-dir={ray_temp_dir}")

        if "CPU" in self._head_resources:
            ray_head_node_cmd.append(
                "--num-cpus={}".format(self._head_resources.pop("CPU"))
            )
        if "GPU" in self._head_resources:
            ray_head_node_cmd.append(
                "--num-gpus={}".format(self._head_resources.pop("GPU"))
            )
        if "memory" in self._head_resources:
            ray_head_node_cmd.append(
                "--memory={}".format(self._head_resources.pop("memory"))
            )
        if "object_store_memory" in self._head_resources:
            ray_head_node_cmd.append(
                "--object-store-memory={}".format(
                    self._head_resources.pop("object_store_memory")
                )
            )

        head_node_options = _append_resources_config(
            head_node_options, self._head_resources
        )
        ray_head_node_cmd.extend(_convert_ray_node_options(head_node_options))

        extra_env = {
            "AUTOSCALER_UPDATE_INTERVAL_S": "1",
            RAY_ON_SPARK_COLLECT_LOG_TO_PATH: collect_log_to_path or "",
            RAY_ON_SPARK_START_RAY_PARENT_PID: str(os.getpid()),
            **ray_node_custom_env,
        }

        self.ray_head_node_cmd = ray_head_node_cmd

        return _start_ray_head_node(
            ray_head_node_cmd, synchronous=False, extra_env=extra_env
        )


def _start_ray_head_node(ray_head_node_cmd, synchronous, extra_env):
    def preexec_function():
        # Make `start_ray_node` script and Ray node process run
        # in a separate group,
        # otherwise Ray node will be in the same group of parent process,
        # if parent process is a Jupyter notebook kernel, when user
        # clicks interrupt cell button, SIGINT signal is sent, then Ray node will
        # receive SIGINT signal, and it causes Ray node process dies.
        # `start_ray_node` script should also run in a separate group
        # because on Databricks Runtime, because if Databricks notebook
        # is detached, if the children processes don't exit within 1s,
        # they will receive SIGKILL, this behavior makes start_ray_node
        # doesn't have enough time to complete cleanup work like removing
        # temp directory and collecting logs.
        os.setpgrp()

    return exec_cmd(
        ray_head_node_cmd,
        synchronous=synchronous,
        extra_env=extra_env,
        preexec_fn=preexec_function,
    )


_sigterm_signal_installed = False


def _install_sigterm_signal():
    global _sigterm_signal_installed

    if _sigterm_signal_installed:
        return

    try:
        _origin_sigterm_handler = signal.getsignal(signal.SIGTERM)

        def _sigterm_handler(signum, frame):
            try:
                shutdown_ray_cluster()
            except Exception:
                # swallow exception to continue executing the following code in the
                # handler
                pass
            signal.signal(
                signal.SIGTERM, _origin_sigterm_handler
            )  # Reset to original signal
            os.kill(
                os.getpid(), signal.SIGTERM
            )  # Re-raise the signal to trigger original behavior

        signal.signal(signal.SIGTERM, _sigterm_handler)
        _sigterm_signal_installed = True
    except Exception:
        _logger.warning("Install Ray-on-Spark SIGTERM handler failed.")
