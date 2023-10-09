import copy
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
from typing import Optional, Dict, Type

import ray
import ray._private.services
from ray.autoscaler._private.spark.node_provider import HEAD_NODE_ID
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray._private.storage import _load_class

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
)
from .start_hook_base import RayOnSparkStartHook
from .databricks_hook import DefaultDatabricksRayOnSparkStartHook


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
        autoscale,
        address,
        head_proc,
        spark_job_group_id,
        num_workers_node,
        temp_dir,
        cluster_unique_id,
        start_hook,
        ray_dashboard_port,
        spark_job_server,
    ):
        self.autoscale = autoscale
        self.address = address
        self.head_proc = head_proc
        self.spark_job_group_id = spark_job_group_id
        self.num_worker_nodes = num_workers_node
        self.temp_dir = temp_dir
        self.cluster_unique_id = cluster_unique_id
        self.start_hook = start_hook
        self.ray_dashboard_port = ray_dashboard_port
        self.spark_job_server = spark_job_server

        self.is_shutdown = False
        self.spark_job_is_canceled = False
        self.background_job_exception = None

        # Ray client context returns by `ray.init`
        self.ray_ctx = None

    def _cancel_background_spark_job(self):
        self.spark_job_is_canceled = True
        get_spark_session().sparkContext.cancelJobGroup(self.spark_job_group_id)

    def wait_until_ready(self):
        import ray

        if self.is_shutdown:
            raise RuntimeError(
                "The ray cluster has been shut down or it failed to start."
            )

        try:
            ray.init(address=self.address)

            if self.ray_dashboard_port is not None and _wait_service_up(
                self.address.split(":")[0],
                self.ray_dashboard_port,
                _RAY_DASHBOARD_STARTUP_TIMEOUT,
            ):
                self.start_hook.on_ray_dashboard_created(self.ray_dashboard_port)
            else:
                try:
                    __import__("ray.dashboard.optional_deps")
                except ModuleNotFoundError:
                    _logger.warning(
                        "Dependencies to launch the optional dashboard API "
                        "server cannot be found. They can be installed with "
                        "pip install ray[default]."
                    )

            if self.autoscale:
                return

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

                if cur_alive_worker_count >= self.num_worker_nodes:
                    return

                if cur_alive_worker_count > last_alive_worker_count:
                    last_alive_worker_count = cur_alive_worker_count
                    last_progress_move_time = time.time()
                    _logger.info(
                        "Ray worker nodes are starting. Progress: "
                        f"({cur_alive_worker_count} / {self.num_worker_nodes})"
                    )
                else:
                    if (
                        time.time() - last_progress_move_time
                        > _RAY_CONNECT_CLUSTER_POLL_PROGRESS_TIMEOUT
                    ):
                        if cur_alive_worker_count == 0:
                            raise RuntimeError(
                                "Current spark cluster has no resources to launch "
                                "Ray worker nodes."
                            )
                        _logger.warning(
                            "Timeout in waiting for all ray workers to start. "
                            "Started / Total requested: "
                            f"({cur_alive_worker_count} / {self.num_worker_nodes}). "
                            "Current spark cluster does not have sufficient resources "
                            "to launch requested number of Ray worker nodes."
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

    def shutdown(self, cancel_background_job=True):
        """
        Shutdown the ray cluster created by the `setup_ray_cluster` API.
        NB: In the background thread that runs the background spark job, if spark job
        raise unexpected error, its exception handler will also call this method, in
        the case, it will set cancel_background_job=False to avoid recursive call.
        """
        if not self.is_shutdown:
            self.disconnect()
            os.environ.pop("RAY_ADDRESS", None)
            if self.autoscale:
                self.spark_job_server.shutdown()
            if cancel_background_job:
                if self.autoscale:
                    pass
                else:
                    try:
                        self._cancel_background_spark_job()
                    except Exception as e:
                        # swallow exception.
                        _logger.warning(
                            f"An error occurred while cancelling the ray cluster "
                            f"background spark job: {repr(e)}"
                        )
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


def _prepare_for_ray_worker_node_startup():
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


def _setup_ray_cluster(
    *,
    num_worker_nodes: int,
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
    autoscale: bool,
    autoscale_upscaling_speed: float,
    autoscale_idle_timeout_minutes: float,
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
    from pyspark.util import inheritable_thread_target

    if RAY_ON_SPARK_START_HOOK in os.environ:
        start_hook = _load_class(os.environ[RAY_ON_SPARK_START_HOOK])()
    elif is_in_databricks_runtime():
        start_hook = DefaultDatabricksRayOnSparkStartHook()
    else:
        start_hook = RayOnSparkStartHook()

    spark = get_spark_session()

    ray_head_ip = socket.gethostbyname(get_spark_application_driver_host(spark))
    ray_head_port = get_random_unused_port(ray_head_ip, min_port=9000, max_port=10000)
    port_exclude_list = [ray_head_port]

    # Make a copy for head_node_options to avoid changing original dict in user code.
    head_node_options = head_node_options.copy()
    include_dashboard = head_node_options.pop("include_dashboard", None)
    ray_dashboard_port = head_node_options.pop("dashboard_port", None)

    if autoscale:
        spark_job_server_port = get_random_unused_port(
            ray_head_ip,
            min_port=9000,
            max_port=10000,
            exclude_list=port_exclude_list,
        )
        port_exclude_list.append(spark_job_server_port)
    else:
        spark_job_server_port = None

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

    _logger.info(f"Ray head hostname {ray_head_ip}, port {ray_head_port}")

    cluster_unique_id = uuid.uuid4().hex[:8]

    if ray_temp_root_dir is None:
        ray_temp_root_dir = start_hook.get_default_temp_dir()
    ray_temp_dir = os.path.join(
        ray_temp_root_dir, f"ray-{ray_head_port}-{cluster_unique_id}"
    )
    os.makedirs(ray_temp_dir, exist_ok=True)
    object_spilling_dir = os.path.join(ray_temp_dir, "spill")
    os.makedirs(object_spilling_dir, exist_ok=True)

    head_node_options = _append_default_spilling_dir_config(
        head_node_options, object_spilling_dir
    )

    if autoscale:
        from ray.autoscaler._private.spark.spark_job_server import (
            _start_spark_job_server,
        )

        spark_job_server = _start_spark_job_server(
            ray_head_ip, spark_job_server_port, spark
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
                    "min_workers": 0,
                    "max_workers": num_worker_nodes,
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
            ray_temp_dir,
            dashboard_options,
            head_node_options,
            collect_log_to_path,
        )
        ray_head_node_cmd = autoscaling_cluster.ray_head_node_cmd
    else:
        ray_head_node_cmd = [
            sys.executable,
            "-m",
            "ray.util.spark.start_ray_node",
            f"--temp-dir={ray_temp_dir}",
            "--block",
            "--head",
            f"--node-ip-address={ray_head_ip}",
            f"--port={ray_head_port}",
            f"--num-cpus={num_cpus_head_node}",
            f"--num-gpus={num_gpus_head_node}",
            f"--memory={heap_memory_head_node}",
            f"--object-store-memory={object_store_memory_head_node}",
            *dashboard_options,
            *_convert_ray_node_options(head_node_options),
        ]

        _logger.info(f"Starting Ray head, command: {' '.join(ray_head_node_cmd)}")

        ray_head_proc, tail_output_deque = exec_cmd(
            ray_head_node_cmd,
            synchronous=False,
            extra_env={
                RAY_ON_SPARK_COLLECT_LOG_TO_PATH: collect_log_to_path or "",
                RAY_ON_SPARK_START_RAY_PARENT_PID: str(os.getpid()),
            },
        )
        spark_job_server = None

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

    cluster_address = f"{ray_head_ip}:{ray_head_port}"
    # Set RAY_ADDRESS environment variable to the cluster address.
    os.environ["RAY_ADDRESS"] = cluster_address

    ray_cluster_handler = RayClusterOnSpark(
        autoscale=autoscale,
        address=cluster_address,
        head_proc=ray_head_proc,
        spark_job_group_id=None,
        num_workers_node=num_worker_nodes,
        temp_dir=ray_temp_dir,
        cluster_unique_id=cluster_unique_id,
        start_hook=start_hook,
        ray_dashboard_port=ray_dashboard_port,
        spark_job_server=spark_job_server,
    )

    if not autoscale:
        spark_job_group_id = f"ray-cluster-{ray_head_port}-{cluster_unique_id}"
        ray_cluster_handler.spark_job_group_id = spark_job_group_id

        def background_job_thread_fn():
            try:
                _start_ray_worker_nodes(
                    spark=spark,
                    spark_job_group_id=spark_job_group_id,
                    spark_job_group_desc=(
                        "This job group is for spark job which runs the Ray cluster "
                        f"with ray head node {ray_head_ip}:{ray_head_port}"
                    ),
                    num_worker_nodes=num_worker_nodes,
                    using_stage_scheduling=using_stage_scheduling,
                    ray_head_ip=ray_head_ip,
                    ray_head_port=ray_head_port,
                    ray_temp_dir=ray_temp_dir,
                    num_cpus_per_node=num_cpus_worker_node,
                    num_gpus_per_node=num_gpus_worker_node,
                    heap_memory_per_node=heap_memory_worker_node,
                    object_store_memory_per_node=object_store_memory_worker_node,
                    worker_node_options=worker_node_options,
                    collect_log_to_path=collect_log_to_path,
                    autoscale_mode=False,
                    spark_job_server_port=spark_job_server_port,
                )
            except Exception as e:
                # NB:
                # The background spark job is designed to running forever until it is
                # killed, The exception might be raised in following cases:
                #  1. The background job raises unexpected exception (i.e. ray cluster
                #     dies unexpectedly)
                #  2. User explicitly orders shutting down the ray cluster.
                #  3. On Databricks runtime, when a notebook is detached, it triggers
                #     python REPL `onCancel` event, cancelling the background running
                #     spark job.
                #  For case 1 and 3, only ray workers are killed, but driver side ray
                #  head might still be running and the ray context might be in
                #  connected status.
                #  In order to disconnect and kill the ray head node, a call to
                #  `ray_cluster_handler.shutdown()` is performed.
                if not ray_cluster_handler.spark_job_is_canceled:
                    # Set `background_job_exception` attribute before calling
                    # `shutdown` so inside `shutdown` we can get exception information
                    # easily.
                    ray_cluster_handler.background_job_exception = e
                    ray_cluster_handler.shutdown(cancel_background_job=False)

        try:
            threading.Thread(
                target=inheritable_thread_target(background_job_thread_fn), args=()
            ).start()

            # Call hook immediately after spark job started.
            start_hook.on_cluster_created(ray_cluster_handler)

            # wait background spark task starting.
            for _ in range(_BACKGROUND_JOB_STARTUP_WAIT):
                time.sleep(1)
                if ray_cluster_handler.background_job_exception is not None:
                    raise RuntimeError(
                        "Ray workers failed to start."
                    ) from ray_cluster_handler.background_job_exception

        except Exception:
            # If driver side setup ray-cluster routine raises exception, it might
            # result in part of ray processes has been launched (e.g. ray head or
            # some ray workers have been launched), calling
            # `ray_cluster_handler.shutdown()` to kill them and clean status.
            ray_cluster_handler.shutdown()
            raise

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


@PublicAPI(stability="alpha")
def setup_ray_cluster(
    num_worker_nodes: int,
    *,
    num_cpus_worker_node: Optional[int] = None,
    num_cpus_head_node: Optional[int] = None,
    num_gpus_worker_node: Optional[int] = None,
    num_gpus_head_node: Optional[int] = None,
    object_store_memory_worker_node: Optional[int] = None,
    object_store_memory_head_node: Optional[int] = None,
    head_node_options: Optional[Dict] = None,
    worker_node_options: Optional[Dict] = None,
    ray_temp_root_dir: Optional[str] = None,
    strict_mode: bool = False,
    collect_log_to_path: Optional[str] = None,
    autoscale: bool = False,
    autoscale_upscaling_speed: Optional[float] = 1.0,
    autoscale_idle_timeout_minutes: Optional[float] = 1.0,
    **kwargs,
) -> str:
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
        num_worker_nodes: This argument represents how many ray worker nodes to start
            for the ray cluster.
            Specifying the `num_worker_nodes` as `ray.util.spark.MAX_NUM_WORKER_NODES`
            represents a ray cluster
            configuration that will use all available resources configured for the
            spark application.
            To create a spark application that is intended to exclusively run a
            shared ray cluster, it is recommended to set this argument to
            `ray.util.spark.MAX_NUM_WORKER_NODES`.
            If autoscale=True, then the ray cluster starts with zero worker node,
            and it can scale up to at most `num_worker_nodes` worker nodes.
        num_cpus_worker_node: Number of cpus available to per-ray worker node, if not
            provided, use spark application configuration 'spark.task.cpus' instead.
            **Limitation** Only spark version >= 3.4 or Databricks Runtime 12.x
            supports setting this argument.
        num_cpus_head_node: Number of cpus available to Ray head node, if not provide,
            use 0 instead. Number 0 means tasks requiring CPU resources are not
            scheduled to Ray head node.
        num_gpus_worker_node: Number of gpus available to per-ray worker node, if not
            provided, use spark application configuration
            'spark.task.resource.gpu.amount' instead.
            This argument is only available on spark cluster that is configured with
            'gpu' resources.
            **Limitation** Only spark version >= 3.4 or Databricks Runtime 12.x
            supports setting this argument.
        num_gpus_head_node: Number of gpus available to Ray head node, if not provide,
            use 0 instead.
            This argument is only available on spark cluster which spark driver node
            has GPUs.
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
        autoscale: If True, enable autoscaling, the number of initial Ray worker nodes
            is zero, and the maximum number of Ray worker nodes is set to
            `num_worker_nodes`. Default value is False.
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
        The address of the initiated Ray cluster on spark.
    """
    global _active_ray_cluster

    _check_system_environment()

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

    num_spark_task_gpus = int(
        spark.sparkContext.getConf().get("spark.task.resource.gpu.amount", "0")
    )

    if num_gpus_worker_node is not None and num_spark_task_gpus == 0:
        raise ValueError(
            "The spark cluster worker nodes are not configured with 'gpu' resources, "
            "so that you cannot specify the `num_gpus_worker_node` argument."
        )

    if num_gpus_worker_node is not None and num_gpus_worker_node < 0:
        raise ValueError("Argument `num_gpus_worker_node` value must be >= 0.")

    if num_cpus_worker_node is not None or num_gpus_worker_node is not None:
        if support_stage_scheduling:
            num_cpus_worker_node = num_cpus_worker_node or num_spark_task_cpus
            num_gpus_worker_node = num_gpus_worker_node or num_spark_task_gpus

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
                "and number of 'spark.task.resource.gpu.amount' "
                f"(equals to {num_spark_task_gpus}) GPUs. To enable spark stage "
                "scheduling, you need to upgrade spark to 3.4 version or use "
                "Databricks Runtime 12.x, and you cannot use spark local mode."
            )
    else:
        using_stage_scheduling = False
        res_profile = None

        num_cpus_worker_node = num_spark_task_cpus
        num_gpus_worker_node = num_spark_task_gpus

    (
        ray_worker_node_heap_mem_bytes,
        ray_worker_node_object_store_mem_bytes,
    ) = get_avail_mem_per_ray_worker_node(
        spark,
        object_store_memory_worker_node,
        num_cpus_worker_node,
        num_gpus_worker_node,
    )

    if num_worker_nodes == MAX_NUM_WORKER_NODES:
        if autoscale:
            raise ValueError(
                "If you set autoscale=True, you cannot set `num_worker_nodes` to "
                "`MAX_NUM_WORKER_NODES`, instead, you should set `num_worker_nodes` "
                "to the number that represents the upper bound of the ray worker "
                "nodes number."
            )

        # num_worker_nodes=MAX_NUM_WORKER_NODES represents using all available
        # spark task slots
        num_worker_nodes = get_max_num_concurrent_tasks(spark.sparkContext, res_profile)
    elif num_worker_nodes <= 0:
        raise ValueError(
            "The value of 'num_worker_nodes' argument must be either a positive "
            "integer or 'ray.util.spark.MAX_NUM_WORKER_NODES'."
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
            "(SPARK_WORKER_NODE_PHYSICAL_MEMORY / num_local_spark_task_slots * 0.8) - "
            "object_store_memory_worker_node. To increase the heap space available, "
            "increase the memory in the spark cluster by changing instance types or "
            "worker count, reduce the target `num_worker_nodes`, or apply a lower "
            "`object_store_memory_worker_node`."
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
        num_cpus_head_node = 0
    else:
        if num_cpus_head_node < 0:
            raise ValueError(
                "Argument `num_cpus_head_node` value must be >= 0. "
                f"Current value is {num_cpus_head_node}."
            )

    if num_gpus_head_node is None:
        num_gpus_head_node = 0
    else:
        if num_gpus_head_node < 0:
            raise ValueError(
                "Argument `num_gpus_head_node` value must be >= 0."
                f"Current value is {num_gpus_head_node}."
            )

    if num_cpus_head_node == 0 and num_gpus_head_node == 0:
        # Because tasks that require CPU or GPU resources are not scheduled to Ray
        # head node, limit the heap memory and object store memory allocation to the
        # head node.
        heap_memory_head_node = 128 * 1024 * 1024
        object_store_memory_head_node = 128 * 1024 * 1024
    else:
        heap_memory_head_node, object_store_memory_head_node = calc_mem_ray_head_node(
            object_store_memory_head_node
        )

    with _active_ray_cluster_rwlock:
        cluster = _setup_ray_cluster(
            num_worker_nodes=num_worker_nodes,
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
            autoscale=autoscale,
            autoscale_upscaling_speed=autoscale_upscaling_speed,
            autoscale_idle_timeout_minutes=autoscale_idle_timeout_minutes,
        )

        cluster.wait_until_ready()  # NB: this line might raise error.

        # If connect cluster successfully, set global _active_ray_cluster to be the
        # started cluster.
        _active_ray_cluster = cluster
    return cluster.address


def _start_ray_worker_nodes(
    *,
    spark,
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
    autoscale_mode,
    spark_job_server_port,
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

    def ray_cluster_job_mapper(_):
        from pyspark.taskcontext import TaskContext

        _worker_logger = logging.getLogger("ray.util.spark.worker")

        context = TaskContext.get()

        (
            worker_port_range_begin,
            worker_port_range_end,
        ) = _prepare_for_ray_worker_node_startup()

        # Ray worker might run on a machine different with the head node, so create the
        # local log dir and temp dir again.
        os.makedirs(ray_temp_dir, exist_ok=True)

        ray_worker_node_dashboard_agent_port = get_random_unused_port(
            ray_head_ip, min_port=10000, max_port=20000
        )
        ray_worker_node_cmd = [
            sys.executable,
            "-m",
            "ray.util.spark.start_ray_node",
            f"--temp-dir={ray_temp_dir}",
            f"--num-cpus={num_cpus_per_node}",
            "--block",
            f"--address={ray_head_ip}:{ray_head_port}",
            f"--memory={heap_memory_per_node}",
            f"--object-store-memory={object_store_memory_per_node}",
            f"--min-worker-port={worker_port_range_begin}",
            f"--max-worker-port={worker_port_range_end - 1}",
            f"--dashboard-agent-listen-port={ray_worker_node_dashboard_agent_port}",
            *_convert_ray_node_options(worker_node_options),
        ]

        ray_worker_node_extra_envs = {
            RAY_ON_SPARK_COLLECT_LOG_TO_PATH: collect_log_to_path or "",
            RAY_ON_SPARK_START_RAY_PARENT_PID: str(os.getpid()),
            "RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER": "1",
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
            if autoscale_mode:
                # Notify job server the task has been launched.
                requests.post(
                    url=(
                        f"http://{ray_head_ip}:{spark_job_server_port}"
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
            if autoscale_mode:
                # In autoscaling mode, when Ray worker node is down, autoscaler will
                # try to start new Ray worker node if necessary,
                # but we use spark job to launch Ray worker node process,
                # to avoid trigger spark task retries, we swallow exception here
                # to make spark task exit normally.
                _logger.warning(f"Ray worker node process exit, reason: {repr(e)}.")
            else:
                raise

        # NB: Not reachable.
        yield 0

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

    job_rdd.mapPartitions(ray_cluster_job_mapper).collect()


@PublicAPI(stability="alpha")
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
        custom_config["provider"].update(extra_provider_config)

        custom_config["upscaling_speed"] = upscaling_speed
        custom_config["idle_timeout_minutes"] = idle_timeout_minutes

        return custom_config

    def start(
        self,
        ray_head_ip,
        ray_head_port,
        ray_temp_dir,
        dashboard_options,
        head_node_options,
        collect_log_to_path,
    ):
        """Start the cluster.

        After this call returns, you can connect to the cluster with
        ray.init("auto").
        """
        from ray.util.spark.cluster_init import (
            RAY_ON_SPARK_COLLECT_LOG_TO_PATH,
            _append_resources_config,
            _convert_ray_node_options,
            exec_cmd,
        )

        autoscale_config = os.path.join(ray_temp_dir, "autoscaling_config.json")
        with open(autoscale_config, "w") as f:
            f.write(json.dumps(self._config))

        ray_head_node_cmd = [
            sys.executable,
            "-m",
            "ray.util.spark.start_ray_node",
            f"--temp-dir={ray_temp_dir}",
            "--block",
            "--head",
            f"--node-ip-address={ray_head_ip}",
            f"--port={ray_head_port}",
            f"--autoscaling-config={autoscale_config}",
            *dashboard_options,
        ]

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
        }

        self.ray_head_node_cmd = ray_head_node_cmd

        return exec_cmd(
            ray_head_node_cmd,
            synchronous=False,
            extra_env=extra_env,
        )
