import os
import socket
import sys
import time
import threading
import logging
import uuid
from packaging.version import Version
from typing import Optional, Dict

import ray
from ray.util.annotations import PublicAPI
from ray._private.storage import _load_class

from .utils import (
    exec_cmd,
    check_port_open,
    get_random_unused_port,
    get_spark_session,
    get_spark_application_driver_host,
    is_in_databricks_runtime,
    get_spark_task_assigned_physical_gpus,
    get_avail_mem_per_ray_worker_node,
    get_max_num_concurrent_tasks,
    gen_cmd_exec_failure_msg,
    setup_sigterm_on_parent_death,
)
from .start_hook_base import RayOnSparkStartHook
from .databricks_hook import DefaultDatabricksRayOnSparkStartHook


_logger = logging.getLogger("ray.util.spark")
_logger.setLevel(logging.INFO)


RAY_ON_SPARK_START_HOOK = "RAY_ON_SPARK_START_HOOK"

MAX_NUM_WORKER_NODES = -1

RAY_ON_SPARK_COLLECT_LOG_TO_PATH = "RAY_ON_SPARK_COLLECT_LOG_TO_PATH"


def _check_system_environment():
    if not sys.platform.startswith("linux"):
        raise RuntimeError("Ray on spark only supports running on Linux.")

    spark_dependency_error = "ray.util.spark module requires pyspark >= 3.3"
    try:
        import pyspark

        if Version(pyspark.__version__).release < (3, 3, 0):
            raise RuntimeError(spark_dependency_error)
    except ImportError:
        raise RuntimeError(spark_dependency_error)


class RayClusterOnSpark:
    """
    This class is the type of instance returned by the `setup_ray_cluster` API.
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
        spark_job_group_id,
        num_workers_node,
        temp_dir,
        cluster_unique_id,
    ):
        self.address = address
        self.head_proc = head_proc
        self.spark_job_group_id = spark_job_group_id
        self.num_worker_nodes = num_workers_node
        self.temp_dir = temp_dir
        self.cluster_unique_id = cluster_unique_id

        self.is_shutdown = False
        self.spark_job_is_canceled = False
        self.background_job_exception = None

    def _cancel_background_spark_job(self):
        self.spark_job_is_canceled = True
        get_spark_session().sparkContext.cancelJobGroup(self.spark_job_group_id)

    def wait_until_ready(self):
        import ray

        if self.background_job_exception is not None:
            raise RuntimeError(
                "Ray workers has exited."
            ) from self.background_job_exception

        if self.is_shutdown:
            raise RuntimeError(
                "The ray cluster has been shut down or it failed to start."
            )
        try:
            # connect to the ray cluster.
            ray.init(address=self.address)
        except Exception:
            self.shutdown()
            raise

        try:
            last_alive_worker_count = 0
            last_progress_move_time = time.time()
            while True:
                time.sleep(_RAY_CLUSTER_STARTUP_PROGRESS_CHECKING_INTERVAL)
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
                        _logger.warning(
                            "Timeout in waiting for all ray workers to start. "
                            "Started / Total requested: "
                            f"({cur_alive_worker_count} / {self.num_worker_nodes}). "
                            "Please check ray logs to see why some ray workers "
                            "failed to start."
                        )
                        return
        finally:
            ray.shutdown()

    def connect(self):
        if ray.is_initialized():
            raise RuntimeError("Already connected to Ray cluster.")
        ray.init(address=self.address)

    def disconnect(self):
        ray.shutdown()

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
            if cancel_background_job:
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
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()


def _convert_ray_node_option_key(key):
    return f"--{key.replace('_', '-')}"


def _convert_ray_node_options(options):
    return [f"{_convert_ray_node_option_key(k)}={str(v)}" for k, v in options.items()]


_RAY_HEAD_STARTUP_TIMEOUT = 5
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


def _setup_ray_cluster(
    *,
    num_worker_nodes: int,
    num_cpus_per_node: int,
    num_gpus_per_node: int,
    using_stage_scheduling: bool,
    heap_memory_per_node: int,
    object_store_memory_per_node: int,
    head_node_options: Dict,
    worker_node_options: Dict,
    ray_temp_root_dir: str,
    collect_log_to_path: str,
):
    """
    The public API `ray.util.spark.setup_ray_cluster` does some argument
    validation and then pass validated arguments to this interface.
    and it returns a `RayClusterOnSpark` instance.

    The returned instance can be used to connect to, disconnect from and shutdown the
    ray cluster. This instance can also be used as a context manager (used by
    encapsulating operations within `with setup_ray_cluster(...):`). Upon entering the
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

    include_dashboard = head_node_options.pop("include_dashboard", None)
    ray_dashboard_port = head_node_options.pop("dashboard_port", None)

    if include_dashboard is None or include_dashboard is True:
        if ray_dashboard_port is None:
            ray_dashboard_port = get_random_unused_port(
                ray_head_ip, min_port=9000, max_port=10000, exclude_list=[ray_head_port]
            )
        ray_dashboard_agent_port = get_random_unused_port(
            ray_head_ip,
            min_port=9000,
            max_port=10000,
            exclude_list=[ray_head_port, ray_dashboard_port],
        )

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

    ray_head_node_cmd = [
        sys.executable,
        "-m",
        "ray.util.spark.start_ray_node",
        f"--temp-dir={ray_temp_dir}",
        "--block",
        "--head",
        f"--node-ip-address={ray_head_ip}",
        f"--port={ray_head_port}",
        # disallow ray tasks with cpu/gpu requirements from being scheduled on the head
        # node.
        "--num-cpus=0",
        "--num-gpus=0",
        # limit the memory allocation to the head node (actual usage may increase
        # beyond this for processing of tasks and actors).
        f"--memory={128 * 1024 * 1024}",
        # limit the object store memory allocation to the head node (actual usage
        # may increase beyond this for processing of tasks and actors).
        f"--object-store-memory={128 * 1024 * 1024}",
        *dashboard_options,
        *_convert_ray_node_options(head_node_options),
    ]

    _logger.info(f"Starting Ray head, command: {' '.join(ray_head_node_cmd)}")

    # `preexec_fn=setup_sigterm_on_parent_death` ensures the ray head node being
    # killed if parent process died unexpectedly.
    ray_head_proc, tail_output_deque = exec_cmd(
        ray_head_node_cmd,
        synchronous=False,
        preexec_fn=setup_sigterm_on_parent_death,
        extra_env={RAY_ON_SPARK_COLLECT_LOG_TO_PATH: collect_log_to_path or ""},
    )

    # wait ray head node spin up.
    time.sleep(_RAY_HEAD_STARTUP_TIMEOUT)

    if not check_port_open(ray_head_ip, ray_head_port):
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

    if include_dashboard:
        start_hook.on_ray_dashboard_created(ray_dashboard_port)

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
    #  4. When the ray cluster is shutdown, killing ray worker nodes is implemented by:
    #     Installing a PR_SET_PDEATHSIG signal for the `ray start ...` child processes
    #     so that when parent process (pyspark task) is killed, the child processes
    #     (`ray start ...` processes) will receive a SIGTERM signal, killing it.
    #     Shutting down the ray cluster is performed by calling
    #     `sparkContext.cancelJobGroup` to cancel the background spark job, sending a
    #     SIGKILL signal to all spark tasks. Once the spark tasks are killed, this
    #     triggers the sending of a SIGTERM to the child processes spawned by the
    #     `ray_start ...` process.

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
            RAY_ON_SPARK_COLLECT_LOG_TO_PATH: collect_log_to_path or ""
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

        # `preexec_fn=setup_sigterm_on_parent_death` handles the case:
        # If a user cancels the PySpark job, the worker process gets killed, regardless
        # of PySpark daemon and worker reuse settings.
        # We use prctl to ensure the command process receives SIGTERM after spark job
        # cancellation.
        # Note:
        # When a pyspark job cancelled, the UDF python process are killed by signal
        # "SIGKILL", This case neither "atexit" nor signal handler can capture SIGKILL
        # signal. prctl is the only way to capture SIGKILL signal.
        exec_cmd(
            ray_worker_node_cmd,
            synchronous=True,
            extra_env=ray_worker_node_extra_envs,
            preexec_fn=setup_sigterm_on_parent_death,
        )

        # NB: Not reachable.
        yield 0

    spark_job_group_id = f"ray-cluster-{ray_head_port}-{cluster_unique_id}"

    cluster_address = f"{ray_head_ip}:{ray_head_port}"
    # Set RAY_ADDRESS environment variable to the cluster address.
    os.environ["RAY_ADDRESS"] = cluster_address

    ray_cluster_handler = RayClusterOnSpark(
        address=cluster_address,
        head_proc=ray_head_proc,
        spark_job_group_id=spark_job_group_id,
        num_workers_node=num_worker_nodes,
        temp_dir=ray_temp_dir,
        cluster_unique_id=cluster_unique_id,
    )

    def background_job_thread_fn():

        try:
            spark.sparkContext.setJobGroup(
                spark_job_group_id,
                "This job group is for spark job which runs the Ray cluster with ray "
                f"head node {ray_head_ip}:{ray_head_port}",
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
        except Exception as e:
            # NB:
            # The background spark job is designed to running forever until it is
            # killed, The exception might be raised in following cases:
            #  1. The background job raises unexpected exception (i.e. ray cluster dies
            #    unexpectedly)
            #  2. User explicitly orders shutting down the ray cluster.
            #  3. On Databricks runtime, when a notebook is detached, it triggers
            #     python REPL `onCancel` event, cancelling the background running spark
            #     job.
            #  For case 1 and 3, only ray workers are killed, but driver side ray head
            #  might still be running and the ray context might be in connected status.
            #  In order to disconnect and kill the ray head node, a call to
            #  `ray_cluster_handler.shutdown()` is performed.
            if not ray_cluster_handler.spark_job_is_canceled:
                # Set `background_job_exception` attribute before calling `shutdown`
                # so inside `shutdown` we can get exception information easily.
                ray_cluster_handler.background_job_exception = e
                ray_cluster_handler.shutdown(cancel_background_job=False)

    try:
        threading.Thread(
            target=inheritable_thread_target(background_job_thread_fn), args=()
        ).start()

        # Call hook immediately after spark job started.
        start_hook.on_spark_background_job_created(spark_job_group_id)

        # wait background spark task starting.
        for _ in range(_BACKGROUND_JOB_STARTUP_WAIT):
            time.sleep(1)
            if ray_cluster_handler.background_job_exception is not None:
                raise RuntimeError(
                    "Ray workers failed to start."
                ) from ray_cluster_handler.background_job_exception

        return ray_cluster_handler
    except Exception:
        # If driver side setup ray-cluster routine raises exception, it might result
        # in part of ray processes has been launched (e.g. ray head or some ray workers
        # have been launched), calling `ray_cluster_handler.shutdown()` to kill them
        # and clean status.
        ray_cluster_handler.shutdown()
        raise


_active_ray_cluster = None


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
    "memory": None,
    "object_store_memory": None,
    "dashboard_host": None,
    "dashboard_agent_listen_port": None,
}

_worker_node_option_block_keys = {
    "temp_dir": "ray_temp_root_dir",
    "block": None,
    "head": None,
    "address": None,
    "num_cpus": "num_cpus_per_node",
    "num_gpus": "num_gpus_per_node",
    "memory": None,
    "object_store_memory": "object_store_memory_per_node",
    "dashboard_agent_listen_port": None,
}


def _verify_node_options(node_options, block_keys, node_type):
    for key in node_options:
        if key.startswith("--") or '-' in key:
            raise ValueError(
                "For a ray node option like '--foo-bar', you should convert it to following "
                "format 'foo_bar' in 'head_node_options' / 'worker_node_options' arguments."
            )

        if key in block_keys:
            common_err_msg = \
                f"Setting option {_convert_ray_node_options(key)} for {node_type} is not allowed."
            replacement_arg = block_keys[key]
            if replacement_arg:
                raise ValueError(f"{common_err_msg} You should set '{replacement_arg}' argument instead.")
            else:
                raise ValueError(f"{common_err_msg} The option is controlled by Ray on Spark routine.")


@PublicAPI(stability="alpha")
def setup_ray_cluster(
    num_worker_nodes: int,
    num_cpus_per_node: Optional[int] = None,
    num_gpus_per_node: Optional[int] = None,
    object_store_memory_per_node: Optional[int] = None,
    head_node_options: Optional[Dict] = None,
    worker_node_options: Optional[Dict] = None,
    ray_temp_root_dir: Optional[str] = None,
    safe_mode: Optional[bool] = False,
    collect_log_to_path: Optional[str] = None,
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

    Args
        num_worker_nodes: This argument represents how many ray worker nodes to start
            for the ray cluster.
            Specifying the `num_worker_nodes` as `ray.util.spark.MAX_NUM_WORKER_NODES`
            represents a ray cluster
            configuration that will use all available resources configured for the
            spark application.
            To create a spark application that is intended to exclusively run a
            shared ray cluster, it is recommended to set this argument to
            `ray.util.spark.MAX_NUM_WORKER_NODES`.
        num_cpus_per_node: Number of cpus available to per-ray worker node, if not
            provided, use spark application configuration 'spark.task.cpus' instead.
            Limitation: Only spark version >= 3.4 or Databricks Runtime 12.x supports
            setting this argument.
        num_gpus_per_node: Number of gpus available to per-ray worker node, if not
            provided, use spark application configuration
            'spark.task.resource.gpu.amount' instead.
            This argument is only available on spark cluster that is configured with
            'gpu' resources.
            Limitation: Only spark version >= 3.4 or Databricks Runtime 12.x supports
            setting this argument.
        object_store_memory_per_node: Object store memory available to per-ray worker
            node, but it is capped by
            "dev_shm_available_size * 0.8 / num_tasks_per_spark_worker".
            The default value equals to
            "0.3 * spark_worker_physical_memory * 0.8 / num_tasks_per_spark_worker".
        head_node_options: A dict representing Ray head node extra options.
        worker_node_options: A dict representing Ray worker node extra options.
        ray_temp_root_dir: A local disk path to store the ray temporary data. The
            created cluster will create a subdirectory
            "ray-{head_port}-{random_suffix}" beneath this path.
        safe_mode: Boolean flag to fast-fail initialization of the ray cluster if
            the available spark cluster does not have sufficient resources to fulfill
            the resource allocation for memory, cpu and gpu. When set to true, if the
            requested resources are not available for minimum recommended
            functionality, an exception will be raised that details the inadequate
            spark cluster configuration settings. If overridden as `False`,
            a warning is raised.
        collect_log_to_path: If specified, after ray head / worker nodes terminated,
            collect their logs to the specified path. On Databricks Runtime, we
            recommend you to specify a local path starts with '/dbfs/', because the
            path mounts with a centralized storage device and stored data is persisted
            after databricks spark cluster terminated.

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
    if not (
        spark_master.startswith("spark://") or spark_master.startswith("local-cluster[")
    ):
        raise RuntimeError(
            "Ray on Spark only supports spark cluster in standalone mode or local-cluster mode"
        )

    if (
        is_in_databricks_runtime() and
        Version(os.environ["DATABRICKS_RUNTIME_VERSION"]).major >= 12
    ):
        support_stage_scheduling = True
    else:
        import pyspark
        if Version(pyspark.__version__).release >= (3, 4, 0):
            support_stage_scheduling = True
        else:
            support_stage_scheduling = False

    # Environment configurations within the Spark Session that dictate how many cpus
    # and gpus to use for each submitted spark task.
    num_spark_task_cpus = int(spark.sparkContext.getConf().get("spark.task.cpus", "1"))

    if num_cpus_per_node is not None and num_cpus_per_node <= 0:
        raise ValueError("Argument `num_cpus_per_node` value must be > 0.")

    num_spark_task_gpus = int(
        spark.sparkContext.getConf().get("spark.task.resource.gpu.amount", "0")
    )

    if num_gpus_per_node is not None and num_spark_task_gpus == 0:
        raise ValueError(
            "The spark cluster is not configured with 'gpu' resources, so that "
            "you cannot specify the `num_gpus_per_node` argument."
        )

    if num_gpus_per_node is not None and num_gpus_per_node < 0:
        raise ValueError("Argument `num_gpus_per_node` value must be >= 0.")

    if num_cpus_per_node is not None or num_gpus_per_node is not None:
        if support_stage_scheduling:
            num_cpus_per_node = num_cpus_per_node or num_spark_task_cpus
            num_gpus_per_node = num_gpus_per_node or num_spark_task_gpus

            using_stage_scheduling = True
            res_profile = _create_resource_profile(
                num_cpus_per_node, num_gpus_per_node
            )
        else:
            raise ValueError(
                "Current spark version does not support stage scheduling, so that "
                "you cannot set the argument `num_cpus_per_node` and "
                "`num_gpus_per_node` values. Without setting the 2 arguments, "
                "per-Ray worker node will be assigned with number of "
                f"'spark.task.cpus' (equals to {num_spark_task_cpus}) cpu cores "
                "and number of 'spark.task.resource.gpu.amount' "
                f"(equals to {num_spark_task_gpus}) GPUs. To enable spark stage "
                "scheduling, you need to upgrade spark to 3.4 version or use "
                "Databricks Runtime 12.x."
            )
    else:
        using_stage_scheduling = False
        res_profile = None

        num_cpus_per_node = num_spark_task_cpus
        num_gpus_per_node = num_spark_task_gpus

    (
        ray_worker_node_heap_mem_bytes,
        ray_worker_node_object_store_mem_bytes,
    ) = get_avail_mem_per_ray_worker_node(
        spark,
        object_store_memory_per_node,
        num_cpus_per_node,
        num_gpus_per_node,
    )

    if num_worker_nodes == MAX_NUM_WORKER_NODES:
        # num_worker_nodes=MAX_NUM_WORKER_NODES represents using all available
        # spark task slots
        num_worker_nodes = get_max_num_concurrent_tasks(spark.sparkContext, res_profile)
    elif num_worker_nodes <= 0:
        raise ValueError(
            "The value of 'num_worker_nodes' argument must be either a positive "
            "integer or 'ray.util.spark.MAX_NUM_WORKER_NODES'."
        )

    insufficient_resources = []

    if num_cpus_per_node < 4:
        insufficient_resources.append(
            "The provided CPU resources for each ray worker are inadequate to start "
            "a ray cluster. Based on the total cpu resources available and the "
            "configured task sizing, each ray worker node would start with "
            f"{num_cpus_per_node} CPU cores. This is less than the recommended "
            "value of `4` CPUs per worker. On spark version >= 3.4 or Databricks "
            "Runtime 12.x, you can set the argument `num_cpus_per_node` to "
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
            "object_store_memory_per_node. To increase the heap space available, "
            "increase the memory in the spark cluster by changing instance types or "
            "worker count, reduce the target `num_worker_nodes`, or apply a lower "
            "`object_store_memory_per_node`."
        )
    if insufficient_resources:
        if safe_mode:
            raise ValueError(
                "You are creating ray cluster on spark with safe mode (it can be "
                "disabled by setting argument 'safe_mode=False' when calling API "
                "'setup_ray_cluster'), safe mode requires the spark cluster config "
                "satisfying following criterion: "
                "\n".join(insufficient_resources)
            )
        else:
            _logger.warning("\n".join(insufficient_resources))

    cluster = _setup_ray_cluster(
        num_worker_nodes=num_worker_nodes,
        num_cpus_per_node=num_cpus_per_node,
        num_gpus_per_node=num_gpus_per_node,
        using_stage_scheduling=using_stage_scheduling,
        heap_memory_per_node=ray_worker_node_heap_mem_bytes,
        object_store_memory_per_node=ray_worker_node_object_store_mem_bytes,
        head_node_options=head_node_options,
        worker_node_options=worker_node_options,
        ray_temp_root_dir=ray_temp_root_dir,
        collect_log_to_path=collect_log_to_path,
    )
    cluster.wait_until_ready()  # NB: this line might raise error.

    # If connect cluster successfully, set global _active_ray_cluster to be the started
    # cluster.
    _active_ray_cluster = cluster
    return cluster.address


@PublicAPI(stability="alpha")
def shutdown_ray_cluster() -> None:
    """
    Shut down the active ray cluster.
    """
    global _active_ray_cluster
    if _active_ray_cluster is None:
        raise RuntimeError("No active ray cluster to shut down.")

    _active_ray_cluster.shutdown()
    _active_ray_cluster = None
