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

        if Version(pyspark.__version__) < Version("3.3"):
            raise RuntimeError(spark_dependency_error)
    except ImportError:
        raise RuntimeError(spark_dependency_error)


class RayClusterOnSpark:
    """
    This class is the type of instance returned by the `init_ray_cluster` API.
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
    ):
        self.address = address
        self.head_proc = head_proc
        self.spark_job_group_id = spark_job_group_id
        self.num_worker_nodes = num_workers_node
        self.temp_dir = temp_dir

        self.ray_context = None
        self.is_shutdown = False
        self.spark_job_is_canceled = False
        self.background_job_exception = None

    def _cancel_background_spark_job(self):
        self.spark_job_is_canceled = True
        get_spark_session().sparkContext.cancelJobGroup(self.spark_job_group_id)

    def connect(self):
        import ray

        if self.background_job_exception is not None:
            raise RuntimeError(
                "Ray workers has exited."
            ) from self.background_job_exception

        if self.is_shutdown:
            raise RuntimeError(
                "The ray cluster has been shut down or it failed to start."
            )
        if self.ray_context is None:
            try:
                # connect to the ray cluster.
                self.ray_context = ray.init(address=self.address)
            except Exception:
                self.shutdown()
                raise

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
        else:
            _logger.warning("Already connected to this ray cluster.")

    def disconnect(self):
        if self.ray_context is not None:
            try:
                self.ray_context.disconnect()
            except Exception as e:
                # swallow exception.
                _logger.warning(
                    f"An error occurred while disconnecting from the ray cluster: "
                    f"{repr(e)}"
                )
            self.ray_context = None
        else:
            _logger.warning("Already disconnected from this ray cluster.")

    def shutdown(self, cancel_background_job=True):
        """
        Shutdown the ray cluster created by the `init_ray_cluster` API.
        NB: In the background thread that runs the background spark job, if spark job
        raise unexpected error, its exception handler will also call this method, in
        the case, it will set cancel_background_job=False to avoid recursive call.
        """
        if not self.is_shutdown:
            if self.ray_context is not None:
                self.disconnect()
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


def _convert_ray_node_options(options):
    return [f"--{k.replace('_', '-')}={str(v)}" for k, v in options.items()]


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


def _init_ray_cluster(
    num_worker_nodes,
    object_store_memory_per_node=None,
    head_options=None,
    worker_options=None,
    ray_temp_root_dir=None,
    safe_mode=False,
    collect_log_to_path=None,
):
    """
    This function is used in testing, it has the same arguments with
    `ray.util.spark.init_ray_cluster` API, but it returns a `RayClusterOnSpark`
    instance instead.

    The returned instance can be used to connect to, disconnect from and shutdown the
    ray cluster. This instance can also be used as a context manager (used by
    encapsulating operations within `with init_ray_cluster(...):`). Upon entering the
    managed scope, the ray cluster is initiated and connected to. When exiting the
    scope, the ray cluster is disconnected and shut down.
    """
    from pyspark.util import inheritable_thread_target

    if RAY_ON_SPARK_START_HOOK in os.environ:
        start_hook = _load_class(os.environ[RAY_ON_SPARK_START_HOOK])()
    elif is_in_databricks_runtime():
        start_hook = DefaultDatabricksRayOnSparkStartHook()
    else:
        start_hook = RayOnSparkStartHook()

    head_options = head_options or {}
    worker_options = worker_options or {}

    spark = get_spark_session()

    # Environment configurations within the Spark Session that dictate how many cpus
    # and gpus to use for each submitted spark task.
    num_spark_task_cpus = int(spark.sparkContext.getConf().get("spark.task.cpus", "1"))
    num_spark_task_gpus = int(
        spark.sparkContext.getConf().get("spark.task.resource.gpu.amount", "0")
    )

    (
        ray_worker_node_heap_mem_bytes,
        ray_worker_node_object_store_mem_bytes,
    ) = get_avail_mem_per_ray_worker_node(spark, object_store_memory_per_node)

    max_concurrent_tasks = get_max_num_concurrent_tasks(spark.sparkContext)
    if num_worker_nodes == -1:
        # num_worker_nodes=-1 represents using all available spark task slots
        num_worker_nodes = max_concurrent_tasks
    elif num_worker_nodes <= 0:
        raise ValueError(
            "The value of 'num_worker_nodes' argument must be either a positive "
            "integer or 'ray.util.spark.MAX_NUM_WORKER_NODES'."
        )

    insufficient_resources = []

    if num_spark_task_cpus < 4:
        insufficient_resources.append(
            "The provided CPU resources for each ray worker are inadequate to start "
            "a ray cluster. Based on the total cpu resources available and the "
            "configured task sizing, each ray worker would start with "
            f"{num_spark_task_cpus} CPU cores. This is less than the recommended "
            "value of `4` CPUs per worker. Increasing the spark configuration "
            "'spark.task.cpus' to a minimum of `4` addresses it."
        )

    if ray_worker_node_heap_mem_bytes < 10 * 1024 * 1024 * 1024:
        insufficient_resources.append(
            "The provided memory resources for each ray worker are inadequate. Based "
            "on the total memory available on the spark cluster and the configured "
            "task sizing, each ray worker would start with "
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
                "'init_ray_cluster'), safe mode requires the spark cluster config "
                "satisfying following criterion: "
                "\n".join(insufficient_resources)
            )
        else:
            _logger.warning("\n".join(insufficient_resources))

    ray_head_ip = socket.gethostbyname(get_spark_application_driver_host(spark))

    ray_head_port = get_random_unused_port(ray_head_ip, min_port=9000, max_port=10000)
    ray_dashboard_port = get_random_unused_port(
        ray_head_ip, min_port=9000, max_port=10000, exclude_list=[ray_head_port]
    )
    ray_dashboard_agent_port = get_random_unused_port(
        ray_head_ip,
        min_port=9000,
        max_port=10000,
        exclude_list=[ray_head_port, ray_dashboard_port],
    )

    _logger.info(f"Ray head hostname {ray_head_ip}, port {ray_head_port}")

    temp_dir_unique_suffix = uuid.uuid4().hex[:8]

    if ray_temp_root_dir is None:
        ray_temp_root_dir = start_hook.get_default_temp_dir()
    ray_temp_dir = os.path.join(
        ray_temp_root_dir, f"ray-{ray_head_port}-{temp_dir_unique_suffix}"
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
        "--include-dashboard=true",
        "--dashboard-host=0.0.0.0",
        f"--dashboard-port={ray_dashboard_port}",
        f"--dashboard-agent-listen-port={ray_dashboard_agent_port}",
        # disallow ray tasks with cpu requirements from being scheduled on the head
        # node.
        "--num-cpus=0",
        # limit the memory allocation to the head node (actual usage may increase
        # beyond this for processing of tasks and actors).
        f"--memory={128 * 1024 * 1024}",
        # limit the object store memory allocation to the head node (actual usage
        # may increase beyond this for processing of tasks and actors).
        f"--object-store-memory={128 * 1024 * 1024}",
        *_convert_ray_node_options(head_options),
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
            f"--num-cpus={num_spark_task_cpus}",
            "--block",
            f"--address={ray_head_ip}:{ray_head_port}",
            f"--memory={ray_worker_node_heap_mem_bytes}",
            f"--object-store-memory={ray_worker_node_object_store_mem_bytes}",
            f"--min-worker-port={worker_port_range_begin}",
            f"--max-worker-port={worker_port_range_end - 1}",
            f"--dashboard-agent-listen-port={ray_worker_node_dashboard_agent_port}",
            *_convert_ray_node_options(worker_options),
        ]

        ray_worker_node_extra_envs = {
            RAY_ON_SPARK_COLLECT_LOG_TO_PATH: collect_log_to_path or ""
        }

        if num_spark_task_gpus > 0:
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

    spark_job_group_id = f"ray-cluster-job-head-{ray_head_ip}-port-{ray_head_port}"

    ray_cluster_handler = RayClusterOnSpark(
        address=f"{ray_head_ip}:{ray_head_port}",
        head_proc=ray_head_proc,
        spark_job_group_id=spark_job_group_id,
        num_workers_node=num_worker_nodes,
        temp_dir=ray_temp_dir,
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
            spark.sparkContext.parallelize(
                list(range(num_worker_nodes)), num_worker_nodes
            ).mapPartitions(ray_cluster_job_mapper).collect()
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
                ray_cluster_handler.shutdown(cancel_background_job=False)
                ray_cluster_handler.background_job_exception = e

    try:
        threading.Thread(
            target=inheritable_thread_target(background_job_thread_fn), args=()
        ).start()

        # wait background spark task starting.
        for _ in range(_BACKGROUND_JOB_STARTUP_WAIT):
            time.sleep(1)
            if ray_cluster_handler.background_job_exception is not None:
                raise RuntimeError(
                    "Ray workers failed to start."
                ) from ray_cluster_handler.background_job_exception

        start_hook.on_spark_background_job_created(spark_job_group_id)
        return ray_cluster_handler
    except Exception:
        # If driver side setup ray-cluster routine raises exception, it might result
        # in part of ray processes has been launched (e.g. ray head or some ray workers
        # have been launched), calling `ray_cluster_handler.shutdown()` to kill them
        # and clean status.
        ray_cluster_handler.shutdown()
        raise


_active_ray_cluster = None


@PublicAPI(stability="alpha")
def init_ray_cluster(
    num_worker_nodes: int,
    object_store_memory_per_node: Optional[int] = None,
    head_options: Optional[Dict] = None,
    worker_options: Optional[Dict] = None,
    ray_temp_root_dir: Optional[str] = None,
    safe_mode: Optional[bool] = False,
    collect_log_to_path: Optional[str] = None,
) -> str:
    """
    Initialize a ray cluster on the spark cluster by starting a ray head node in the
    spark application's driver side node.
    After creating the head node, a background spark job is created that
    generates an instance of `RayClusterOnSpark` that contains configuration for the
    ray cluster that will run on the Spark cluster's worker nodes.
    After a ray cluster initialized, your python process automatically connect to the
    ray cluster, you can call `ray.util.spark.shutdown_ray_cluster` to shut down the
    ray cluster.
    Note: If the active ray cluster haven't shut down, you cannot create a new ray
    cluster.

    Args
        num_worker_nodes: The number of spark worker nodes that the spark job will be
            submitted to. This argument represents how many concurrent spark tasks will
            be available in the creation of the ray cluster. The ray cluster's total
            available resources (memory, CPU and/or GPU) is equal to the quantity of
            resources allocated within these spark tasks.
            Specifying the `num_worker_nodes` as `-1` represents a ray cluster
            configuration that will use all available spark tasks slots (and resources
            allocated to the spark application) on the spark cluster.
            To create a spark cluster that is intended to be used exclusively as a
            shared ray cluster, it is recommended to set this argument to
            `ray.spark.utils.MAX_NUM_WORKER_NODES`.
        object_store_memory_per_node: Object store memory available to per-ray worker
            node, but it is capped by
            "dev_shm_available_size * 0.8 / num_tasks_per_spark_worker".
            The default value equals to
            "dev_shm_available_size * 0.8 / num_tasks_per_spark_worker".
        head_options: A dict representing Ray head node options.
        worker_options: A dict representing Ray worker node options.
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

    cluster = _init_ray_cluster(
        num_worker_nodes=num_worker_nodes,
        object_store_memory_per_node=object_store_memory_per_node,
        head_options=head_options,
        worker_options=worker_options,
        ray_temp_root_dir=ray_temp_root_dir,
        safe_mode=safe_mode,
        collect_log_to_path=collect_log_to_path,
    )
    cluster.connect()  # NB: this line might raise error.

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
