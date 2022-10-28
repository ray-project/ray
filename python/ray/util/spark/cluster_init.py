import os
import shutil
import subprocess
import sys
import time
import threading
import logging
import uuid
from packaging.version import Version

from ray.util.annotations import PublicAPI

from .utils import (
    exec_cmd,
    check_port_open,
    get_safe_port,
    get_spark_session,
    get_spark_application_driver_host,
    is_in_databricks_runtime,
    get_spark_task_assigned_physical_gpus,
    get_avail_mem_per_ray_worker,
    get_dbutils,
    get_max_num_concurrent_tasks,
    get_target_spark_tasks,
    _HEAP_TO_SHARED_RATIO,
    _port_acquisition_delay,
)

if not sys.platform.startswith("linux"):
    raise RuntimeError("Ray on spark ony supports linux system.")

_logger = logging.getLogger("ray.util.spark")

_spark_dependency_error = "ray.spark module requires pyspark >= 3.3"
try:
    import pyspark

    if Version(pyspark.__version__) < Version("3.3"):
        raise RuntimeError(_spark_dependency_error)
except ImportError:
    raise RuntimeError(_spark_dependency_error)


def wait_ray_node_available(hostname, port, timeout, error_on_failure):
    # Wait for the Ray head node to start.
    for _ in range(timeout):
        time.sleep(1)
        if check_port_open(hostname, port):
            break

    if not check_port_open(hostname, port):
        raise RuntimeError(error_on_failure)


@PublicAPI(stability="alpha")
class RayClusterOnSpark:
    """
    This class is the type of instance returned by the `init_ray_cluster` API.
    Its main functionality is to:
    Connect to, disconnect from, and shutdown the Ray cluster running on Apache Spark.
    Serve as a Python context manager for the `RayClusterOnSpark` instance.

    Args
        address: The url for the ray head node (defined as the hostname and unused port on
            Spark driver node)
        head_proc: Ray head process
        spark_job_group_id: The Spark job id for a submitted ray job
        num_ray_workers: The number of workers in the ray cluster.
    """

    def __init__(self, address, head_proc, spark_job_group_id, num_ray_workers):
        self.address = address
        self.head_proc = head_proc
        self.spark_job_group_id = spark_job_group_id
        self.ray_context = None
        self.is_shutdown = False
        self.num_ray_workers = num_ray_workers

    def _cancel_background_spark_job(self):
        get_spark_session().sparkContext.cancelJobGroup(self.spark_job_group_id)

    def connect(self):
        import ray

        if self.is_shutdown:
            raise RuntimeError(
                "The ray cluster has been shut down or it failed to start."
            )
        if self.ray_context is None:
            # connect to the ray cluster.
            self.ray_context = ray.init(address=self.address)

            last_alive_worker_count = 0
            last_progress_move_time = time.time()
            while True:
                time.sleep(10)
                cur_alive_worker_count = len(
                    [
                        node
                        for node in ray.nodes()
                        if node["Alive"]
                    ]
                )
                # The head node is included. If alive count is greater than worker count,
                # the ray cluster is fully initialized
                if cur_alive_worker_count > self.num_ray_workers:
                    return

                if cur_alive_worker_count > last_alive_worker_count:
                    last_alive_worker_count = cur_alive_worker_count
                    last_progress_move_time = time.time()
                    _logger.info(
                        "Ray worker nodes are starting. Progress: "
                        f"({cur_alive_worker_count} / {self.num_ray_workers})"
                    )
                else:
                    if time.time() - last_progress_move_time > 120:
                        _logger.warning(
                            "Timeout in waiting for all ray workers to start. Started / Total "
                            f"requested: ({cur_alive_worker_count} / {self.num_ray_workers}). "
                            "Please check ray logs to see why some ray workers failed to start."
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

    def shutdown(self):
        """
        Shutdown the ray cluster created by the `init_ray_cluster` API.
        """
        if not self.is_shutdown:
            if self.ray_context is not None:
                self.disconnect()
            try:
                self._cancel_background_spark_job()
            except Exception as e:
                # swallow exception.
                _logger.warning(
                    f"An error occurred while cancelling the ray cluster background spark job: "
                    f"{repr(e)}"
                )
            try:
                self.head_proc.kill()
            except Exception as e:
                # swallow exception.
                _logger.warning(
                    f"An Error occurred during shutdown of ray head node: {repr(e)}"
                )
            self.is_shutdown = True
        else:
            _logger.warning("The cluster has been shut down.")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()


def _convert_ray_node_options(options):
    return [f"--{k.replace('_', '-')}={str(v)}" for k, v in options.items()]


@PublicAPI(stability="alpha")
def init_ray_cluster(
    num_worker_nodes=None,
    total_cpus=None,
    total_gpus=None,
    total_heap_memory_bytes=None,
    total_object_store_memory_bytes=None,
    heap_to_object_store_memory_ratio=None,
    head_options=None,
    worker_options=None,
    ray_temp_root_dir="/tmp",
    ray_log_root_dir="/tmp",
    safe_mode=True,
):
    """
    Initialize a ray cluster on the spark cluster by starting a ray head node in the spark
    application's driver side node.
    After creating the head node, a background spark job is created that
    generates an instance of `RayClusterOnSpark` that contains configuration for the ray cluster
    that will run on the Spark cluster's worker nodes.
    The returned instance can be used to connect to, disconnect from and shutdown the ray cluster.
    This instance can also be used as a context manager (used by encapsulating operations within
    `with init_ray_cluster(...):`). Upon entering the managed scope, the ray cluster is initiated
    and connected to. When exiting the scope, the ray cluster is disconnected and shut down.

    Args
        num_worker_nodes: The number of spark worker nodes that the spark job will be submitted to.
            This argument represents how many concurrent spark tasks will be available in the
            creation of the ray cluster. The ray cluster's total available resources
            (memory, CPU and/or GPU)
            is equal to the quantity of resources allocated within these spark tasks.
            Specifying the `num_worker_nodes` as `-1` represents a ray cluster configuration that
            will use all available spark tasks slots (and resources allocated to the spark
            application) on the spark cluster.
            To create a spark cluster that is intended to be used exclusively as a shared ray
            cluster, it is recommended to set this argument to `-1`.
        total_cpus: The total cpu core count for the ray cluster to utilize.
        total_gpus: The total gpu count for the ray cluster to utilize.
        total_heap_memory_bytes: The total amount of heap memory (in bytes) for the ray cluster
            to utilize.
        total_object_store_memory_bytes: The total amount of object store memory (in bytes) for
            the ray cluster to utilize.
        heap_to_object_store_memory_ratio: The ratio of per-ray worker node available memory to the
            size of the `/dev/shm` capacity per worker on the spark worker. Without modification,
            this ratio is 0.4.
        head_options: A dict representing Ray head node options.
        worker_options: A dict representing Ray worker node options.
        ray_temp_root_dir: A local disk path to store the ray temporary data. The created cluster
            will create a subdirectory "ray-temp-{head_port}_{random_suffix}" beneath this path.
        ray_log_root_dir: A local disk path to store "ray start" script logs. The created cluster
            will create a subdirectory "ray-logs-{head_port}_{random_suffix}" beneath this path.
        safe_mode: Boolean flag to fast-fail initialization of the ray cluster if the available
            spark cluster does not have sufficient resources to fulfill the resource allocation
            for memory, cpu and gpu. When set to true, if the requested resources are
            not available for minimum recommended functionality, an exception will be raised that
            details the inadequate spark cluster configuration settings. If overridden as `False`,
            a warning is raised.
    """
    from pyspark.util import inheritable_thread_target

    head_options = head_options or {}
    worker_options = worker_options or {}

    num_worker_nodes_specified = num_worker_nodes is not None
    total_resources_req_specified = (
        total_cpus is not None
        or total_gpus is not None
        or total_heap_memory_bytes is not None
        or total_object_store_memory_bytes is not None
    )

    if (num_worker_nodes_specified and total_resources_req_specified) or (
        not num_worker_nodes_specified and not total_resources_req_specified
    ):
        raise ValueError(
            "You should specify either 'num_worker_nodes' argument or an argument group of "
            "'total_cpus', 'total_gpus', 'total_heap_memory_bytes' and "
            "'total_object_store_memory_bytes'."
        )

    spark = get_spark_session()

    # Environment configurations within the Spark Session that dictate how many cpus and gpus to
    # use for each submitted spark task.
    num_spark_task_cpus = int(spark.sparkContext.getConf().get("spark.task.cpus", "1"))
    num_spark_task_gpus = int(
        spark.sparkContext.getConf().get("spark.task.resource.gpu.amount", "0")
    )

    if heap_to_object_store_memory_ratio is None:
        heap_to_object_store_memory_ratio = _HEAP_TO_SHARED_RATIO

    (
        ray_worker_heap_mem_bytes,
        ray_worker_object_store_mem_bytes,
    ) = get_avail_mem_per_ray_worker(spark, heap_to_object_store_memory_ratio)

    if total_gpus is not None and num_spark_task_gpus == 0:
        raise ValueError(
            "The spark cluster is not configured with available GPUs. Start a GPU instance cluster "
            "to set the 'total_gpus' argument"
        )

    max_concurrent_tasks = get_max_num_concurrent_tasks(spark.sparkContext)

    num_worker_nodes = get_target_spark_tasks(
        max_concurrent_tasks,
        num_spark_task_cpus,
        num_spark_task_gpus,
        ray_worker_heap_mem_bytes,
        ray_worker_object_store_mem_bytes,
        num_worker_nodes,
        total_cpus,
        total_gpus,
        total_heap_memory_bytes,
        total_object_store_memory_bytes,
    )

    insufficient_resources = []

    if num_spark_task_cpus < 4:
        insufficient_resources.append(
            f"The provided CPU resources for each ray worker are inadequate to start a ray "
            f"cluster. Based on the total cpu resources available and the configured task sizing, "
            f"each ray worker would start with {num_spark_task_cpus} CPU cores. This is "
            "less than the recommended value of `4` CPUs per worker. Either Increasing the spark "
            "configuration 'spark.task.cpus' to a minimum of `4` or starting more "
            "spark worker nodes is recommended."
        )

    if ray_worker_heap_mem_bytes < 10 * 1024 * 1024 * 1024:
        insufficient_resources.append(
            f"The provided memory resources for each ray worker are inadequate. Based on the total "
            f"memory available on the spark cluster and the configured task sizing, each ray "
            f"worker would start with {ray_worker_heap_mem_bytes} bytes heap "
            "memory. This is less than the recommended value of 10GB. The ray worker node heap "
            "memory size is calculated by (SPARK_WORKER_NODE_PHYSICAL_MEMORY - SHARED_MEMORY) / "
            "num_local_spark_task_slots * 0.8. To increase the heap space available, "
            "increase the memory in the spark cluster by changing instance types or worker count, "
            "reduce the target `num_worker_nodes`, or apply a lower "
            "`heap_to_object_store_memory_ratio`."
        )
    if insufficient_resources:
        if safe_mode:
            raise (ValueError, "\n".join(insufficient_resources))
        else:
            _logger.warning("\n".join(insufficient_resources))

    ray_head_hostname = get_spark_application_driver_host(spark)
    ray_head_port = get_safe_port()

    _logger.info(f"Ray head hostname {ray_head_hostname}, port {ray_head_port}")

    ray_exec_path = os.path.join(os.path.dirname(sys.executable), "ray")

    temp_dir_unique_suffix = uuid.uuid4().hex[:4]
    ray_log_dir = os.path.join(
        ray_log_root_dir, f"ray-logs-{ray_head_port}-{temp_dir_unique_suffix}"
    )
    os.makedirs(ray_log_dir, exist_ok=True)

    # TODO: Many ray processes logs are outputted under "{ray_temp_dir}/session_latest/logs",
    #  Proposal: Update "ray start" scirpt to add a new option "ray_log_dir", and output logs
    #  to a different directory specified by "ray_log_dir", instead of using
    #  "{ray_temp_dir}/session_latest/logs".
    #  The reason is, for ray on spark, user is hard to access log files on spark worker machines,
    #  (especially on databricks runtime), so we'd better set the log output dir to be a
    #  path mounted with NFS shared by all spark cluster nodes, so that the user can access
    #  these remote log files from spark drive side easily.
    ray_temp_dir = os.path.join(
        ray_temp_root_dir, f"ray-temp-{ray_head_port}-{temp_dir_unique_suffix}"
    )
    os.makedirs(ray_temp_dir, exist_ok=True)

    _logger.warning(
        "Logs for ray head / worker starting script can be found in local disk path "
        f"{ray_log_dir}. Logs for ray processes can be found in local disk path "
        f"{ray_temp_dir}/session_latest/logs."
    )

    ray_head_node_cmd = [
        ray_exec_path,
        "start",
        f"--temp-dir={ray_temp_dir}",
        "--block",
        "--head",
        "--disable-usage-stats",
        f"--port={ray_head_port}",
        "--include-dashboard=false",
        # disallow ray tasks with cpu requirements from being scheduled on the head node.
        f"--num-cpus=0",
        # limit the memory allocation to the head node (actual usage may increase beyond this
        # for processing of tasks and actors).
        f"--memory={128 * 1024 * 1024}",
        # limit the object store memory allocation to the head node (actual usage may increase
        # beyond this for processing of tasks and actors).
        f"--object-store-memory={128 * 1024 * 1024}",
        *_convert_ray_node_options(head_options),
    ]

    _logger.info(f"Start Ray head, command: {' '.join(ray_head_node_cmd)}")

    with open(
        os.path.join(ray_log_dir, "ray-start-head.log"), "w", buffering=1
    ) as head_log_fp:
        ray_head_proc = exec_cmd(
            ray_head_node_cmd,
            synchronous=False,
            capture_output=False,
            stream_output=False,
            stdout=head_log_fp,
            stderr=subprocess.STDOUT,
        )

    # wait ray head node spin up.
    wait_ray_node_available(
        ray_head_hostname,
        ray_head_port,
        40,
        error_on_failure="Start Ray head node failed!",
    )

    _logger.info("Ray head node started.")

    # NB:
    # In order to start ray worker nodes on spark cluster worker machines,
    # We launch a background spark job:
    #  1. Each spark task launches one ray worker node. This design ensures all ray worker nodes
    #     have the same shape (same cpus / gpus / memory configuration). If ray worker nodes have a
    #     non-uniform shape, the Ray cluster setup will be non-deterministic and could create
    #     issues with node sizing.
    #  2. A ray worker node is started via the `ray start` CLI. In each spark task, a child
    #     process is started and will execute a `ray start ...` command in blocking mode.
    #  3. Each task will acquire a file lock for 10s to ensure that the ray worker init will
    #     acquire a port connection to the ray head node that does not contend with other
    #     worker processes on the same Spark worker node.
    #  4. When the ray cluster is shutdown, killing ray worker nodes is implemented by:
    #     Installing a PR_SET_PDEATHSIG signal for the `ray start ...` child processes
    #     so that when parent process (pyspark task) is killed, the child processes
    #     (`ray start ...` processes) will receive a SIGTERM signal, killing it.
    #     Shutting down the ray cluster is performed by calling `sparkContext.cancelJobGroup`
    #     to cancel the background spark job, sending a SIGKILL signal to all spark tasks.
    #     Once the spark tasks are killed, this triggers the sending of a SIGTERM to the child
    #     processes spawned by the `ray_start ...` process.

    def ray_cluster_job_mapper(_):
        from pyspark.taskcontext import TaskContext

        _worker_logger = logging.getLogger("ray.util.spark.worker")

        context = TaskContext.get()
        task_id = context.partitionId()

        # NB: If we launch multiple ray worker nodes at the same time,
        #  it might cause Raylet to have a port conflict, likely due to a race condition.
        #  A sleep is added here to attempt to avoid resource allocation contention with available
        #  ports.
        port_acquisition_thread = threading.Thread(
            target=_port_acquisition_delay, args=[]
        )

        # Ray worker might run on a machine different with the head node, so create the
        # local log dir and temp dir again.
        os.makedirs(ray_temp_dir, exist_ok=True)
        os.makedirs(ray_log_dir, exist_ok=True)

        ray_worker_cmd = [
            ray_exec_path,
            "start",
            f"--temp-dir={ray_temp_dir}",
            f"--num-cpus={num_spark_task_cpus}",
            "--block",
            "--disable-usage-stats",
            f"--address={ray_head_hostname}:{ray_head_port}",
            f"--memory={ray_worker_heap_mem_bytes}",
            f"--object-store-memory={ray_worker_object_store_mem_bytes}",
            *_convert_ray_node_options(worker_options),
        ]

        ray_worker_extra_envs = {}

        if num_spark_task_gpus > 0:
            task_resources = context.resources()

            if "gpu" not in task_resources:
                raise RuntimeError(
                    "Couldn't get the gpu id, Please check the GPU resource configuration"
                )
            gpu_addr_list = [
                int(addr.strip()) for addr in task_resources["gpu"].addresses
            ]

            available_physical_gpus = get_spark_task_assigned_physical_gpus(
                gpu_addr_list
            )
            ray_worker_cmd.append(
                f"--num-gpus={len(available_physical_gpus)}",
            )
            ray_worker_extra_envs["CUDA_VISIBLE_DEVICES"] = ",".join(
                [str(gpu_id) for gpu_id in available_physical_gpus]
            )

        if sys.platform.startswith("linux"):

            def setup_sigterm_on_parent_death():
                """
                Uses prctl to automatically send SIGTERM to the command process when its parent is
                dead.

                This handles the case when the parent is a PySpark worker process.
                If a user cancels the PySpark job, the worker process gets killed, regardless of
                PySpark daemon and worker reuse settings.
                We use prctl to ensure the command process receives SIGTERM after spark job
                cancellation.
                The command process itself should handle SIGTERM properly.
                This is a no-op on macOS because prctl is not supported.

                Note:
                When a pyspark job cancelled, the UDF python process are killed by signal "SIGKILL",
                This case neither "atexit" nor signal handler can capture SIGKILL signal.
                prctl is the only way to capture SIGKILL signal.
                """
                try:
                    import ctypes
                    import signal

                    libc = ctypes.CDLL("libc.so.6")
                    # Set the parent process death signal of the command process to SIGTERM.
                    libc.prctl(1, signal.SIGTERM)  # PR_SET_PDEATHSIG, see prctl.h
                except OSError as e:
                    _worker_logger.warning(
                        f"Setup libc.prctl PR_SET_PDEATHSIG failed, error {repr(e)}."
                    )

        else:
            setup_sigterm_on_parent_death = None

        _worker_logger.info(f"Start Ray worker, command: {' '.join(ray_worker_cmd)}")

        def _worker_exec_cmd(
            cmd,
            synchronous,
            capture_output,
            stream_output,
            extra_env,
            preexec_fn,
            stdout,
            stderr,
        ):
            exec_cmd(
                cmd,
                synchronous=synchronous,
                capture_output=capture_output,
                stream_output=stream_output,
                extra_env=extra_env,
                preexec_fn=preexec_fn,
                stdout=stdout,
                stderr=stderr,
            )

        # TODO: write to NFS mount
        with open(
            os.path.join(ray_log_dir, f"ray-start-worker-{task_id}.log"),
            "w",
            buffering=1,
        ) as worker_log_fp:
            worker_thread = threading.Thread(
                target=_worker_exec_cmd,
                args=(
                    ray_worker_cmd,
                    True,
                    False,
                    False,
                    ray_worker_extra_envs,
                    setup_sigterm_on_parent_death,
                    worker_log_fp,
                    subprocess.STDOUT,
                ),
            )
            port_acquisition_thread.start()
            worker_thread.start()
            port_acquisition_thread.join(timeout=60 * 4)

        # Delete the worker temp and log directories at the conclusion of running the
        # submitted task.
        shutil.rmtree(ray_temp_dir, ignore_errors=True)
        shutil.rmtree(ray_log_dir, ignore_errors=True)

        # NB: Not reachable.
        yield 0

    spark_job_group_id = (
        f"ray-cluster-job-head-{ray_head_hostname}-port-{ray_head_port}"
    )

    ray_cluster_handler = RayClusterOnSpark(
        address=f"{ray_head_hostname}:{ray_head_port}",
        head_proc=ray_head_proc,
        spark_job_group_id=spark_job_group_id,
        num_ray_workers=num_worker_nodes,
    )

    def backgroud_job_thread_fn():

        try:
            spark.sparkContext.setJobGroup(
                spark_job_group_id,
                "This job group is for spark job which runs the Ray cluster with ray head node "
                f"{ray_head_hostname}:{ray_head_port}",
            )
            spark.sparkContext.parallelize(
                list(range(num_worker_nodes)), num_worker_nodes
            ).mapPartitions(ray_cluster_job_mapper).collect()
        finally:
            # NB:
            # The background spark job is designed to running forever until it is killed,
            # So this `finally` block is reachable only when:
            #  1. The background job raises unexpected exception (i.e. ray cluster dies
            #    unexpectedly)
            #  2. User explicitly orders shutting down the ray cluster.
            #  3. On Databricks runtime, when a notebook is detached, it triggers python REPL
            #    `onCancel` event, cancelling the background running spark job
            #  For case 1 and 3, only ray workers are killed, but driver side ray head might still
            #  be running and the ray context might be in connected status. In order to disconnect
            #  and kill the ray head node, a call to `ray_cluster_handler.shutdown()` is performed.
            ray_cluster_handler.shutdown()

    try:
        threading.Thread(
            target=inheritable_thread_target(backgroud_job_thread_fn), args=()
        ).start()

        time.sleep(5)  # wait background spark task starting.

        if is_in_databricks_runtime():
            try:
                get_dbutils().entry_point.registerBackgroundSparkJobGroup(
                    spark_job_group_id
                )
            except Exception:
                _logger.warning(
                    "Register ray cluster spark job as background job failed. You need to manually "
                    "call `ray_cluster_on_spark.shutdown()` before detaching your databricks "
                    "python REPL."
                )
        return ray_cluster_handler
    except Exception:
        # If driver side setup ray-cluster routine raises exception, it might result in part of ray
        # processes has been launched (e.g. ray head or some ray workers have been launched),
        # calling `ray_cluster_handler.shutdown()` to kill them and clean status.
        ray_cluster_handler.shutdown()
        raise
