import os
import shutil
import subprocess
import sys
import time
import threading
import logging
import math
from packaging.version import Version

from .utils import (
    exec_cmd,
    check_port_open,
    get_random_safe_port,
    get_spark_session,
    get_spark_driver_hostname,
    is_in_databricks_runtime,
    get_spark_task_assigned_physical_gpus,
    get_avail_mem_per_ray_worker,
    get_dbutils,
    get_max_num_concurrent_tasks,
)

if not sys.platform.startswith("linux"):
    raise RuntimeError("Ray on spark ony supports linux system.")

_logger = logging.getLogger("ray.spark")

_spark_dependency_error = "ray.spark module requires pyspark >= 3.3"
try:
    import pyspark
    if Version(pyspark.__version__) < Version("3.3"):
        raise RuntimeError(_spark_dependency_error)
except ImportError:
    raise RuntimeError(_spark_dependency_error)


def wait_ray_node_available(hostname, port, timeout, error_on_failure):
    # Wait Ray head node spin up.
    for _ in range(timeout):
        time.sleep(1)
        if check_port_open(hostname, port):
            break

    if not check_port_open(hostname, port):
        raise RuntimeError(error_on_failure)


class RayClusterOnSpark:
    """
    The class is the type of instance returned by `init_cluster` API.
    It can be used to connect / disconnect / shut down the cluster.
    It can be also used as a python context manager,
    when entering the `RayClusterOnSpark` context, connect to the ray cluster,
    when exiting the `RayClusterOnSpark` context, disconnect from the ray cluster and
    shut down the cluster.
    """

    def __init__(self, address, head_proc, spark_job_group_id, ray_temp_dir):
        self.address = address
        self.head_proc = head_proc
        self.spark_job_group_id = spark_job_group_id
        self.ray_temp_dir = ray_temp_dir
        self.ray_context = None
        self.is_shutdown = False

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
        else:
            _logger.warning("Already connected to this ray cluster.")

    def disconnect(self):
        if self.ray_context is not None:
            try:
                self.ray_context.disconnect()
            except Exception as e:
                # swallow exception.
                _logger.warning(f"Error happens during disconnecting: {repr(e)}")
            self.ray_context = None
        else:
            _logger.warning("Already disconnected from this ray cluster.")

    def shutdown(self):
        """
        Shutdown the ray cluster created by `init_cluster` API.
        """
        if not self.is_shutdown:
            if self.ray_context is not None:
                self.disconnect()
            try:
                self._cancel_background_spark_job()
            except Exception as e:
                # swallow exception.
                _logger.warning(
                    f"Error happens during cancelling ray cluster background spark job: {repr(e)}"
                )
            try:
                self.head_proc.kill()
            except Exception as e:
                # swallow exception.
                _logger.warning(
                    f"Error happens during killing ray head node: {repr(e)}"
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


def init_cluster(
    num_spark_tasks=None,
    total_cpus=None,
    total_gpus=None,
    total_heap_memory_bytes=None,
    total_object_store_memory_bytes=None,
    head_options=None,
    worker_options=None,
    ray_temp_dir="/tmp/ray/temp",
    ray_log_dir="/tmp/ray/logs"
):
    """
    Initialize a ray cluster on the spark cluster, via starting a ray head node
    in spark drive side and creating a background spark barrier mode job and each
    spark task running a ray worker node, returns an instance of `RayClusterOnSpark` type.
    The returned instance can be used to connect / disconnect / shut down the ray cluster.
    We can also use `with` statement like `with init_cluster(...):` , when entering
    the managed scope, the ray cluster is initiated and connected, and when exiting the
    scope, the ray cluster is disconnected and shut down.

    Args
        num_spark_tasks: Specify the spark task number the spark job will create.
            This argument represents how many concurrent spark tasks it will use to create the
            ray cluster, and the ray cluster total available resources (CPU / GPU / memory)
            equals to the resources allocated to these spark tasks.
            You can specify num_spark_tasks to -1, representing the ray cluster uses all
            available spark tasks slots, if you want to create a shared ray cluster
            and use the whole spark cluster resources, simply set it to -1.
        total_cpus: Specify the total cpus resources the ray cluster requests.
        total_gpus: Specify the total gpus resources the ray cluster requests.
        total_heap_memory_bytes: Specify the total heap memory resources (in bytes)
            the ray cluster requests.
        total_object_store_memory_bytes: Specify the total object store memory resources (in bytes)
            the ray cluster requests.
        head_options: A dict representing Ray head node options.
        worker_options: A dict representing Ray worker node options.
        ray_temp_dir: A local disk path to store the ray temporary data.
        ray_log_dir: A local disk path to store "ray start" script logs.
    """
    import ray
    from pyspark.util import inheritable_thread_target

    head_options = head_options or {}
    worker_options = worker_options or {}

    num_spark_tasks_specified = num_spark_tasks is not None
    total_resources_req_specified = (
        total_cpus is not None or
        total_gpus is not None or
        total_heap_memory_bytes is not None or
        total_object_store_memory_bytes is not None
    )

    if (num_spark_tasks_specified and total_resources_req_specified) or \
            (not num_spark_tasks_specified and not total_resources_req_specified):
        raise ValueError(
            "You should specify either 'num_spark_tasks' argument or argument group of "
            "'total_cpus', 'total_gpus', 'total_heap_memory_bytes' and 'total_object_store_memory_bytes'."
        )

    spark = get_spark_session()

    num_spark_task_cpus = int(spark.sparkContext.getConf().get("spark.task.cpus", "1"))
    num_spark_task_gpus = int(spark.sparkContext.getConf().get("spark.task.resource.gpu.amount", "0"))
    ray_worker_heap_mem_bytes, ray_worker_object_store_mem_bytes = get_avail_mem_per_ray_worker(spark)

    if total_gpus is not None and num_spark_task_gpus == 0:
        raise ValueError(
            "The spark cluster is without GPU configuration, so you cannot specify 'total_gpus' "
            "argument"
        )

    if num_spark_tasks is not None:
        if num_spark_tasks == -1:
            # num_spark_tasks=-1 represents using all spark task slots
            num_spark_tasks = get_max_num_concurrent_tasks(spark.sparkContext)
        elif num_spark_tasks <= 0:
            raise ValueError(
                "You should specify 'num_spark_tasks' argument to a positive integer or -1."
            )
    else:
        num_spark_tasks = 1
        if total_cpus is not None:
            if total_cpus <= 0:
                raise ValueError(
                    "You should specify 'total_cpus' argument to a positive integer."
                )
            num_spark_tasks_for_cpus_req = int(math.ceil(total_cpus / num_spark_task_cpus))
            if num_spark_tasks_for_cpus_req > num_spark_tasks:
                num_spark_tasks = num_spark_tasks_for_cpus_req

        if total_gpus is not None:
            if total_gpus <= 0:
                raise ValueError(
                    "You should specify 'total_gpus' argument to a positive integer."
                )
            num_spark_tasks_for_gpus_req = int(math.ceil(total_gpus / num_spark_task_gpus))
            if num_spark_tasks_for_gpus_req > num_spark_tasks:
                num_spark_tasks = num_spark_tasks_for_gpus_req

        if total_heap_memory_bytes is not None:
            if total_heap_memory_bytes <= 0:
                raise ValueError(
                    "You should specify 'total_heap_memory_bytes' argument to a positive integer."
                )
            num_spark_tasks_for_heap_mem_req = int(math.ceil(total_heap_memory_bytes / ray_worker_heap_mem_bytes))
            if num_spark_tasks_for_heap_mem_req > num_spark_tasks:
                num_spark_tasks = num_spark_tasks_for_heap_mem_req

        if total_object_store_memory_bytes is not None:
            if total_object_store_memory_bytes <= 0:
                raise ValueError(
                    "You should specify 'total_object_store_memory_bytes' argument to a positive integer."
                )
            num_spark_tasks_for_object_store_mem_req = \
                int(math.ceil(total_object_store_memory_bytes / ray_worker_object_store_mem_bytes))
            if num_spark_tasks_for_object_store_mem_req > num_spark_tasks:
                num_spark_tasks = num_spark_tasks_for_object_store_mem_req

    if num_spark_task_cpus < 4:
        _logger.warning(
            f"Each ray worker node will be assigned with {num_spark_task_cpus} CPU cores, less than "
            "recommended value 4, because ray worker node cpu cors aligns with the cpu cores assigned to "
            "a spark task, you can increase 'spark.task.cpus' config value to address it."
        )

    if ray_worker_heap_mem_bytes < 10 * 1024 * 1024 * 1024:
        _logger.warning(
            f"Each ray worker node will be assigned with {ray_worker_heap_mem_bytes} bytes heap memory, "
            "less than recommended value 10GB, the ray worker node heap memory size is calculated by "
            "(SPARK_WORKER_NODE_PHYSICAL_MEMORY - SHARED_MEMORY) / num_local_spark_task_slots * 0.8, "
            "so you can increase spark cluster worker machine memory, or reduce spark task slots "
            "number on spark cluster worker, or reduce spark worker machine /dev/shm quota to "
            "address it."
        )

    ray_head_hostname = get_spark_driver_hostname(spark.conf.get("spark.master"))
    ray_head_port = get_random_safe_port()

    ray_head_node_manager_port = get_random_safe_port()
    ray_head_object_manager_port = get_random_safe_port()

    _logger.info(f"Ray head hostname {ray_head_hostname}, port {ray_head_port}")

    ray_exec_path = os.path.join(os.path.dirname(sys.executable), "ray")

    ray_log_dir = os.path.join(ray_log_dir, f"ray-{ray_head_port}")
    os.makedirs(ray_log_dir, exist_ok=True)

    # TODO: Many ray processes logs are outputted under "{ray_temp_dir}/session_latest/logs",
    #  Proposal: Update "ray start" scirpt to add a new option "ray_log_dir", and output logs
    #  to a different directory specified by "ray_log_dir", instead of using
    #  "{ray_temp_dir}/session_latest/logs".
    #  The reason is, for ray on spark, user is hard to access log files on spark worker machines,
    #  (especially on databricks runtime), so we'd better set the log output dir to be a
    #  path mounted with NFS shared by all spark cluster nodes, so that the user can access
    #  these remote log files from spark drive side easily.
    ray_temp_dir = os.path.join(ray_temp_dir, f"ray-{ray_head_port}")
    os.makedirs(ray_temp_dir, exist_ok=True)

    _logger.warning(
        f"You can check ray head / worker starting script logs under local disk path {ray_log_dir}, "
        f"and you can check ray processes logs under local disk path {ray_temp_dir}/session_latest/logs."
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
        f"--num-cpus=0",  # disallow ray tasks scheduled to ray head node.
        # limit the memory usage of head node because no task running on it.
        f"--memory={128 * 1024 * 1024}",
        # limit the object store memory usage of head node because no task running on it.
        f"--object-store-memory={128 * 1024 * 1024}",
        f"--node-manager-port={ray_head_node_manager_port}",
        f"--object-manager-port={ray_head_object_manager_port}",
        *_convert_ray_node_options(head_options)
    ]

    _logger.info(f"Start Ray head, command: {' '.join(ray_head_node_cmd)}")

    with open(
        os.path.join(ray_log_dir, "ray-start-head.log"),
        "w", buffering=1
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
        ray_head_hostname, ray_head_port, 40,
        error_on_failure="Start Ray head node failed!"
    )

    _logger.info("Ray head node started.")

    # NB:
    # In order to start ray worker nodes on spark cluster worker machines,
    # We launch a background spark job:
    #  1. it is a barrier mode spark job, i.e. all spark tasks in the job runs concurrently.
    #     if the spark cluster resources are not sufficient to launch all these tasks concurrently,
    #     the spark job will hang and retry, if exceeding maximum retries, it will fail.
    #  2. Each spark task launches one ray worker node. This design ensures all ray worker nodes
    #     has the same shape (same cpus / gpus / memory configuration). If ray worker nodes have
    #     different shape, the Ray cluster setup will be nondeterministic, and you could get very
    #     strange results with bad luck on the node sizing.
    #  3. It starts ray worker node via `ray start` CLI. In each spark task, it creates a
    #     child process and run `ray start ...` command in blocking mode.
    #  4. When shut down ray cluster, killing these ray worker nodes is implemented by:
    #     First, it installs a PR_SET_PDEATHSIG signal for the `ray start ...` child processes
    #     so that when parent process (pyspark task) dead, the child processes
    #     (`ray start ...` processes) will receive SIGTERM signal.
    #     When we need to shut down the ray cluster, call `sparkContext.cancelJobGroup`
    #     to cancel the background spark job, and it sends SIGKILL signal to all spark tasks,
    #     so this make spark task processes dead and triggers sending SIGTERM to `ray start ...`
    #     child processes.

    def ray_cluster_job_mapper(_):
        from pyspark.taskcontext import BarrierTaskContext
        _worker_logger = logging.getLogger("ray.spark.worker")

        context = BarrierTaskContext.get()
        task_id = context.partitionId()

        worker_hostname = context.getTaskInfos()[task_id].address.split(":")[0].strip()

        # TODO: remove worker side ray temp dir when ray worker exits.
        # Ray worker might run on a machine different with the head node, so create the
        # local log dir and temp dir again.
        os.makedirs(ray_temp_dir, exist_ok=True)
        os.makedirs(ray_log_dir, exist_ok=True)

        ray_worker_node_manager_port = get_random_safe_port()
        ray_worker_object_manager_port = get_random_safe_port()

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
            f"--node-manager-port={ray_worker_node_manager_port}",
            f"--object-manager-port={ray_worker_object_manager_port}",
            *_convert_ray_node_options(worker_options)
        ]

        ray_worker_extra_envs = {}

        if num_spark_task_gpus > 0:
            task_resources = context.resources()

            if "gpu" not in task_resources:
                raise RuntimeError(
                    "Couldn't get the gpu id, Please check the GPU resource configuration"
                )
            gpu_addr_list = [int(addr.strip()) for addr in task_resources["gpu"].addresses]

            available_physical_gpus = get_spark_task_assigned_physical_gpus(gpu_addr_list)
            ray_worker_cmd.append(
                f"--num-gpus={len(available_physical_gpus)}",
            )
            ray_worker_extra_envs['CUDA_VISIBLE_DEVICES'] = ",".join([
                str(gpu_id) for gpu_id in available_physical_gpus
            ])

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
                    _worker_logger.warning(f"Setup libc.prctl PR_SET_PDEATHSIG failed, error {repr(e)}.")

        else:
            setup_sigterm_on_parent_death = None

        _worker_logger.info(f"Start Ray worker, command: {' '.join(ray_worker_cmd)}")

        with open(
            os.path.join(ray_log_dir, f"ray-start-worker-{task_id}.log"),
            "w", buffering=1
        ) as worker_log_fp:
            exec_cmd(
                ray_worker_cmd,
                synchronous=True,
                capture_output=False,
                stream_output=False,
                extra_env=ray_worker_extra_envs,
                preexec_fn=setup_sigterm_on_parent_death,
                stdout=worker_log_fp,
                stderr=subprocess.STDOUT,
            )

        # NB: Not reachable.
        yield 0

    spark_job_group_id = f"ray-cluster-job-head-{ray_head_hostname}-port-{ray_head_port}"

    ray_cluster_handler = RayClusterOnSpark(
        address=f"{ray_head_hostname}:{ray_head_port}",
        head_proc=ray_head_proc,
        spark_job_group_id=spark_job_group_id,
        ray_temp_dir=ray_temp_dir,
    )

    def backgroud_job_thread_fn():
        try:
            spark.sparkContext.setJobGroup(
                spark_job_group_id,
                "This job group is for spark job which runs the Ray cluster with ray head node "
                f"{ray_head_hostname}:{ray_head_port}"
            )
            spark.sparkContext.parallelize(
                list(range(num_spark_tasks)), num_spark_tasks
            ).barrier().mapPartitions(
                ray_cluster_job_mapper
            ).collect()
        finally:
            # NB:
            # The background spark job is designed to running forever until it is killed,
            # So this `finally` block is reachable only when:
            #  1. The background job raises unexpected exception (i.e. ray worker nodes failed unexpectedly)
            #  2. User explicitly orders shutting down the ray cluster.
            #  3. On databricks runtime, when notebook detached, it triggers python REPL onCancel event and
            #     it cancelled the background running spark job
            #  For case 1 and 3, only ray workers are killed, but driver side ray head might be still
            #  running, and ray context might be in connected status, we need to disconnect and kill ray
            #  head node, so call `ray_cluster_handler.shutdown()` here.
            ray_cluster_handler.shutdown()

    threading.Thread(
        target=inheritable_thread_target(backgroud_job_thread_fn),
        args=()
    ).start()

    try:
        # Waiting all ray workers spin up.
        time.sleep(10)

        if is_in_databricks_runtime():
            try:
                get_dbutils().entry_point.registerBackgroundSparkJobGroup(spark_job_group_id)
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
