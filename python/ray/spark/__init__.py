import os
import shutil
import subprocess
import sys
import time
import threading
import logging
from packaging.version import Version

from .utils import (
    exec_cmd,
    check_port_open,
    get_safe_port,
    get_spark_session,
    get_spark_driver_hostname,
    is_in_databricks_runtime,
    get_spark_task_assigned_physical_gpus,
    get_avail_mem_per_ray_worker,
    get_dbutils,
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
    It can be used to shutdown the cluster.
    """

    def __init__(self, address, head_proc, spark_job_group_id, ray_context, ray_head_temp_dir):
        self.address = address
        self.head_proc = head_proc
        self.spark_job_group_id = spark_job_group_id
        self.ray_context = ray_context
        self.ray_head_temp_dir = ray_head_temp_dir

    def _cancel_background_spark_job(self):
        get_spark_session().sparkContext.cancelJobGroup(self.spark_job_group_id)

    def shutdown(self):
        """
        Shutdown the ray cluster created by `init_cluster` API.
        """
        self.ray_context.disconnect()
        self._cancel_background_spark_job()
        self.head_proc.kill()
        shutil.rmtree(self.ray_head_temp_dir, ignore_errors=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()


def _convert_ray_node_options(options):
    return [f"--{k.replace('_', '-')}={str(v)}" for k, v in options.items()]


def init_cluster(
        num_spark_tasks,
        head_options=None,
        worker_options=None,
        ray_temp_dir="/tmp/ray/temp",
        ray_node_log_dir="/tmp/ray/logs"
):
    """
    Initialize a ray cluster on the spark cluster, via creating a background spark barrier
    mode job and each spark task running a ray worker node, and in spark driver side
    a ray head node is started. And then connect to the created ray cluster.

    Args
        num_spark_tasks: Specify the spark task number the spark job will create.
            This argument controls how many resources (CPU / GPU / memory) the ray cluster
            can use.
        head_options: A dict representing Ray head node options.
        worker_options: A dict representing Ray worker node options.
        ray_temp_dir: A local disk path to store the ray temporary data.
        ray_node_log_dir: A local disk path to store the ray head / worker nodes logs.
            On databricks runtime, we recommend to use path under `/dbfs/` that is mounted
            with DBFS shared by all spark cluster nodes, so that we can check all ray worker
            logs from driver side easily.
    """
    import ray
    from pyspark.util import inheritable_thread_target

    head_options = head_options or {}
    worker_options = worker_options or {}

    spark = get_spark_session()
    ray_head_hostname = get_spark_driver_hostname(spark)
    ray_head_port = get_safe_port(ray_head_hostname)

    _logger.info(f"Ray head hostanme {ray_head_hostname}, port {ray_head_port}")

    num_spark_task_cpus = int(spark.sparkContext.getConf().get("spark.task.cpus", "1"))
    num_spark_task_gpus = int(spark.sparkContext.getConf().get("spark.task.resource.gpu.amount", "0"))

    ray_worker_heap_mem_bytes, ray_worker_object_store_mem_bytes = get_avail_mem_per_ray_worker(spark)

    ray_exec_path = os.path.join(os.path.dirname(sys.executable), "ray")

    ray_node_log_dir = os.path.join(ray_node_log_dir, f"cluster-{ray_head_hostname}-{ray_head_port}")

    _logger.warning(f"You can check ray head / worker nodes logs under local disk path {ray_node_log_dir}")
    if is_in_databricks_runtime() and not ray_node_log_dir.startswith("/dbfs"):
        _logger.warning(
            "We recommend you to set `ray_node_log_root_path` argument to be a path under '/dbfs/', "
            "because for all spark cluster nodes '/dbfs/' path is mounted with a shared disk, "
            "so that you can check ray worker logs on spark driver node."
        )

    os.makedirs(ray_node_log_dir, exist_ok=True)

    ray_temp_dir = os.path.join(ray_temp_dir, f"cluster-{ray_head_hostname}-{ray_head_port}")
    ray_head_temp_dir = os.path.join(ray_temp_dir, "head")
    os.makedirs(ray_head_temp_dir, exist_ok=True)

    ray_head_node_cmd = [
        ray_exec_path,
        "start",
        f"--temp-dir={ray_head_temp_dir}",
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
        *_convert_ray_node_options(head_options)
    ]

    _logger.info(f"Start Ray head, command: {' '.join(ray_head_node_cmd)}")

    with open(os.path.join(ray_node_log_dir, "ray-head.log"), "w", buffering=1) as head_log_fp:
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

    def ray_cluster_job_mapper(_):
        from pyspark.taskcontext import BarrierTaskContext
        _worker_logger = logging.getLogger("ray.spark.worker")

        context = BarrierTaskContext.get()
        context.barrier()
        task_id = context.partitionId()

        # TODO: remove temp dir when ray worker exits.
        ray_worker_temp_dir = os.path.join(ray_temp_dir, f"worker-{task_id}")
        os.makedirs(ray_worker_temp_dir, exist_ok=True)

        ray_worker_cmd = [
            ray_exec_path,
            "start",
            f"--temp-dir={ray_worker_temp_dir}",
            f"--num-cpus={num_spark_task_cpus}",
            "--block",
            "--disable-usage-stats",
            f"--address={ray_head_hostname}:{ray_head_port}",
            f"--memory={ray_worker_heap_mem_bytes}",
            f"--object-store-memory={ray_worker_object_store_mem_bytes}",
            *_convert_ray_node_options(worker_options)
        ]

        ray_worker_extra_envs = {}

        if num_spark_task_gpus > 0:
            available_physical_gpus = get_spark_task_assigned_physical_gpus(context.resources())
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
                When a pyspark job canceled, the UDF python process are killed by signal "SIGKILL",
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

        ray_worker_log_file = os.path.join(
            ray_node_log_dir,
            f"ray-worker-{task_id}.log"
        )
        with open(ray_worker_log_file, "w", buffering=1) as worker_log_fp:
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

    def backgroud_job_thread_fn():
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

    spark_job_group_id = f"ray-cluster-job-head-{ray_head_hostname}-port-{ray_head_port}"

    threading.Thread(
        target=inheritable_thread_target(backgroud_job_thread_fn),
        args=()
    ).start()

    try:
        # Waiting all ray workers spin up.
        time.sleep(10)

        # connect to the ray cluster.
        ray_context = ray.init(address=f"{ray_head_hostname}:{ray_head_port}")

        if is_in_databricks_runtime():
            try:
                get_dbutils().entry_point.registerBackgroundSparkJobGroup(spark_job_group_id)
            except Exception:
                _logger.warning(
                    "Register ray cluster spark job as background job failed. You need to manually "
                    "call `ray_cluster_on_spark.shutdown()` before detaching your databricks "
                    "python REPL."
                )
        return RayClusterOnSpark(
            address=f"{ray_head_hostname}:{ray_head_port}",
            head_proc=ray_head_proc,
            spark_job_group_id=spark_job_group_id,
            ray_context=ray_context,
            ray_head_temp_dir=ray_head_temp_dir,
        )
    except Exception:
        # If init ray cluster raise exception, kill the ray head proc.
        ray_head_proc.kill()
        raise
