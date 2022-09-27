import os
import sys
from urllib.parse import urlparse
import time
import threading
import logging

from .utils import (
    exec_cmd,
    check_port_open,
    get_safe_port,
    get_spark_session,
    get_spark_driver_hostname,
    is_in_databricks_runtime,
    get_spark_task_assigned_physical_gpus,
    get_per_spark_task_memory,
)


def _create_ray_tmp_dir(prefix):
    import tempfile

    return tempfile.mkdtemp(prefix=prefix)


def wait_ray_node_available(hostname, port, timeout, error_on_failure):
    # Wait Ray head node spin up.
    for _ in range(timeout):
        time.sleep(1)
        if check_port_open(hostname, port):
            break

    if not check_port_open(hostname, port):
        raise RuntimeError(error_on_failure)



class RayClusterOnSpark:

    def __init__(self, address, head_proc, spark_job_group_id):
        self.address = address
        self.head_proc = head_proc
        self.spark_job_group_id = spark_job_group_id

    def shutdown(self):
        get_spark_session().sparkContext.cancelJobGroup(self.spark_job_group_id)
        self.head_proc.kill()


def init_cluster(num_spark_tasks):
    # TODO:
    #  support more ray head / worker arguments.
    #  support more ray.init arguments.
    import ray
    from pyspark.util import inheritable_thread_target

    spark = get_spark_session()
    ray_head_hostname = get_spark_driver_hostname(spark)
    ray_head_port = get_safe_port(ray_head_hostname)

    logging.info(f"Ray head hostanme {ray_head_hostname}, port {ray_head_port}")

    ray_exec_path = os.path.join(os.path.dirname(sys.executable), "ray")

    ray_head_tmp_dir = _create_ray_tmp_dir(f"ray-head-port-{ray_head_port}-tmp-")
    ray_head_node_cmd = [
        ray_exec_path,
        "start",
        f"--temp-dir={ray_head_tmp_dir}",
        f"--num-cpus=0",  # disallow ray tasks scheduled to ray head node.
        "--block",
        "--head",
        f"--port={ray_head_port}",
        "--include-dashboard=false",
    ]

    logging.info(f"Start Ray head, command: {' '.join(ray_head_node_cmd)}")
    ray_node_proc = exec_cmd(
        ray_head_node_cmd,
        synchronous=False,
        capture_output=False,
        stream_output=False,
    )

    # wait ray head node spin up.
    wait_ray_node_available(
        ray_head_hostname, ray_head_port, 40,
        "Start Ray head node failed!"
    )

    logging.info("Ray head node started.")

    num_spark_task_cpus = int(spark.sparkContext.getConf().get("spark.task.cpus", "1"))
    num_spark_task_gpus = int(spark.sparkContext.getConf().get("spark.task.resource.gpu.amount", "0"))

    ray_worker_memory_in_bytes = get_per_spark_task_memory()
    def ray_cluster_job_mapper(_):
        from pyspark.taskcontext import BarrierTaskContext

        context = BarrierTaskContext.get()
        context.barrier()
        task_id = context.partitionId()

        ray_worker_tmp_dir = _create_ray_tmp_dir(
            f"ray-worker-{task_id}-head-{ray_head_hostname}:{ray_head_port}-tmp-"
        )

        ray_worker_cmd = [
            ray_exec_path,
            "start",
            f"--temp-dir={ray_worker_tmp_dir}",
            f"--num-cpus={num_spark_task_cpus}",
            "--block",
            f"--address={ray_head_hostname}:{ray_head_port}",
            f"--memory={ray_worker_memory_in_bytes}",
        ]

        ray_worker_extra_envs = {}

        if num_spark_task_gpus > 0:
            available_physical_gpus = get_spark_task_assigned_physical_gpus(context)
            ray_worker_cmd.append(
                f"--num-gpus={len(available_physical_gpus)}",
            )
            ray_worker_extra_envs['CUDA_VISIBLE_DEVICES'] = ",".join([
                str(gpu_id) for gpu_id in num_spark_task_gpus
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
                    logging.warning(f"Setup libc.prctl PR_SET_PDEATHSIG failed, error {repr(e)}.")

        else:
            setup_sigterm_on_parent_death = None

        # TODO: Add a thread to redirect subprocess logs
        #  and collect tail logs and raise error if subprocess failed.
        logging.info(f"Start Ray worker, command: {' '.join(ray_worker_cmd)}")

        # Q: When Ray head node killed, will ray worker node exit as well ?
        exec_cmd(
            ray_worker_cmd,
            synchronous=True,
            capture_output=False,
            stream_output=False,
            extra_env=ray_worker_extra_envs,
            preexec_fn=setup_sigterm_on_parent_death,
        )

        # NB: Not reachable.
        yield 0

    spark_job_group_id = f"ray-cluster-job-head-{ray_head_hostname}-port-{ray_head_port}"

    # TODO: redirect background thread output.
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
        ).collect()[0]

    threading.Thread(
        target=inheritable_thread_target(backgroud_job_thread_fn),
        args=()
    ).start()

    # Waiting all ray workers spin up.
    time.sleep(10)

    # discover the ray cluster.
    ray.init(address=f"{ray_head_hostname}:{ray_head_port}")

    # TODO:
    #  Register databricks REPL detach event hook to shutdown the ray cluster.
    return RayClusterOnSpark(
        address=f"{ray_head_hostname}:{ray_head_port}",
        head_proc=ray_node_proc,
        spark_job_group_id=spark_job_group_id
    )
