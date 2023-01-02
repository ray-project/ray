import subprocess
import os
import sys
import random
import threading
import collections
import logging


_MEMORY_BUFFER_OFFSET = 0.8


_logger = logging.getLogger("ray.util.spark.utils")


def is_in_databricks_runtime():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def gen_cmd_exec_failure_msg(cmd, return_code, tail_output_deque):
    cmd_str = " ".join(cmd)
    tail_output = "".join(tail_output_deque)
    return (
        f"Command {cmd_str} failed with return code {return_code}, tail output are "
        f"included below.\n{tail_output}\n"
    )


def exec_cmd(
    cmd,
    *,
    extra_env=None,
    synchronous=True,
    **kwargs,
):
    """
    A convenience wrapper of `subprocess.Popen` for running a command from a Python
    script.
    If `synchronous` is True, wait until the process terminated and if subprocess
    return code is not 0, raise error containing last 100 lines output.
    If `synchronous` is False, return an `Popen` instance and a deque instance holding
    tail outputs.
    The subprocess stdout / stderr output will be streamly redirected to current
    process stdout.
    """
    illegal_kwargs = set(kwargs.keys()).intersection({"text", "stdout", "stderr"})
    if illegal_kwargs:
        raise ValueError(f"`kwargs` cannot contain {list(illegal_kwargs)}")

    env = kwargs.pop("env", None)
    if extra_env is not None and env is not None:
        raise ValueError("`extra_env` and `env` cannot be used at the same time")

    env = env if extra_env is None else {**os.environ, **extra_env}

    process = subprocess.Popen(
        cmd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        **kwargs,
    )

    tail_output_deque = collections.deque(maxlen=100)

    def redirect_log_thread_fn():
        for line in process.stdout:
            # collect tail logs by `tail_output_deque`
            tail_output_deque.append(line)

            # redirect to stdout.
            sys.stdout.write(line)

    threading.Thread(target=redirect_log_thread_fn, args=()).start()

    if not synchronous:
        return process, tail_output_deque

    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(
            gen_cmd_exec_failure_msg(cmd, return_code, tail_output_deque)
        )


def check_port_open(host, port):
    import socket
    from contextlib import closing

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        return sock.connect_ex((host, port)) == 0


def get_random_unused_port(
    host, min_port=1024, max_port=65535, max_retries=100, exclude_list=None
):
    """
    Get random unused port.
    """
    # Use true random generator
    rng = random.SystemRandom()

    exclude_list = exclude_list or []
    for _ in range(max_retries):
        port = rng.randint(min_port, max_port)
        if port in exclude_list:
            continue
        if not check_port_open(host, port):
            return port
    raise RuntimeError(
        f"Get available port between range {min_port} and {max_port} failed."
    )


def get_spark_session():
    from pyspark.sql import SparkSession

    spark_session = SparkSession.getActiveSession()
    if spark_session is None:
        raise RuntimeError(
            "Spark session haven't been initiated yet. Please use "
            "`SparkSession.builder` to create a spark session and connect to a spark "
            "cluster."
        )
    return spark_session


def get_spark_application_driver_host(spark):
    return spark.conf.get("spark.driver.host")


def get_max_num_concurrent_tasks(spark_context, resource_profile):
    """Gets the current max number of concurrent tasks."""
    # pylint: disable=protected-access=
    return spark_context._jsc.sc().maxNumConcurrentTasks(
        resource_profile._java_resource_profile
    )


def _get_total_physical_memory():
    import psutil

    return psutil.virtual_memory().total


def _get_total_shared_memory():
    import shutil

    return shutil.disk_usage("/dev/shm").total


def _get_cpu_cores():
    import multiprocessing

    return multiprocessing.cpu_count()


def _calc_mem_per_ray_worker_node(
    num_task_slots, physical_mem_bytes, shared_mem_bytes, object_store_memory_per_node
):
    available_physical_mem_per_node = int(
        physical_mem_bytes / num_task_slots * _MEMORY_BUFFER_OFFSET
    )
    available_shared_mem_per_node = int(
        shared_mem_bytes / num_task_slots * _MEMORY_BUFFER_OFFSET
    )
    if object_store_memory_per_node is None:
        object_store_bytes = available_shared_mem_per_node
    else:
        object_store_bytes = int(
            min(
                object_store_memory_per_node,
                available_shared_mem_per_node,
            )
        )
    heap_mem_bytes = available_physical_mem_per_node - object_store_bytes
    return heap_mem_bytes, object_store_bytes


def get_avail_mem_per_ray_worker_node(spark, object_store_memory_per_node):
    """
    Return the available heap memory and object store memory for each ray worker.
    NB: We have one ray node per spark task.
    """
    num_cpus_per_spark_task = int(
        spark.sparkContext.getConf().get("spark.task.cpus", "1")
    )

    def mapper(_):
        try:
            num_cpus = _get_cpu_cores()
            num_task_slots = num_cpus // num_cpus_per_spark_task

            physical_mem_bytes = _get_total_physical_memory()
            shared_mem_bytes = _get_total_shared_memory()

            (
                ray_worker_node_heap_mem_bytes,
                ray_worker_node_object_store_bytes,
            ) = _calc_mem_per_ray_worker_node(
                num_task_slots,
                physical_mem_bytes,
                shared_mem_bytes,
                object_store_memory_per_node,
            )
            return (
                ray_worker_node_heap_mem_bytes,
                ray_worker_node_object_store_bytes,
                None,
            )
        except Exception as e:
            return -1, -1, repr(e)

    # Running memory inference routine on spark executor side since the spark worker
    # nodes may have a different machine configuration compared to the spark driver
    # node.
    (
        inferred_ray_worker_node_heap_mem_bytes,
        inferred_ray_worker_node_object_store_bytes,
        err,
    ) = (
        spark.sparkContext.parallelize([1], 1).map(mapper).collect()[0]
    )

    if err is not None:
        raise RuntimeError(
            f"Inferring ray worker available memory failed, error: {err}"
        )
    return (
        inferred_ray_worker_node_heap_mem_bytes,
        inferred_ray_worker_node_object_store_bytes,
    )


def get_spark_task_assigned_physical_gpus(gpu_addr_list):
    if "CUDA_VISIBLE_DEVICES" in os.environ:
        visible_cuda_dev_list = [
            int(dev.strip()) for dev in os.environ["CUDA_VISIBLE_DEVICES"].split(",")
        ]
        return [visible_cuda_dev_list[addr] for addr in gpu_addr_list]
    else:
        return gpu_addr_list


def setup_sigterm_on_parent_death():
    """
    Uses prctl to automatically send SIGTERM to the child process when its parent is
    dead. The child process itself should handle SIGTERM properly.
    """
    try:
        import ctypes
        import signal

        libc = ctypes.CDLL("libc.so.6")
        # Set the parent process death signal of the command process to SIGTERM.
        libc.prctl(1, signal.SIGTERM)  # PR_SET_PDEATHSIG, see prctl.h
    except OSError as e:
        _logger.warning(f"Setup libc.prctl PR_SET_PDEATHSIG failed, error {repr(e)}.")
