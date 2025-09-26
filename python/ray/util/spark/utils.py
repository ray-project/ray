"""
Core utilities for Ray on Spark.

This module provides essential utilities that are used throughout the
Ray on Spark implementation. Functionality is delegated to specialized
modules where appropriate.
"""

import collections
import logging
import os
import shutil
import subprocess
import sys
import threading
import time
from typing import Deque, Optional

from .databricks import is_in_databricks_runtime

_logger = logging.getLogger("ray.util.spark.utils")


def gen_cmd_exec_failure_msg(cmd, return_code, tail_output_deque):
    """Generate command execution failure message."""
    cmd_str = " ".join(cmd)
    tail_output = "".join(tail_output_deque)
    return (
        f"Running cmd: {cmd_str} failed with return code: {return_code}, "
        f"tail output: {tail_output}"
    )


def exec_cmd(
    cmd,
    *,
    extra_env=None,
    capture_stdout=True,
    **kwargs,
):
    """Execute command with proper error handling."""
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    if capture_stdout:
        kwargs.setdefault("stdout", subprocess.PIPE)
        kwargs.setdefault("stderr", subprocess.STDOUT)
        kwargs.setdefault("text", True)

        tail_output_deque = collections.deque(maxlen=100)
        popen = subprocess.Popen(cmd, env=env, **kwargs)

        for line in iter(popen.stdout.readline, ""):
            tail_output_deque.append(line)

        return_code = popen.wait()

        if return_code != 0:
            raise RuntimeError(
                gen_cmd_exec_failure_msg(cmd, return_code, tail_output_deque)
            )
    else:
        popen = subprocess.Popen(cmd, env=env, **kwargs)
        return_code = popen.wait()

        if return_code != 0:
            raise RuntimeError(
                f"Command failed with return code {return_code}: {' '.join(cmd)}"
            )


def get_spark_session():
    """Get the active Spark session."""
    try:
        from pyspark.sql import SparkSession

        return SparkSession.getActiveSession()
    except ImportError:
        raise RuntimeError("PySpark not available")


def get_spark_application_driver_host(spark=None):
    """Get Spark application driver host."""
    if spark is None:
        spark = get_spark_session()
    return spark.sparkContext.getConf().get("spark.driver.host", "localhost")


def get_random_unused_port(host="localhost", min_port=1024, max_port=65535):
    """Get a random unused port."""
    import socket
    import random

    for _ in range(100):  # Try up to 100 times
        port = random.randint(min_port, max_port)
        if not is_port_in_use(host, port):
            return port

    raise RuntimeError(f"Could not find unused port in range {min_port}-{max_port}")


def is_port_in_use(host, port):
    """Check if a port is in use."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((host, port))
            return False
        except OSError:
            return True


def _wait_service_up(address, timeout=30):
    """Wait for service to be available."""
    import socket
    import time

    host, port = address.split(":")
    port = int(port)

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                s.connect((host, port))
                return True
        except (socket.error, ConnectionRefusedError):
            time.sleep(0.5)

    return False


# Resource detection functions - delegate to common module
def _get_cpu_cores():
    """Get CPU core count with enhanced detection."""
    from .common import ResourceDetector

    return ResourceDetector.get_cpu_count()


def _get_num_physical_gpus():
    """Get number of physical GPUs."""
    from .common import ResourceDetector

    return ResourceDetector.get_gpu_count()


def _get_spark_worker_total_physical_memory():
    """Get total physical memory."""
    from .common import ResourceDetector

    memory_info = ResourceDetector.get_memory_info()
    return memory_info["effective_total"]


def _get_spark_worker_total_shared_memory():
    """Get total shared memory."""
    from .common import ResourceDetector

    shared_memory_info = ResourceDetector.get_shared_memory_info()
    return shared_memory_info["total"]


# Configuration constants
RAY_ON_SPARK_WORKER_CPU_CORES = "RAY_ON_SPARK_WORKER_CPU_CORES"
RAY_ON_SPARK_WORKER_GPU_NUM = "RAY_ON_SPARK_WORKER_GPU_NUM"
RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES = "RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES"
RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES = "RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES"
RAY_ON_SPARK_DRIVER_PHYSICAL_MEMORY_BYTES = "RAY_ON_SPARK_DRIVER_PHYSICAL_MEMORY_BYTES"
RAY_ON_SPARK_DRIVER_SHARED_MEMORY_BYTES = "RAY_ON_SPARK_DRIVER_SHARED_MEMORY_BYTES"

# Memory management constants
_RAY_ON_SPARK_MAX_OBJECT_STORE_MEMORY_PROPORTION = 0.8
_RAY_ON_SPARK_NODE_MEMORY_BUFFER_OFFSET = 0.8
OBJECT_STORE_MINIMUM_MEMORY_BYTES = 100 * 1024 * 1024  # 100MB


def _calc_mem_per_ray_node(
    available_physical_mem_per_node,
    available_shared_mem_per_node,
    configured_heap_memory_bytes,
    configured_object_store_bytes,
):
    """Calculate memory allocation per Ray node."""
    heap_mem_bytes = None
    object_store_bytes = None
    warning_msg = None

    if configured_object_store_bytes is None:
        object_store_bytes = min(
            available_physical_mem_per_node * 0.3,
            available_shared_mem_per_node
            * _RAY_ON_SPARK_MAX_OBJECT_STORE_MEMORY_PROPORTION,
        )
    else:
        object_store_bytes = configured_object_store_bytes

    if object_store_bytes < OBJECT_STORE_MINIMUM_MEMORY_BYTES:
        if configured_object_store_bytes is None:
            warning_msg = (
                "Ray worker object store memory is too small, "
                f"configured to minimal size ({OBJECT_STORE_MINIMUM_MEMORY_BYTES} bytes). "
                "Please increase system /dev/shm size."
            )
        else:
            warning_msg = (
                "Configured Ray worker object store memory is too small, "
                f"configured to minimal size ({OBJECT_STORE_MINIMUM_MEMORY_BYTES} bytes). "
                "Please increase 'object_store_memory_worker_node' argument value."
            )
        object_store_bytes = OBJECT_STORE_MINIMUM_MEMORY_BYTES

    object_store_bytes = int(object_store_bytes)

    if configured_heap_memory_bytes is None:
        heap_mem_bytes = int(available_physical_mem_per_node - object_store_bytes)
    else:
        heap_mem_bytes = int(configured_heap_memory_bytes)

    return heap_mem_bytes, object_store_bytes, warning_msg


def calc_mem_ray_head_node(configured_heap_memory_bytes, configured_object_store_bytes):
    """Calculate memory allocation for Ray head node."""
    import psutil

    # Get driver memory configuration
    if RAY_ON_SPARK_DRIVER_PHYSICAL_MEMORY_BYTES in os.environ:
        available_physical_mem = int(
            os.environ[RAY_ON_SPARK_DRIVER_PHYSICAL_MEMORY_BYTES]
        )
    else:
        available_physical_mem = psutil.virtual_memory().total

    available_physical_mem = (
        available_physical_mem * _RAY_ON_SPARK_NODE_MEMORY_BUFFER_OFFSET
    )

    if RAY_ON_SPARK_DRIVER_SHARED_MEMORY_BYTES in os.environ:
        available_shared_mem = int(os.environ[RAY_ON_SPARK_DRIVER_SHARED_MEMORY_BYTES])
    else:
        available_shared_mem = shutil.disk_usage("/dev/shm").total

    available_shared_mem = (
        available_shared_mem * _RAY_ON_SPARK_NODE_MEMORY_BUFFER_OFFSET
    )

    heap_mem_bytes, object_store_bytes, warning_msg = _calc_mem_per_ray_node(
        available_physical_mem,
        available_shared_mem,
        configured_heap_memory_bytes,
        configured_object_store_bytes,
    )

    if warning_msg is not None:
        _logger.warning(warning_msg)

    return heap_mem_bytes, object_store_bytes


def _get_local_ray_node_slots(num_cpus, num_gpus, num_cpus_per_node, num_gpus_per_node):
    """Calculate number of Ray node slots that can fit on a Spark worker."""
    if num_cpus_per_node > num_cpus:
        raise ValueError(
            f"CPU number per Ray worker node ({num_cpus_per_node}) should be <= "
            f"spark worker node CPU cores ({num_cpus})"
        )

    num_ray_node_slots = num_cpus // num_cpus_per_node

    if num_gpus_per_node > 0:
        if num_gpus_per_node > num_gpus:
            raise ValueError(
                f"GPU number per Ray worker node ({num_gpus_per_node}) should be <= "
                f"spark worker node GPU number ({num_gpus})"
            )

        gpu_slots = num_gpus // num_gpus_per_node
        num_ray_node_slots = min(num_ray_node_slots, gpu_slots)

    return num_ray_node_slots


def get_max_num_concurrent_tasks(spark_context):
    """Get maximum number of concurrent tasks in Spark."""
    return spark_context.defaultParallelism


def get_configured_spark_executor_memory_bytes(spark_context):
    """Get configured Spark executor memory in bytes."""
    executor_memory = spark_context.getConf().get("spark.executor.memory", "1g")

    # Parse memory string (e.g., "2g", "1024m")
    if executor_memory.endswith("g"):
        return int(executor_memory[:-1]) * 1024**3
    elif executor_memory.endswith("m"):
        return int(executor_memory[:-1]) * 1024**2
    elif executor_memory.endswith("k"):
        return int(executor_memory[:-1]) * 1024
    else:
        return int(executor_memory)


def get_spark_task_assigned_physical_gpus(gpu_addr_list):
    """Get physical GPU addresses assigned to Spark task."""
    # Handle CUDA_VISIBLE_DEVICES environment variable
    if "CUDA_VISIBLE_DEVICES" in os.environ:
        try:
            visible_cuda_dev_list = [
                int(dev.strip())
                for dev in os.environ["CUDA_VISIBLE_DEVICES"].split(",")
                if dev.strip()
            ]
            return [
                visible_cuda_dev_list[addr]
                for addr in gpu_addr_list
                if addr < len(visible_cuda_dev_list)
            ]
        except (ValueError, IndexError):
            pass

    return gpu_addr_list if gpu_addr_list else []


def _calc_mem_per_ray_worker_node(
    num_task_slots,
    physical_mem_bytes,
    shared_mem_bytes,
    configured_heap_memory_bytes,
    configured_object_store_bytes,
):
    """Calculate memory allocation per Ray worker node."""
    available_physical_mem_per_node = int(
        physical_mem_bytes / num_task_slots * _RAY_ON_SPARK_NODE_MEMORY_BUFFER_OFFSET
    )
    available_shared_mem_per_node = int(
        shared_mem_bytes / num_task_slots * _RAY_ON_SPARK_NODE_MEMORY_BUFFER_OFFSET
    )
    return _calc_mem_per_ray_node(
        available_physical_mem_per_node,
        available_shared_mem_per_node,
        configured_heap_memory_bytes,
        configured_object_store_bytes,
    )


def _get_avail_mem_per_ray_worker_node(
    num_cpus_per_node,
    num_gpus_per_node,
    heap_memory_per_node,
    object_store_memory_per_node,
):
    """
    Returns tuple of (
        ray_worker_node_heap_mem_bytes,
        ray_worker_node_object_store_bytes,
        error_message, # always None
        warning_message,
    )
    """
    num_cpus = _get_cpu_cores()
    if num_gpus_per_node > 0:
        num_gpus = _get_num_physical_gpus()
    else:
        num_gpus = 0

    num_ray_node_slots = _get_local_ray_node_slots(
        num_cpus, num_gpus, num_cpus_per_node, num_gpus_per_node
    )

    physical_mem_bytes = _get_spark_worker_total_physical_memory()
    shared_mem_bytes = _get_spark_worker_total_shared_memory()

    (
        ray_worker_node_heap_mem_bytes,
        ray_worker_node_object_store_bytes,
        warning_msg,
    ) = _calc_mem_per_ray_worker_node(
        num_ray_node_slots,
        physical_mem_bytes,
        shared_mem_bytes,
        heap_memory_per_node,
        object_store_memory_per_node,
    )
    return (
        ray_worker_node_heap_mem_bytes,
        ray_worker_node_object_store_bytes,
        None,  # No error
        warning_msg,
    )


def get_avail_mem_per_ray_worker_node(
    spark,
    heap_memory_per_node,
    object_store_memory_per_node,
    num_cpus_per_node,
    num_gpus_per_node,
):
    """
    Return the available heap memory and object store memory for each ray worker,
    and error / warning message if it has.

    Return value is a tuple of:
    (ray_worker_node_heap_mem_bytes, ray_worker_node_object_store_bytes,
     error_message, warning_message)

    Note: We have one ray node per spark task.
    """

    def mapper(_):
        try:
            return _get_avail_mem_per_ray_worker_node(
                num_cpus_per_node,
                num_gpus_per_node,
                heap_memory_per_node,
                object_store_memory_per_node,
            )
        except Exception as e:
            import traceback

            trace_msg = "\n".join(traceback.format_tb(e.__traceback__))
            return -1, -1, repr(e) + trace_msg, None

    # Running memory inference routine on spark executor side since the spark worker
    # nodes may have a different machine configuration compared to the spark driver
    # node.
    (
        inferred_ray_worker_node_heap_mem_bytes,
        inferred_ray_worker_node_object_store_bytes,
        err,
        warning_msg,
    ) = (
        spark.sparkContext.parallelize([1], 1).map(mapper).collect()[0]
    )

    if err is not None:
        raise RuntimeError(
            f"Inferring ray worker node available memory failed, error: {err}. "
            "You can bypass this error by setting following spark configs: "
            "spark.executorEnv.RAY_ON_SPARK_WORKER_CPU_CORES, "
            "spark.executorEnv.RAY_ON_SPARK_WORKER_GPU_NUM, "
            "spark.executorEnv.RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES, "
            "spark.executorEnv.RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES."
        )
    if warning_msg is not None:
        _logger.warning(warning_msg)
    return (
        inferred_ray_worker_node_heap_mem_bytes,
        inferred_ray_worker_node_object_store_bytes,
    )
